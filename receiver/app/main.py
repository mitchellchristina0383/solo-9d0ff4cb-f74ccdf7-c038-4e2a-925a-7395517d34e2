"""审计接收端（含热库归档）。

接收“部署完成通知”，按 (env, release_no) 幂等入库，并提供回执查询。
- 相同 (env, release_no) 且内容一致的重试：返回既有回执，不新增记录；
- 相同 (env, release_no) 但内容不同：409 冲突；
- 故障注入（仅演示）：simulate=lose_response（入库后丢响应）/ slow（入库后慢响应）。

热库归档（缩小热库、历史回执不丢）：
- 管理员在网页按 **UTC 截止时间** 把已入账的旧回执搬到同一数据卷内的独立
  SQLite 归档库（默认 /data/archive.db）；
- 每条回执的搬迁严格按「先复制进归档库并提交 → 再从热库删除」的顺序，
  配合热库内的归档暂存表（archive_pending）记录每行进度，因此：
    * 任意时刻按 (env, release_no) 查询都能拿到这一份回执（热库或归档库），
      绝不丢失、绝不重复显示；
    * 归档进行中、写入失败、进程重启后重试，都收敛到同一条可查记录；
    * 只有某行确认「已进归档库且已从热库删除」，其归档状态才算完成，
      中途失败绝不误报完成；
- 归档后同键同内容重传：命中归档库，返回原回执（deduplicated，location=archive）；
  同键不同内容：仍报 409 冲突，且不会重新写回热库；
- 新通知（归档库与热库都没有的键）正常进入热库。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "/data/receiver.db")
# 归档库与热库在同一数据卷（同一目录），保证卷内搬迁不丢数据
ARCHIVE_DB_PATH = os.environ.get("ARCHIVE_DB_PATH",
                                os.path.join(os.path.dirname(DB_PATH) or ".", "archive.db"))
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
VALID_SIMULATE = {"lose_response", "slow"}
# 归档故障注入（仅演示/自测）：
#   fail_after_copy —— 行已复制进归档库并提交、热库尚未删除时失败
#   fail_before_delete —— 同上（同义，便于按语义命名）
VALID_ARCHIVE_SIMULATE = {"fail_after_copy", "fail_before_delete"}
BATCH_LIMIT = 100

# 归档任务/行状态
JOB_RUNNING, JOB_DONE, JOB_FAILED = "RUNNING", "DONE", "FAILED"
ROW_PENDING, ROW_COPIED, ROW_DONE = "PENDING", "COPIED", "DONE"

# 同一时刻只允许一个归档任务（内存锁 + 库内 RUNNING 状态双保险）
_archive_lock = threading.Lock()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def content_hash(env: str, release_no: str, result: str) -> str:
    canonical = json.dumps(
        {"env": env, "release_no": release_no, "result": result},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_utc(value: str) -> datetime:
    """解析网页传入的 UTC 截止时间（接受 datetime-local 与带 Z/偏移量的写法）。"""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        # 网页约定为 UTC，无时区后缀时按 UTC 解释
        dt = dt.replace(tzinfo=datetime.now(timezone.utc).tzinfo)
    return dt.astimezone(timezone.utc)


def connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, isolation_level=None, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(env, release_no)
            )
            """
        )
        # 归档任务表：记录每次按截止时间发起的归档及最终状态
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS archive_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cutoff TEXT NOT NULL,
                status TEXT NOT NULL,
                total INTEGER NOT NULL DEFAULT 0,
                archived INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                simulate TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        # 行级归档进度（热库内暂存）：崩溃/重试后据此续做，不会重复也不会丢
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS archive_pending (
                job_id INTEGER NOT NULL,
                receipt_id INTEGER NOT NULL,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (job_id, receipt_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_archive_pending_lookup "
            "ON archive_pending(env, release_no)"
        )
    finally:
        conn.close()
    init_archive_db()


def init_archive_db() -> None:
    os.makedirs(os.path.dirname(ARCHIVE_DB_PATH) or ".", exist_ok=True)
    conn = connect(ARCHIVE_DB_PATH)
    try:
        # 结构与热库 receipts 对齐，另加 archived_at；UNIQUE 键保证同键只存一份
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS receipts (
                id INTEGER PRIMARY KEY,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                received_at TEXT NOT NULL,
                archived_at TEXT NOT NULL,
                UNIQUE(env, release_no)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_archive_receipts_key "
            "ON receipts(env, release_no)"
        )
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def fetch_dict(conn: sqlite3.Connection, rid: int) -> dict:
    row = conn.execute("SELECT * FROM receipts WHERE id=?", (rid,)).fetchone()
    return row_to_dict(row) if row is not None else {}


def find_archived(env: str, release_no: str) -> sqlite3.Row | None:
    """在归档库按幂等键查回执；归档库不可用时向上抛出由调用方处理。"""
    conn = connect(ARCHIVE_DB_PATH)
    try:
        return conn.execute(
            "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
        ).fetchone()
    finally:
        conn.close()


# ---------------- 归档核心 ----------------

def _resume_stale_jobs() -> None:
    """进程启动时把上次中断在 RUNNING 的任务续做完（重启收敛）。"""
    conn = connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT id FROM archive_jobs WHERE status=?", (JOB_RUNNING,)
        ).fetchall()
    finally:
        conn.close()
    for row in rows:
        try:
            run_archive_job(row["id"])
        except Exception as exc:  # noqa: BLE001 - 启动续做失败不致命，可稍后手动重试
            print(f"[archive] 启动续做任务 {row['id']} 失败：{exc}", flush=True)


def _set_job(conn: sqlite3.Connection, job_id: int, **fields) -> None:
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(
        f"UPDATE archive_jobs SET {cols}, updated_at=? WHERE id=?",
        (*fields.values(), now_iso(), job_id),
    )


def run_archive_job(job_id: int) -> dict:
    """执行（或续做）一个归档任务，直到无可搬行；返回任务最终状态。

    一致性要点：
    - 每行先在独立事务里写归档库并提交，再在热库事务里删除；
    - archive_pending 记录 PENDING -> COPIED -> DONE，崩溃后按状态续做；
    - 任何一步失败：任务置 FAILED（绝不误报完成），已复制的行仍可查，重试收敛。
    """
    with _archive_lock:
        conn = connect(DB_PATH)
        try:
            job = conn.execute(
                "SELECT * FROM archive_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if job is None:
                return {"id": job_id, "status": JOB_FAILED, "last_error": "任务不存在"}
            if job["status"] == JOB_DONE:
                return row_to_dict(job)
            _set_job(conn, job_id, status=JOB_RUNNING, last_error=None)
            simulate = job["simulate"]
            cutoff = job["cutoff"]

            while True:
                # 1) 把截止时间之前、尚未登记进暂存表的热库回执登记为 PENDING。
                #    排除已被其他在途任务登记的键，避免两个任务争抢同一行。
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute(
                        """
                        INSERT INTO archive_pending (job_id, receipt_id, env, release_no, state, updated_at)
                        SELECT ?, r.id, r.env, r.release_no, ?, ?
                        FROM receipts r
                        WHERE r.received_at <= ?
                          AND NOT EXISTS (
                              SELECT 1 FROM archive_pending p
                              WHERE p.env = r.env AND p.release_no = r.release_no
                          )
                        LIMIT ?
                        """,
                        (job_id, ROW_PENDING, now_iso(), cutoff, BATCH_LIMIT),
                    )
                    conn.execute("COMMIT")
                except sqlite3.Error:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    raise

                pending = conn.execute(
                    "SELECT * FROM archive_pending WHERE job_id=? AND state != ? ORDER BY receipt_id",
                    (job_id, ROW_DONE),
                ).fetchall()
                if not pending:
                    break

                for p in pending:
                    rid, env, rel = p["receipt_id"], p["env"], p["release_no"]
                    # 2) 复制进归档库（独立事务、独立库，先提交）
                    if p["state"] == ROW_PENDING:
                        hot = conn.execute(
                            "SELECT * FROM receipts WHERE id=?", (rid,)
                        ).fetchone()
                        if hot is None:
                            # 热库行已不在：可能上次已删除。若归档库确有此键，
                            # 直接把本行收束为 DONE；否则保留 PENDING 下轮再判。
                            try:
                                arch = find_archived(env, rel)
                            except sqlite3.Error as exc:
                                _set_job(conn, job_id, status=JOB_FAILED,
                                         last_error=f"归档库不可用：{exc}")
                                raise
                            if arch is not None:
                                conn.execute(
                                    "UPDATE archive_pending SET state=?, updated_at=? "
                                    "WHERE job_id=? AND receipt_id=?",
                                    (ROW_DONE, now_iso(), job_id, rid),
                                )
                            continue

                        archived = False
                        arch_conn = connect(ARCHIVE_DB_PATH)
                        try:
                            arch_conn.execute("BEGIN IMMEDIATE")
                            try:
                                # INSERT OR IGNORE：重试/重复任务下同键只存一份
                                arch_conn.execute(
                                    """
                                    INSERT OR IGNORE INTO receipts
                                        (id, env, release_no, result, content_hash,
                                         received_at, archived_at)
                                    VALUES (?,?,?,?,?,?,?)
                                    """,
                                    (hot["id"], hot["env"], hot["release_no"],
                                     hot["result"], hot["content_hash"],
                                     hot["received_at"], now_iso()),
                                )
                                arch_conn.execute("COMMIT")
                                archived = True
                            except sqlite3.Error:
                                try:
                                    arch_conn.execute("ROLLBACK")
                                except sqlite3.Error:
                                    pass
                                raise
                        finally:
                            arch_conn.close()
                        if not archived:
                            raise sqlite3.Error("归档库写入未成功")

                        conn.execute(
                            "UPDATE archive_pending SET state=?, updated_at=? "
                            "WHERE job_id=? AND receipt_id=?",
                            (ROW_COPIED, now_iso(), job_id, rid),
                        )

                        # 故障注入：已确认复制进归档库，但热库删除前失败
                        if simulate in VALID_ARCHIVE_SIMULATE:
                            _set_job(
                                conn, job_id, status=JOB_FAILED,
                                last_error=f"模拟故障（{simulate}）：行已复制进归档库，热库尚未删除",
                            )
                            return row_to_dict(
                                conn.execute(
                                    "SELECT * FROM archive_jobs WHERE id=?", (job_id,)
                                ).fetchone()
                            )

                    # 3) 从热库删除；删除前再次确认归档库确有此键（双保险，绝不丢）
                    arch = find_archived(env, rel)
                    if arch is None:
                        _set_job(conn, job_id, status=JOB_FAILED,
                                 last_error=f"删除前校验失败：归档库缺少 {env}/{rel}")
                        raise sqlite3.Error("archive copy missing before delete")

                    conn.execute("BEGIN IMMEDIATE")
                    try:
                        conn.execute(
                            "DELETE FROM receipts WHERE id=? AND env=? AND release_no=?",
                            (rid, env, rel),
                        )
                        conn.execute(
                            "UPDATE archive_pending SET state=?, updated_at=? "
                            "WHERE job_id=? AND receipt_id=?",
                            (ROW_DONE, now_iso(), job_id, rid),
                        )
                        conn.execute("COMMIT")
                    except sqlite3.Error:
                        try:
                            conn.execute("ROLLBACK")
                        except sqlite3.Error:
                            pass
                        raise

            # 4) 统计并收尾：所有暂存行都 DONE 才算完成
            total = conn.execute(
                "SELECT COUNT(*) AS c FROM archive_pending WHERE job_id=?", (job_id,)
            ).fetchone()["c"]
            done = conn.execute(
                "SELECT COUNT(*) AS c FROM archive_pending WHERE job_id=? AND state=?",
                (job_id, ROW_DONE),
            ).fetchone()["c"]
            if total != done:
                _set_job(conn, job_id, status=JOB_FAILED,
                         last_error=f"仍有 {total - done} 行未完成归档")
                raise sqlite3.Error("archive not fully converged")

            conn.execute("DELETE FROM archive_pending WHERE job_id=?", (job_id,))
            _set_job(conn, job_id, status=JOB_DONE, total=total, archived=done,
                     last_error=None)
            return row_to_dict(
                conn.execute("SELECT * FROM archive_jobs WHERE id=?", (job_id,)).fetchone()
            )
        except sqlite3.Error as exc:
            try:
                _set_job(conn, job_id, status=JOB_FAILED,
                         last_error=f"归档写入失败：{exc}")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()


def _run_archive_async(job_id: int) -> None:
    try:
        run_archive_job(job_id)
    except Exception as exc:  # noqa: BLE001 - 线程内兜底，任务已在库内标记 FAILED
        print(f"[archive] 任务 {job_id} 失败：{exc}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # 重启后自动续做上次中断的归档（守护线程，不阻塞启动）
    threading.Thread(target=_resume_stale_jobs, daemon=True).start()
    yield


app = FastAPI(title="审计接收端", lifespan=lifespan)


class ReceiptIn(BaseModel):
    env: str
    release_no: str
    result: str
    simulate: str | None = None  # lose_response | slow


class ArchiveIn(BaseModel):
    cutoff: str  # UTC 截止时间，如 2026-11-01T00:00 或 2026-11-01T00:00:00Z
    simulate: str | None = None  # fail_after_copy（仅演示/自测）
    wait: bool = True  # True=同步等结果；False=后台执行，轮询 /api/archive/jobs/{id}


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/receipts")
def create_receipt(body: ReceiptIn):
    env, release_no, result = body.env.strip(), body.release_no.strip(), body.result.strip()
    if not env or not release_no or not result:
        return JSONResponse(status_code=400, content={"detail": "env / release_no / result 均不能为空"})
    if len(env) > 64 or len(release_no) > 64 or len(result) > 64:
        return JSONResponse(status_code=400, content={"detail": "字段长度不能超过 64 字符"})
    if body.simulate is not None and body.simulate not in VALID_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"simulate 仅支持：{sorted(VALID_SIMULATE)}"})

    ch = content_hash(env, release_no, result)
    conn = connect(DB_PATH)
    try:
        # 0) 先查归档库：历史回执仍参与幂等/冲突判定，且不会被重新写回热库
        try:
            archived = find_archived(env, release_no)
        except sqlite3.Error as exc:
            return JSONResponse(status_code=500,
                                content={"detail": f"归档库暂不可用：{exc}"})
        if archived is not None:
            if archived["content_hash"] == ch:
                out = row_to_dict(archived)
                out["deduplicated"] = True
                out["location"] = "archive"
                return JSONResponse(status_code=200, content=out)
            return JSONResponse(status_code=409, content={
                "detail": "相同环境与发布号已存在不同内容（归档回执）",
                "existing": {**row_to_dict(archived), "location": "archive"},
            })

        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                if row["content_hash"] == ch:
                    # 同内容重试：幂等返回既有回执，绝不重复记账
                    out = row_to_dict(row)
                    out["deduplicated"] = True
                    out["location"] = "hot"
                    return JSONResponse(status_code=200, content=out)
                return JSONResponse(status_code=409, content={
                    "detail": "相同环境与发布号已存在不同内容",
                    "existing": {**row_to_dict(row), "location": "hot"},
                })

            # 该行正处于在途归档（已复制、待删除）：让调用方安全重试，
            # 不与归档 DELETE 抢 UNIQUE 键；归档库随后即可查到。
            in_flight = conn.execute(
                "SELECT 1 FROM archive_pending WHERE env=? AND release_no=? LIMIT 1",
                (env, release_no),
            ).fetchone()
            if in_flight is not None:
                conn.execute("COMMIT")
                return JSONResponse(status_code=503, content={
                    "detail": "该键正在归档搬迁中，请稍后重试",
                    "retryable": True,
                })

            # 事务内再查一次归档库：堵住「步骤 0 查归档库之后、拿到写锁之前，
            # 归档任务恰好把该行复制并删除完」的窄窗口，杜绝热库/归档库同键双份。
            # 此时本事务持有热库写锁，归档的热库 DELETE 无法插入执行，
            # 故本次再查若未命中，之后只可能撞上热库 UNIQUE（转 500 由调用方重试收敛）。
            try:
                arch2 = find_archived(env, release_no)
            except sqlite3.Error as exc:
                conn.execute("ROLLBACK")
                return JSONResponse(status_code=500,
                                    content={"detail": f"归档库暂不可用：{exc}"})
            if arch2 is not None:
                conn.execute("COMMIT")
                if arch2["content_hash"] == ch:
                    out = row_to_dict(arch2)
                    out["deduplicated"] = True
                    out["location"] = "archive"
                    return JSONResponse(status_code=200, content=out)
                return JSONResponse(status_code=409, content={
                    "detail": "相同环境与发布号已存在不同内容（归档回执）",
                    "existing": {**row_to_dict(arch2), "location": "archive"},
                })

            cur = conn.execute(
                "INSERT INTO receipts (env, release_no, result, content_hash, received_at) VALUES (?,?,?,?,?)",
                (env, release_no, result, ch, now_iso()),
            )
            rid = cur.lastrowid
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        receipt = fetch_dict(conn, rid)
        receipt["location"] = "hot"
        # —— 故障注入（仅演示）：记录已提交入库，但调用方拿不到回执 ——
        if body.simulate == "lose_response":
            return JSONResponse(status_code=500, content={"detail": "模拟故障：记录已入库，响应丢失"})
        if body.simulate == "slow":
            time.sleep(6)  # 长于发送端 3s 超时，回执实际已生成
        return JSONResponse(status_code=201, content=receipt)
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"接收端写入失败：{exc}"})
    finally:
        conn.close()


@app.get("/api/receipts")
def list_or_get_receipts(env: str | None = None, release_no: str | None = None):
    conn = connect(DB_PATH)
    try:
        if env is not None or release_no is not None:
            if not env or not release_no:
                return JSONResponse(status_code=400, content={"detail": "env 与 release_no 须同时提供"})
            # 热库优先；热库没有再查归档库：任意时刻同键只有一份可查
            row = conn.execute(
                "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is not None:
                out = row_to_dict(row)
                out["location"] = "hot"
                return out
            try:
                arch = find_archived(env, release_no)
            except sqlite3.Error as exc:
                return JSONResponse(status_code=500,
                                    content={"detail": f"归档库暂不可用：{exc}"})
            if arch is None:
                return JSONResponse(status_code=404, content={"detail": "无此记录"})
            out = row_to_dict(arch)
            out["location"] = "archive"
            return out

        # 列表：热库全量 + 归档库中热库已不存在的部分（去重，绝不重复显示）
        hot_rows = conn.execute("SELECT * FROM receipts ORDER BY id DESC").fetchall()
        hot_keys = {(r["env"], r["release_no"]) for r in hot_rows}
        items = [{**row_to_dict(r), "location": "hot"} for r in hot_rows]
        try:
            arch_conn = connect(ARCHIVE_DB_PATH)
            try:
                arch_rows = arch_conn.execute(
                    "SELECT * FROM receipts ORDER BY id DESC"
                ).fetchall()
            finally:
                arch_conn.close()
        except sqlite3.Error as exc:
            return JSONResponse(status_code=500,
                                content={"detail": f"归档库暂不可用：{exc}"})
        for r in arch_rows:
            if (r["env"], r["release_no"]) not in hot_keys:
                items.append({**row_to_dict(r), "location": "archive"})
        items.sort(key=lambda x: x["id"], reverse=True)
        return {"items": items}
    finally:
        conn.close()


# ---------------- 归档管理接口 ----------------

@app.post("/api/archive/jobs")
def create_archive_job(body: ArchiveIn):
    try:
        cutoff_dt = parse_utc(body.cutoff)
    except (ValueError, TypeError):
        return JSONResponse(status_code=400,
                            content={"detail": "cutoff 须为 UTC 时间，如 2026-11-01T00:00:00Z"})
    if body.simulate is not None and body.simulate not in VALID_ARCHIVE_SIMULATE:
        return JSONResponse(status_code=400,
                            content={"detail": f"simulate 仅支持：{sorted(VALID_ARCHIVE_SIMULATE)}"})

    conn = connect(DB_PATH)
    try:
        running = conn.execute(
            "SELECT id FROM archive_jobs WHERE status=?", (JOB_RUNNING,)
        ).fetchone()
        if running is not None:
            return JSONResponse(status_code=409, content={
                "detail": f"已有归档任务 #{running['id']} 正在执行",
                "job_id": running["id"],
            })
        now = now_iso()
        cur = conn.execute(
            """INSERT INTO archive_jobs (cutoff, status, total, archived, simulate, created_at, updated_at)
               VALUES (?,?,0,0,?,?,?)""",
            (cutoff_dt.isoformat(timespec="seconds"), JOB_RUNNING, body.simulate, now, now),
        )
        job_id = cur.lastrowid
    finally:
        conn.close()

    if body.wait:
        try:
            job = run_archive_job(job_id)
        except sqlite3.Error as exc:
            return JSONResponse(status_code=500,
                                content={"detail": f"归档失败（可重试，已复制回执不丢）：{exc}",
                                         "job_id": job_id})
        return JSONResponse(
            status_code=200 if job["status"] == JOB_DONE else 500,
            content=job,
        )

    threading.Thread(target=_run_archive_async, args=(job_id,), daemon=True).start()
    return JSONResponse(status_code=202, content={"id": job_id, "status": JOB_RUNNING})


@app.get("/api/archive/jobs")
def list_archive_jobs():
    conn = connect(DB_PATH)
    try:
        rows = conn.execute("SELECT * FROM archive_jobs ORDER BY id DESC").fetchall()
        return {"items": [row_to_dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/api/archive/jobs/{job_id}")
def get_archive_job(job_id: int):
    conn = connect(DB_PATH)
    try:
        row = conn.execute("SELECT * FROM archive_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            return JSONResponse(status_code=404, content={"detail": "无此归档任务"})
        out = row_to_dict(row)
        pending = conn.execute(
            "SELECT state, COUNT(*) AS c FROM archive_pending WHERE job_id=? GROUP BY state",
            (job_id,),
        ).fetchall()
        out["rows"] = {r["state"]: r["c"] for r in pending}
        return out
    finally:
        conn.close()


@app.post("/api/archive/jobs/{job_id}/retry")
def retry_archive_job(job_id: int):
    """续做/重试失败或中断的归档任务；已完成的任务幂等返回。"""
    conn = connect(DB_PATH)
    try:
        row = conn.execute("SELECT * FROM archive_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            return JSONResponse(status_code=404, content={"detail": "无此归档任务"})
        if row["status"] == JOB_RUNNING:
            return JSONResponse(status_code=409, content={
                "detail": "该任务仍在执行中", "job_id": job_id,
            })
        # 重试时清除故障注入，保证重试能够收敛
        conn.execute("UPDATE archive_jobs SET simulate=NULL, updated_at=? WHERE id=?",
                     (now_iso(), job_id))
    finally:
        conn.close()
    try:
        job = run_archive_job(job_id)
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500,
                            content={"detail": f"归档仍失败（可继续重试，回执不丢）：{exc}",
                                     "job_id": job_id})
    return JSONResponse(
        status_code=200 if job["status"] == JOB_DONE else 500,
        content=job,
    )


@app.get("/api/archive/stats")
def archive_stats():
    """热库/归档库计数，供网页展示热库收缩效果。"""
    conn = connect(DB_PATH)
    try:
        hot_count = conn.execute("SELECT COUNT(*) AS c FROM receipts").fetchone()["c"]
    finally:
        conn.close()
    try:
        arch_conn = connect(ARCHIVE_DB_PATH)
        try:
            archive_count = arch_conn.execute(
                "SELECT COUNT(*) AS c FROM receipts"
            ).fetchone()["c"]
        finally:
            arch_conn.close()
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"归档库暂不可用：{exc}"})
    return {"hot": hot_count, "archive": archive_count,
            "archive_db": ARCHIVE_DB_PATH}


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
