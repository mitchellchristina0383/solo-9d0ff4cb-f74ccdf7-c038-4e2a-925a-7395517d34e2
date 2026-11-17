"""发布值班发送端。

职责：登记“部署完成通知”，调用审计接收端接口，并仅凭接收端回执确认送达。
原则：
- 发送前先把“已发出、结果未知（待核）”落库，进程崩溃也不会丢失该事实；
- 只有拿到接收端内容一致的回执才置为“已确认”，绝不虚报成功；
- 丢回执 / 接收端不可达 / 发送端重启后，均可凭接收端回执核对补账。
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

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "/data/sender.db")
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://localhost:8001").rstrip("/")
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "3"))
STARTUP_RECONCILE = os.environ.get("STARTUP_RECONCILE", "1") == "1"
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

STATUS_PENDING = "PENDING"        # 待发送：已登记，尚未发送或已确认对方未记账
STATUS_UNCERTAIN = "UNCERTAIN"    # 待核：已发出但回执缺失/对方不可达，是否送达未知
STATUS_ACKED = "ACKED"            # 已确认：收到接收端内容一致的回执
STATUS_CONFLICT = "CONFLICT"      # 冲突：同环境同发布号在接收端已存在不同内容

VALID_SIMULATE = {"lose_response", "slow"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def content_hash(env: str, release_no: str, result: str) -> str:
    canonical = json.dumps(
        {"env": env, "release_no": release_no, "result": result},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                receipt_id INTEGER,
                receipt_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(env, release_no)
            )
            """
        )
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def fetch_dict(conn: sqlite3.Connection, nid: int) -> dict:
    row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
    return row_to_dict(row)


def set_fields(conn: sqlite3.Connection, nid: int, **fields) -> None:
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(
        f"UPDATE notifications SET {cols}, updated_at=? WHERE id=?",
        (*fields.values(), now_iso(), nid),
    )


# ---------------- 核心动作 ----------------

def do_send(nid: int, simulate: str | None = None) -> tuple[int, dict]:
    """发送（或重发）一条通知。只有拿到接收端内容一致的回执才置为已确认。"""
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
        if row is None:
            return 404, {"detail": "通知不存在"}
        if row["status"] == STATUS_ACKED:
            out = row_to_dict(row)
            out["note"] = "已确认，无需重复发送"
            return 200, out

        # 1) 先把“已发出、结果未知”落库：即使此刻进程崩溃，
        #    重启后也知道必须核对，而不是误以为未发或误报成功。
        conn.execute(
            "UPDATE notifications SET status=?, attempt_count=attempt_count+1, updated_at=? WHERE id=?",
            (STATUS_UNCERTAIN, now_iso(), nid),
        )

        # 2) 调用接收端
        payload = {"env": row["env"], "release_no": row["release_no"], "result": row["result"]}
        if simulate:
            payload["simulate"] = simulate
        try:
            resp = httpx.post(f"{RECEIVER_URL}/api/receipts", json=payload, timeout=HTTP_TIMEOUT)
        except httpx.HTTPError as exc:
            set_fields(conn, nid, status=STATUS_UNCERTAIN,
                       last_error=f"接收端不可达或回执丢失（{exc.__class__.__name__}）")
            return 200, fetch_dict(conn, nid)

        # 3) 只有接收端 2xx 回执且内容一致才算送达
        if resp.status_code in (200, 201):
            receipt = resp.json()
            if receipt.get("content_hash") == row["content_hash"]:
                set_fields(conn, nid, status=STATUS_ACKED, last_error=None,
                           receipt_id=receipt.get("id"), receipt_at=receipt.get("received_at"))
            else:
                set_fields(conn, nid, status=STATUS_CONFLICT,
                           last_error="回执内容与本地登记不一致")
        elif resp.status_code == 409:
            detail = resp.json().get("detail", "接收端已存在不同内容")
            set_fields(conn, nid, status=STATUS_CONFLICT, last_error=detail)
        else:
            set_fields(conn, nid, status=STATUS_UNCERTAIN,
                       last_error=f"接收端返回 {resp.status_code}，回执缺失")
        return 200, fetch_dict(conn, nid)
    finally:
        conn.close()


def do_reconcile(nid: int) -> tuple[int, dict]:
    """凭接收端回执核对一条通知的真实状态。"""
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
        if row is None:
            return 404, {"detail": "通知不存在"}
        try:
            resp = httpx.get(
                f"{RECEIVER_URL}/api/receipts",
                params={"env": row["env"], "release_no": row["release_no"]},
                timeout=HTTP_TIMEOUT,
            )
        except httpx.HTTPError:
            # 接收端暂不可查：保持现状（待核仍为待核），绝不虚报成功
            set_fields(conn, nid, last_error="核对待定：接收端暂不可达")
            out = fetch_dict(conn, nid)
            out["note"] = "接收端暂不可达，状态保持不变"
            return 200, out

        if resp.status_code == 404:
            # 接收端明确无此记录：未送达，可安全重发
            if row["status"] == STATUS_UNCERTAIN:
                set_fields(conn, nid, status=STATUS_PENDING, last_error=None)
            out = fetch_dict(conn, nid)
            out["note"] = "接收端无此记录，可安全重发"
            return 200, out
        if resp.status_code == 200:
            receipt = resp.json()
            if receipt.get("content_hash") == row["content_hash"]:
                set_fields(conn, nid, status=STATUS_ACKED, last_error=None,
                           receipt_id=receipt.get("id"), receipt_at=receipt.get("received_at"))
            else:
                set_fields(conn, nid, status=STATUS_CONFLICT,
                           last_error="接收端已存在同环境同发布号的不同内容")
            return 200, fetch_dict(conn, nid)

        set_fields(conn, nid, last_error=f"核对失败：接收端返回 {resp.status_code}")
        return 200, fetch_dict(conn, nid)
    finally:
        conn.close()


def reconcile_all_open() -> int:
    """核对所有未结案（待核/待发送）记录，返回处理条数。"""
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT id FROM notifications WHERE status IN (?, ?)",
            (STATUS_UNCERTAIN, STATUS_PENDING),
        ).fetchall()
    finally:
        conn.close()
    for r in rows:
        do_reconcile(r["id"])
    return len(rows)


def _startup_reconcile() -> None:
    """发送端重启后主动核对所有待核记录：只凭接收端回执定案。"""
    time.sleep(2)  # 等待接收端就绪
    try:
        n = reconcile_all_open()
        print(f"[startup] 重启核对完成，共处理 {n} 条待核/待发送记录", flush=True)
    except Exception as exc:  # noqa: BLE001 - 启动核对失败不致命，可稍后手动核对
        print(f"[startup] 重启核对失败（可稍后手动核对）: {exc}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if STARTUP_RECONCILE:
        threading.Thread(target=_startup_reconcile, daemon=True).start()
    yield


app = FastAPI(title="发布值班发送端", lifespan=lifespan)


class NotificationIn(BaseModel):
    env: str
    release_no: str
    result: str


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/notifications")
def create_notification(body: NotificationIn):
    """登记通知。同 (env, release_no) 同内容幂等去重，内容不同报 409 冲突。"""
    env, release_no, result = body.env.strip(), body.release_no.strip(), body.result.strip()
    if not env or not release_no or not result:
        return JSONResponse(status_code=400, content={"detail": "env / release_no / result 均不能为空"})
    if len(env) > 64 or len(release_no) > 64 or len(result) > 64:
        return JSONResponse(status_code=400, content={"detail": "字段长度不能超过 64 字符"})

    ch = content_hash(env, release_no, result)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM notifications WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                if row["content_hash"] == ch:
                    out = row_to_dict(row)
                    out["deduplicated"] = True
                    return JSONResponse(status_code=200, content=out)
                return JSONResponse(status_code=409, content={
                    "detail": "相同环境与发布号已存在不同内容",
                    "existing": row_to_dict(row),
                })
            now = now_iso()
            cur = conn.execute(
                """INSERT INTO notifications
                   (env, release_no, result, content_hash, status, attempt_count, created_at, updated_at)
                   VALUES (?,?,?,?,?,0,?,?)""",
                (env, release_no, result, ch, STATUS_PENDING, now, now),
            )
            nid = cur.lastrowid
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        return JSONResponse(status_code=201, content=fetch_dict(conn, nid))
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"本地写入失败：{exc}"})
    finally:
        conn.close()


@app.get("/api/notifications")
def list_notifications():
    conn = connect()
    try:
        rows = conn.execute("SELECT * FROM notifications ORDER BY id DESC").fetchall()
        return {"items": [row_to_dict(r) for r in rows]}
    finally:
        conn.close()


@app.post("/api/notifications/{nid}/send")
def send_notification(nid: int, simulate: str | None = Query(default=None)):
    if simulate is not None and simulate not in VALID_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"simulate 仅支持：{sorted(VALID_SIMULATE)}"})
    code, payload = do_send(nid, simulate)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/notifications/{nid}/reconcile")
def reconcile_notification(nid: int):
    code, payload = do_reconcile(nid)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/reconcile-all")
def reconcile_all_endpoint():
    n = reconcile_all_open()
    return {"reconciled": n}


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
