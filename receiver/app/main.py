"""审计接收端（本地演示用）。

接收“部署完成通知”，按 (env, release_no) 幂等入库，并提供回执查询。
- 相同 (env, release_no) 且内容一致的重试：返回既有回执，不新增记录；
- 相同 (env, release_no) 但内容不同：409 冲突；
- 故障注入（仅演示）：simulate=lose_response（入库后丢响应）/ slow（入库后慢响应）。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "/data/receiver.db")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
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
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def fetch_dict(conn: sqlite3.Connection, rid: int) -> dict:
    row = conn.execute("SELECT * FROM receipts WHERE id=?", (rid,)).fetchone()
    return row_to_dict(row)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="审计接收端", lifespan=lifespan)


class ReceiptIn(BaseModel):
    env: str
    release_no: str
    result: str
    simulate: str | None = None  # lose_response | slow


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
    conn = connect()
    try:
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
                    return JSONResponse(status_code=200, content=out)
                return JSONResponse(status_code=409, content={
                    "detail": "相同环境与发布号已存在不同内容",
                    "existing": row_to_dict(row),
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
    conn = connect()
    try:
        if env is not None or release_no is not None:
            if not env or not release_no:
                return JSONResponse(status_code=400, content={"detail": "env 与 release_no 须同时提供"})
            row = conn.execute(
                "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is None:
                return JSONResponse(status_code=404, content={"detail": "无此记录"})
            return row_to_dict(row)
        rows = conn.execute("SELECT * FROM receipts ORDER BY id DESC").fetchall()
        return {"items": [row_to_dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
