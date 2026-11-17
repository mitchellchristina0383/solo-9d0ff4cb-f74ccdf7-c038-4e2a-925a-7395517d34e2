#!/usr/bin/env python3
"""归档高压并发补充测试（标准库）：
- 多线程同时：归档任务重试、同键/异键 POST、查询；
- 结束后校验：无丢失、无重复、无幽灵；热库+归档库计数自洽。
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

RECEIVER = os.environ.get("RECEIVER_URL", "http://127.0.0.1:8001").rstrip("/")
RUN = str(int(time.time()))
ENV = "stress"
PASSED, FAILED = [], []


def check(name, cond, extra=""):
    (PASSED if cond else FAILED).append(name)
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + ("" if cond else f" | {extra}"), flush=True)


def req(method, url, body=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except json.JSONDecodeError:
            return e.code, {}
    except urllib.error.URLError as e:
        return 0, {"error": str(e)}


def k(tag):
    return f"S{RUN}-{tag}"


def post(tag, result="success"):
    return req("POST", RECEIVER + "/api/receipts",
               {"env": ENV, "release_no": k(tag), "result": result})


def get(tag):
    q = urllib.parse.urlencode({"env": ENV, "release_no": k(tag)})
    return req("GET", RECEIVER + "/api/receipts?" + q)


errors = []
created_tags = [f"c{i}" for i in range(20)]
for t in created_tags:
    post(t)


def archiver():
    for _ in range(6):
        c, j = req("POST", RECEIVER + "/api/archive/jobs",
                   {"cutoff": "2999-01-01T00:00:00Z", "wait": True})
        if c not in (200, 409, 500):
            errors.append(("archive", c, j))
        time.sleep(0.05)


def same_key_hammer():
    for _ in range(40):
        c, r = post("c0")  # 与归档争抢的热键
        if c not in (200, 201, 409, 503):
            errors.append(("same", c, r))
        time.sleep(0.01)


def conflict_hammer():
    for _ in range(20):
        c, _ = post("c1", "failed")  # 异内容 → 必须恒 409
        if c != 409:
            errors.append(("conflict", c))
        time.sleep(0.01)


def new_writer():
    for i in range(20):
        c, r = post(f"n{i}")
        if c not in (200, 201, 200, 503):
            errors.append(("new", c, r))
        time.sleep(0.01)


def reader():
    for _ in range(60):
        c, items = req("GET", RECEIVER + "/api/receipts")
        if c != 200:
            errors.append(("read", c))
        else:
            seen = {}
            for it in items.get("items", []):
                if it["env"] != ENV:
                    continue
                seen.setdefault(it["release_no"], []).append(it)
            dups = {kk: v for kk, v in seen.items() if len(v) > 1}
            if dups:
                errors.append(("dup", dups))
                break
        time.sleep(0.02)


threads = [threading.Thread(target=f) for f in
           (archiver, same_key_hammer, conflict_hammer, new_writer, reader)]
for t in threads:
    t.start()
for t in threads:
    t.join()

check("高压交错期间无非法响应/重复显示", not errors, f"{errors[:5]}")

# 收敛：再做一次归档把剩余热库全部搬走
for _ in range(10):
    c, j = req("POST", RECEIVER + "/api/archive/jobs",
               {"cutoff": "2999-01-01T00:00:00Z", "wait": True})
    if c == 200 and j.get("status") == "DONE":
        break
    time.sleep(0.3)

c, items = req("GET", RECEIVER + "/api/receipts")
all_items = [x for x in items["items"] if x["env"] == ENV]
keys = [x["release_no"] for x in all_items]
check("每个键全局仅一份（无重复无丢失）", len(keys) == len(set(keys)),
      f"total={len(keys)} unique={len(set(keys))}")

for t in created_tags:
    c, r = get(t)
    check(f"键 {t} 可查且内容未变", c == 200 and r["result"] == "success", f"{c} {r}")

c, r = get("c1")
check("c1 原内容 success 未被冲突请求篡改", c == 200 and r["result"] == "success", f"{r}")
c, _ = post("c1", "failed")
check("c1 异内容仍稳定 409", c == 409, f"{c}")

st = req("GET", RECEIVER + "/api/archive/stats")[1]
check("热库+归档库计数与列表自洽",
      st["hot"] + st["archive"] == len(items["items"]), f"{st} vs {len(items['items'])}")
check("高压后热库已收缩为 0", st["hot"] == 0, f"{st}")

jobs = req("GET", RECEIVER + "/api/archive/jobs")[1]["items"]
check("不存在误报完成的失败任务（FAILED 与 DONE 状态明确）",
      all(j["status"] in ("DONE", "FAILED", "RUNNING") for j in jobs)
      and not any(j["status"] == "DONE" and j["archived"] != j["total"] for j in jobs),
      f"{jobs}")

print(f"\n通过 {len(PASSED)} 项，失败 {len(FAILED)} 项")
sys.exit(1 if FAILED else 0)
