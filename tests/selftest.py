#!/usr/bin/env python3
"""关键情形端到端自测（仅依赖 Python 标准库）。

覆盖：
  S1  正常发送 → 已确认
  S2  同内容重试只记一次（两端均不重复）
  S3  同键不同内容 → 409 冲突（发送端登记与接收端直连均验证）
  S4  入库后丢回执 → 待核 → 重发去重 → 已确认
  S5  入库后丢回执 → 待核 → 核对补账 → 已确认
  S6  慢回执（发送端超时）→ 待核 → 核对 → 已确认
  S7  并发同内容提交 → 两端各仅一条
  S8  并发不同内容 → 仅一条，败者 409
  S9  接收端不可用 → 待核且不虚报成功（--lifecycle）
  S10 发送端重启（接收端停机中）→ 待核持久 → 恢复后凭回执补核（--lifecycle）
  S11 发送端重启（接收端正常）→ 启动自动核对凭回执结案（--lifecycle）

用法：
  python3 tests/selftest.py [--lifecycle]
环境变量：
  SENDER_URL / RECEIVER_URL   服务地址（默认 http://127.0.0.1:8000 / :8001）
  RN_LIFECYCLE=1              等价于 --lifecycle
  RN_HOOK_STOP_RECEIVER 等    生命周期钩子命令（默认 docker compose ...）
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

SENDER = os.environ.get("SENDER_URL", "http://127.0.0.1:8000").rstrip("/")
RECEIVER = os.environ.get("RECEIVER_URL", "http://127.0.0.1:8001").rstrip("/")
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = str(int(time.time()))
ENV = "itest"

PASSED, FAILED = [], []


def check(name, cond, extra=""):
    (PASSED if cond else FAILED).append(name)
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + ("" if cond else f"  | {extra}"), flush=True)


def req(method, url, body=None, timeout=20):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw.decode(errors="replace")}
        return e.code, payload
    except urllib.error.URLError as e:
        return 0, {"error": str(e)}


def sender_create(env, rel, result):
    return req("POST", SENDER + "/api/notifications",
               {"env": env, "release_no": rel, "result": result})


def sender_send(nid, simulate=None):
    url = f"{SENDER}/api/notifications/{nid}/send"
    if simulate:
        url += f"?simulate={simulate}"
    return req("POST", url)


def sender_reconcile(nid):
    return req("POST", f"{SENDER}/api/notifications/{nid}/reconcile")


def sender_rows(rel):
    _, d = req("GET", SENDER + "/api/notifications")
    return [x for x in d.get("items", []) if x["env"] == ENV and x["release_no"] == rel]


def receiver_rows(rel):
    _, d = req("GET", RECEIVER + "/api/receipts")
    return [x for x in d.get("items", []) if x["env"] == ENV and x["release_no"] == rel]


def hook(env_var, default_cmd):
    cmd = os.environ.get(env_var, default_cmd)
    print(f"  >> 钩子: {cmd}", flush=True)
    r = subprocess.run(cmd, shell=True, cwd=PROJECT_ROOT,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if r.returncode != 0:
        print(f"  !! 钩子返回码 {r.returncode}", flush=True)


def wait_up(base, tries=60):
    for _ in range(tries):
        code, _ = req("GET", base + "/api/health", timeout=3)
        if code == 200:
            return True
        time.sleep(0.5)
    return False


def key(tag):
    return f"R{RUN}-{tag}"


# ---------------- 场景 ----------------

def s1_basic():
    k = key("k1")
    code, n = sender_create(ENV, k, "success")
    check("S1 登记通知成功且初始为待发送", code in (200, 201) and n.get("status") == "PENDING", f"code={code} {n}")
    code, n2 = sender_send(n["id"])
    check("S1 发送后已确认且有回执号", n2.get("status") == "ACKED" and bool(n2.get("receipt_id")), f"{n2}")
    check("S1 接收端仅一条记录", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")


def s2_retry_dedup():
    k = key("k1")  # 与 S1 同键：同内容重试
    code, n = sender_create(ENV, k, "success")
    check("S2 同内容重复登记被幂等去重", code == 200 and n.get("deduplicated") is True, f"code={code} {n}")
    code, n2 = sender_send(n["id"])
    check("S2 重发后仍为已确认", n2.get("status") == "ACKED", f"{n2}")
    check("S2 发送端同键仅一条", len(sender_rows(k)) == 1)
    check("S2 接收端同键仍仅一条", len(receiver_rows(k)) == 1)


def s3_conflict():
    k = key("k1")
    code, n = sender_create(ENV, k, "failed")
    check("S3 同键不同内容登记报 409 冲突", code == 409, f"code={code} {n}")
    rows = sender_rows(k)
    check("S3 发送端原记录未被篡改", len(rows) == 1 and rows[0]["result"] == "success", f"{rows}")
    code, _ = req("POST", RECEIVER + "/api/receipts",
                  {"env": ENV, "release_no": k, "result": "failed"})
    check("S3 接收端直连同键不同内容也报 409", code == 409, f"code={code}")
    check("S3 接收端同键仍仅一条", len(receiver_rows(k)) == 1)


def s4_lost_response_resend():
    k = key("k4")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    code, n2 = sender_send(nid, simulate="lose_response")
    check("S4 丢回执后为待核而非成功", n2.get("status") == "UNCERTAIN", f"{n2}")
    check("S4 接收端其实已记账（1 条）", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")
    code, n3 = sender_send(nid)
    check("S4 重发后凭回执确认", n3.get("status") == "ACKED", f"{n3}")
    check("S4 重发未产生重复记录", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")


def s5_lost_response_reconcile():
    k = key("k5")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid, simulate="lose_response")
    check("S5 丢回执后状态为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    code, n3 = sender_reconcile(nid)
    check("S5 核对后凭接收端回执确认", n3.get("status") == "ACKED" and bool(n3.get("receipt_id")), f"{n3}")
    check("S5 接收端仍仅一条", len(receiver_rows(k)) == 1)


def s6_slow_response():
    k = key("k6")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    code, n2 = sender_send(nid, simulate="slow")
    check("S6 慢回执超时后为待核", n2.get("status") == "UNCERTAIN", f"{n2}")
    code, n3 = sender_reconcile(nid)
    check("S6 核对后确认", n3.get("status") == "ACKED", f"{n3}")
    check("S6 接收端仅一条", len(receiver_rows(k)) == 1)


def s7_concurrent_same():
    k = key("k7")
    results = []

    def worker():
        try:
            c1, n1 = sender_create(ENV, k, "success")
            c2, n2 = sender_send(n1["id"])
            results.append((c1, c2, n2.get("status")))
        except Exception as e:  # noqa: BLE001
            results.append(("EXC", str(e), None))

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("S7 并发同内容：无线程异常", all(r[0] != "EXC" for r in results), f"{results}")
    check("S7 并发同内容：发送端仅一条", len(sender_rows(k)) == 1, f"{sender_rows(k)}")
    check("S7 并发同内容：接收端仅一条", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")
    rows = sender_rows(k)
    check("S7 并发同内容：最终已确认", bool(rows) and rows[0]["status"] == "ACKED", f"{rows}")


def s8_concurrent_diff():
    k = key("k8")
    codes = []

    def worker(i):
        content = "success" if i % 2 == 0 else "failed"
        c, _ = sender_create(ENV, k, content)
        codes.append(c)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ok = sum(1 for c in codes if c in (200, 201))
    conflict = sum(1 for c in codes if c == 409)
    check("S8 并发不同内容：一组入库一组 409", ok == 5 and conflict == 5, f"codes={codes}")
    rows = sender_rows(k)
    check("S8 并发不同内容：发送端仅一条", len(rows) == 1, f"{rows}")
    if rows:
        code, n2 = sender_send(rows[0]["id"])
        check("S8 胜出发送后已确认", n2.get("status") == "ACKED", f"{n2}")
        rrows = receiver_rows(k)
        check("S8 接收端仅一条且内容与发送端一致",
              len(rrows) == 1 and rrows[0]["result"] == rows[0]["result"], f"{rrows} vs {rows}")


def s9_receiver_down():
    k = key("k9")
    hook("RN_HOOK_STOP_RECEIVER", "docker compose stop receiver")
    time.sleep(1)
    code, n = sender_create(ENV, k, "success")
    check("S9 接收端停机时本地可登记", code in (200, 201), f"code={code} {n}")
    nid = n["id"]
    code, n2 = sender_send(nid)
    check("S9 接收端不可达 → 待核而非成功", n2.get("status") == "UNCERTAIN", f"{n2}")
    code, n3 = sender_reconcile(nid)
    check("S9 不可达时核对仍保持待核（不虚报）", n3.get("status") == "UNCERTAIN", f"{n3}")
    return nid


def s10_sender_restart_while_down(nid_k9):
    k = key("k10")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid)  # 接收端仍停机 → 待核
    check("S10 停机期间发送为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    hook("RN_HOOK_RESTART_SENDER", "docker compose restart sender")
    if not wait_up(SENDER):
        check("S10 发送端重启后恢复服务", False)
        return
    time.sleep(4)  # 等启动核对线程跑完（接收端仍停机，应保持待核）
    rows = sender_rows(k)
    check("S10 重启后待核状态持久（未丢未虚报）", bool(rows) and rows[0]["status"] == "UNCERTAIN", f"{rows}")
    hook("RN_HOOK_START_RECEIVER", "docker compose start receiver")
    if not wait_up(RECEIVER):
        check("S10 接收端恢复", False)
        return
    code, n4 = sender_reconcile(nid)
    check("S10 恢复后核对判定未送达（回退待发送）", n4.get("status") == "PENDING", f"{n4}")
    code, n4b = sender_send(nid)
    check("S10 安全重发后确认且接收端仅一条",
          n4b.get("status") == "ACKED" and len(receiver_rows(k)) == 1, f"{n4b}")
    code, n5 = sender_reconcile(nid_k9)
    check("S10 此前停机期间的通知核对同为未送达", n5.get("status") == "PENDING", f"{n5}")
    code, n5b = sender_send(nid_k9)
    check("S10 补发后确认", n5b.get("status") == "ACKED", f"{n5b}")
    check("S10 接收端两键各仅一条",
          len(receiver_rows(key("k9"))) == 1 and len(receiver_rows(k)) == 1)


def s11_auto_reconcile_after_restart():
    k = key("k11")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid, simulate="lose_response")
    check("S11 重启前为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    hook("RN_HOOK_RESTART_SENDER", "docker compose restart sender")
    if not wait_up(SENDER):
        check("S11 发送端重启后恢复服务", False)
        return
    final = None
    for _ in range(20):  # 启动核对线程约 2s 后触发
        rows = sender_rows(k)
        if rows and rows[0]["status"] == "ACKED":
            final = rows[0]
            break
        time.sleep(0.5)
    check("S11 重启后自动凭接收端回执结案", final is not None, f"{sender_rows(k)}")
    check("S11 接收端仍仅一条", len(receiver_rows(k)) == 1)


def main():
    lifecycle = "--lifecycle" in sys.argv or os.environ.get("RN_LIFECYCLE") == "1"
    print(f"发送端 {SENDER}  接收端 {RECEIVER}  lifecycle={lifecycle}", flush=True)
    if not wait_up(SENDER) or not wait_up(RECEIVER):
        print("服务未就绪，请先启动两端。")
        sys.exit(2)

    s1_basic()
    s2_retry_dedup()
    s3_conflict()
    s4_lost_response_resend()
    s5_lost_response_reconcile()
    s6_slow_response()
    s7_concurrent_same()
    s8_concurrent_diff()
    if lifecycle:
        nid_k9 = s9_receiver_down()
        s10_sender_restart_while_down(nid_k9)
        s11_auto_reconcile_after_restart()

    print(f"\n通过 {len(PASSED)} 项，失败 {len(FAILED)} 项")
    if FAILED:
        print("失败项：")
        for name in FAILED:
            print(f"  - {name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
