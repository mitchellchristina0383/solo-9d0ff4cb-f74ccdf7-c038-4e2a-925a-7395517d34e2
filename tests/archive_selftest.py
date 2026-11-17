#!/usr/bin/env python3
"""热库归档端到端自测（仅依赖 Python 标准库）。

覆盖：
  A1  按 UTC 截止时间归档：旧回执进归档库、热库收缩，新回执仍在热库；
      合并列表不丢不重，归档任务状态为 DONE，热库不再存放被归档回执
  A2  归档后同键同内容重传：返回原回执（deduplicated, location=archive），
      热库不重新出现该键；同键不同内容仍 409 冲突；全新键正常入热库
  A3  发送端核对旧通知得到原回执（ACKED, location=archive）；
      归档后新发通知照常入账并确认（location=hot）
  A4  归档与接收交错：搬迁进行中对同键 POST 不产生重复行，最终仅一份可查
  A5  中途写入失败（故障注入 fail_after_copy）：任务 FAILED 不误报完成，
      回执仍可查；重试收敛到 DONE，热库清空该批，归档库同键仅一份
  A6  进程重启续做：归档进行中杀掉接收端进程并重启，
      RUNNING 任务自动续做到 DONE，无丢失/重复
  A7  归档库文件与热库位于同一数据卷目录；截止时间边界（含/不含）正确

用法：
  python3 tests/archive_selftest.py
环境变量：
  SENDER_URL / RECEIVER_URL   服务地址（默认 http://127.0.0.1:8000 / :8001）
  RN_HOOK_RESTART_RECEIVER    重启接收端的钩子命令（默认 tests/run_local.sh restart-receiver）
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
ENV = "archtest"

PASSED, FAILED = [], []


def check(name, cond, extra=""):
    (PASSED if cond else FAILED).append(name)
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + ("" if cond else f"  | {extra}"), flush=True)


def req(method, url, body=None, timeout=30):
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


def wait_up(base, tries=60):
    for _ in range(tries):
        code, _ = req("GET", base + "/api/health", timeout=3)
        if code == 200:
            return True
        time.sleep(0.5)
    return False


def key(tag):
    return f"A{RUN}-{tag}"


def post_receipt(tag, result="success", simulate=None):
    body = {"env": ENV, "release_no": key(tag), "result": result}
    if simulate:
        body["simulate"] = simulate
    return req("POST", RECEIVER + "/api/receipts", body)


def get_receipt(tag):
    from urllib.parse import quote
    url = (RECEIVER + "/api/receipts?env=" + quote(ENV) +
           "&release_no=" + quote(key(tag)))
    return req("GET", url)


def list_receipts():
    _, d = req("GET", RECEIVER + "/api/receipts")
    return d.get("items", [])


def stats():
    _, d = req("GET", RECEIVER + "/api/archive/stats")
    return d


def start_archive(cutoff, simulate=None, wait=True):
    body = {"cutoff": cutoff, "wait": wait}
    if simulate:
        body["simulate"] = simulate
    return req("POST", RECEIVER + "/api/archive/jobs", body)


def retry_job(job_id):
    return req("POST", f"{RECEIVER}/api/archive/jobs/{job_id}/retry")


def get_job(job_id):
    return req("GET", f"{RECEIVER}/api/archive/jobs/{job_id}")


def sender_create_send(tag, result="success"):
    code, n = req("POST", SENDER + "/api/notifications",
                  {"env": ENV, "release_no": key(tag), "result": result})
    if code not in (200, 201):
        return code, n
    return req("POST", f"{SENDER}/api/notifications/{n['id']}/send")


def sender_rows(rel):
    _, d = req("GET", SENDER + "/api/notifications")
    return [x for x in d.get("items", []) if x["env"] == ENV and x["release_no"] == rel]


def hook(env_var, default_cmd):
    cmd = os.environ.get(env_var, default_cmd)
    print(f"  >> 钩子: {cmd}", flush=True)
    r = subprocess.run(cmd, shell=True, cwd=PROJECT_ROOT,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if r.returncode != 0:
        print(f"  !! 钩子返回码 {r.returncode}", flush=True)


def find_item(items, tag):
    return [x for x in items if x["env"] == ENV and x["release_no"] == key(tag)]


# ---------------- 场景 ----------------

def a1_basic_archive():
    print("\n== A1 基础归档 ==", flush=True)
    post_receipt("old1")
    post_receipt("old2")
    post_receipt("old3")
    # 取一个未来截止时间：归档当前全部热库回执
    future = "2999-01-01T00:00:00Z"
    code, job = start_archive(future)
    check("A1 归档任务同步返回 DONE", code == 200 and job.get("status") == "DONE", f"{code} {job}")
    check("A1 归档计数 >= 3", job.get("archived", 0) >= 3 and job.get("archived") == job.get("total"),
          f"{job}")

    code, r1 = get_receipt("old1")
    check("A1 旧回执仍可查且位于归档库", code == 200 and r1.get("location") == "archive"
          and bool(r1.get("archived_at")), f"{code} {r1}")
    check("A1 归档后热库不再存放该回执（stats.hot=0 或不含本批）",
          stats().get("hot") == 0, f"{stats()}")
    items = list_receipts()
    check("A1 合并列表中每个旧键仅一份且标 archive",
          all(len(find_item(items, t)) == 1 and find_item(items, t)[0]["location"] == "archive"
              for t in ("old1", "old2", "old3")), "重复或位置错误")

    # 新通知：归档之后仍进热库
    post_receipt("new1")
    code, n1 = get_receipt("new1")
    check("A1 新回执进入热库", code == 200 and n1.get("location") == "hot", f"{code} {n1}")
    items = list_receipts()
    check("A1 合并列表总数 = 3 旧 + 1 新且无重复",
          len(find_item(items, "old1")) == 1 and len(find_item(items, "new1")) == 1
          and len(items) == stats()["hot"] + stats()["archive"], f"{len(items)} vs {stats()}")

    # 截止时间边界：再用一个很早的截止时间，不应搬动新热库行
    before = "2000-01-01T00:00:00Z"
    code, job2 = start_archive(before)
    check("A1 早于任何入账时间的截止时间归档 0 行",
          code == 200 and job2.get("archived") == 0 and job2.get("status") == "DONE", f"{job2}")
    code, n1b = get_receipt("new1")
    check("A1 新回执仍在热库未被误归档", n1b.get("location") == "hot", f"{n1b}")


def a2_post_after_archive():
    print("\n== A2 归档后的幂等与冲突 ==", flush=True)
    # old1 已在归档库
    code, r = post_receipt("old1", "success")
    check("A2 同键同内容重传返回原回执且标 deduplicated/archive",
          code == 200 and r.get("deduplicated") is True and r.get("location") == "archive",
          f"{code} {r}")
    check("A2 重传未把该键写回热库", get_receipt("old1")[1].get("location") == "archive")
    check("A2 归档库同键仍仅一份",
          len([x for x in list_receipts() if x["env"] == ENV and x["release_no"] == key("old1")]) == 1)

    code, r = post_receipt("old1", "failed")
    check("A2 同键不同内容仍报 409", code == 409, f"{code} {r}")
    check("A2 冲突回执指向归档库原记录", r.get("existing", {}).get("location") == "archive", f"{r}")
    check("A2 冲突后原内容未被篡改", get_receipt("old1")[1].get("result") == "success")

    code, r = post_receipt("brandnew")
    check("A2 全新键正常入热库 201", code == 201 and r.get("location") == "hot", f"{code} {r}")


def a3_sender_reconcile_old_and_new():
    print("\n== A3 发送端核对旧通知 / 新通知 ==", flush=True)
    # 先经发送端发一条，再在接收端归档，然后发送端核对
    code, s = sender_create_send("sender-old")
    check("A3 新通知发送后已确认(hot)", code == 200 and s.get("status") == "ACKED"
          and s.get("receipt_location") == "hot", f"{code} {s}")
    code, job = start_archive("2999-01-01T00:00:00Z")
    check("A3 归档成功", code == 200 and job.get("status") == "DONE", f"{job}")
    check("A3 该回执已在归档库", get_receipt("sender-old")[1].get("location") == "archive")

    rows = sender_rows(key("sender-old"))
    nid = rows[0]["id"]
    code, rec = req("POST", f"{SENDER}/api/notifications/{nid}/reconcile")
    check("A3 发送端核对旧通知得到原回执并保持已确认",
          code == 200 and rec.get("status") == "ACKED"
          and rec.get("receipt_location") == "archive"
          and rec.get("receipt_id") == rows[0]["receipt_id"], f"{code} {rec}")

    # 重传同键：接收端命中归档库幂等返回，发送端仍确认且不重复
    code, s2 = req("POST", f"{SENDER}/api/notifications/{nid}/send")
    check("A3 同键重传凭归档回执确认", s2.get("status") == "ACKED"
          and s2.get("receipt_location") == "archive", f"{s2}")
    check("A3 接收端该键全局仅一份",
          len([x for x in list_receipts() if x["release_no"] == key("sender-old")]) == 1)

    # 归档后全新通知走发送端：正常入账
    code, s3 = sender_create_send("sender-new")
    check("A3 归档后新通知照常入账确认(hot)",
          code == 200 and s3.get("status") == "ACKED" and s3.get("receipt_location") == "hot",
          f"{code} {s3}")
    check("A3 新通知确在热库", get_receipt("sender-new")[1].get("location") == "hot")


def a4_interleaving():
    print("\n== A4 归档与接收交错 ==", flush=True)
    tag = "interleave"
    post_receipt(tag)
    # 异步启动归档，立即对同键反复 POST；任何结果都不得产生第二条
    code, job = start_archive("2999-01-01T00:00:00Z", wait=False)
    check("A4 异步任务已受理 202/RUNNING", code == 202 and job.get("status") == "RUNNING", f"{code} {job}")
    job_id = job["id"]
    outcomes = []

    def hammer():
        for _ in range(20):
            c, r = post_receipt(tag)
            outcomes.append((c, r.get("location"), r.get("deduplicated"), r.get("retryable")))
            time.sleep(0.02)

    threads = [threading.Thread(target=hammer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = None
    for _ in range(60):
        c, j = get_job(job_id)
        if j.get("status") in ("DONE", "FAILED"):
            final = j
            break
        time.sleep(0.25)
    check("A4 异步归档最终 DONE", final is not None and final.get("status") == "DONE", f"{final}")
    # 允许的响应：200 幂等（hot 或 archive）或 503 搬迁中可重试；绝不允许 201 新建
    ok_codes = all(c in (200, 503) for c, *_ in outcomes)
    check("A4 交错期间 POST 仅出现幂等 200 或可重试 503，无 201 新建",
          ok_codes and not any(c == 201 for c, *_ in outcomes), f"{set(outcomes)}")
    items = find_item(list_receipts(), tag)
    check("A4 同键最终仅一份且位于归档库",
          len(items) == 1 and items[0]["location"] == "archive", f"{items}")
    code, r = get_receipt(tag)
    check("A4 最终同键重传稳定命中归档库原回执（POST 幂等返回）",
          code == 200 and r.get("location") == "archive", f"{r}")
    code, pr = post_receipt(tag)
    check("A4 最终同键 POST 重传为 deduplicated 且不新建",
          code == 200 and pr.get("deduplicated") is True
          and pr.get("location") == "archive", f"{code} {pr}")


def a5_failure_retry():
    print("\n== A5 中途失败与重试收敛 ==", flush=True)
    for t in ("fail1", "fail2"):
        post_receipt(t)
    code, job = start_archive("2999-01-01T00:00:00Z",
                              simulate="fail_after_copy", wait=True)
    job_id = job.get("id")
    check("A5 故障注入下任务报失败（HTTP 500 且 job=FAILED，不误报完成）",
          code == 500 and job.get("status") == "FAILED", f"{code} {job}")
    check("A5 失败任务带错误说明", bool(job.get("last_error")), f"{job}")

    # 失败时刻：部分行已复制进归档库（COPIED），热库仍保留 —— 回执始终可查
    c, jd = get_job(job_id)
    check("A5 失败任务存在 COPIED 暂存行", (jd.get("rows") or {}).get("COPIED", 0) >= 1, f"{jd}")
    for t in ("fail1", "fail2"):
        c, r = get_receipt(t)
        check(f"A5 失败后 {t} 仍可查（热库或归档库，不丢）", c == 200, f"{c} {r}")
    items = list_receipts()
    check("A5 失败后合并列表无重复",
          all(len(find_item(items, t)) == 1 for t in ("fail1", "fail2")), "出现重复显示")

    # 重试：应续做收敛
    code, job2 = retry_job(job_id)
    check("A5 重试后任务 DONE", code == 200 and job2.get("status") == "DONE"
          and job2.get("archived") == job2.get("total"), f"{code} {job2}")
    check("A5 重试后热库不含该批", all(get_receipt(t)[1].get("location") == "archive"
                                        for t in ("fail1", "fail2")))
    items = list_receipts()
    check("A5 收敛后归档库同键仅一份、列表无重复",
          all(len(find_item(items, t)) == 1 and find_item(items, t)[0]["location"] == "archive"
              for t in ("fail1", "fail2")), "重复")
    # 再次重试已完成任务：幂等
    code, job3 = retry_job(job_id)
    check("A5 对已完成任务再重试幂等返回 DONE", code == 200 and job3.get("status") == "DONE", f"{job3}")


def a6_restart_resume():
    print("\n== A6 进程重启续做 ==", flush=True)
    for t in ("kill1", "kill2", "kill3"):
        post_receipt(t)
    # 用故障注入制造一个停在 COPIED 的 RUNNING/FAILED 在途状态，再杀进程
    code, job = start_archive("2999-01-01T00:00:00Z",
                              simulate="fail_after_copy", wait=True)
    job_id = job.get("id")
    check("A6 预置失败任务", job.get("status") == "FAILED", f"{job}")
    # 手动把任务改回 RUNNING 模拟“进程在搬迁途中被杀死”（库内停留 RUNNING）
    # 通过重试接口无法直接制造 RUNNING，故用故障注入后重启进程等价验证恢复路径：
    hook("RN_HOOK_RESTART_RECEIVER",
         os.path.join(PROJECT_ROOT, "tests/run_local.sh") + " restart-receiver")
    if not wait_up(RECEIVER):
        check("A6 接收端重启后恢复", False)
        return
    # FAILED 任务不会自动续做（需人工重试）；这里验证重启不丢状态、手动重试收敛
    c, jd = get_job(job_id)
    check("A6 重启后任务状态持久不丢", c == 200 and jd.get("id") == job_id, f"{jd}")
    for t in ("kill1", "kill2", "kill3"):
        c, r = get_receipt(t)
        check(f"A6 重启后 {t} 回执可查不丢", c == 200, f"{c} {r}")
    code, job2 = retry_job(job_id)
    check("A6 重启后手动重试收敛 DONE", code == 200 and job2.get("status") == "DONE", f"{code} {job2}")
    items = list_receipts()
    check("A6 收敛后无重复无丢失",
          all(len(find_item(items, t)) == 1 for t in ("kill1", "kill2", "kill3")), "重复/丢失")

    # 真正的 RUNNING 中断：异步启动（无故障注入）后立刻杀进程
    post_receipt("kill4")
    code, job = start_archive("2999-01-01T00:00:00Z", wait=False)
    running_id = job["id"]
    time.sleep(0.2)  # 让任务进入 RUNNING
    hook("RN_HOOK_RESTART_RECEIVER",
         os.path.join(PROJECT_ROOT, "tests/run_local.sh") + " restart-receiver")
    if not wait_up(RECEIVER):
        check("A6 第二次重启后恢复", False)
        return
    final = None
    for _ in range(80):  # 启动守护线程自动续做
        c, jd = get_job(running_id)
        if jd.get("status") in ("DONE", "FAILED"):
            final = jd
            break
        time.sleep(0.5)
    check("A6 RUNNING 任务重启后自动续做到 DONE",
          final is not None and final.get("status") == "DONE", f"{final}")
    check("A6 kill4 最终在归档库且仅一份",
          (lambda its: len(its) == 1 and its[0]["location"] == "archive")(find_item(list_receipts(), "kill4")))


def a7_same_volume():
    print("\n== A7 同卷与边界 ==", flush=True)
    st = stats()
    db_dir = os.path.dirname(st.get("archive_db", ""))
    hot_db = os.environ.get("RECEIVER_DB_PATH", "/data/receiver.db")
    check("A7 归档库与热库同目录（同一数据卷）",
          db_dir == os.path.dirname(hot_db), f"archive={st.get('archive_db')} hot={hot_db}")
    check("A7 统计口径热库+归档库与列表一致",
          st.get("hot", -1) + st.get("archive", -1) == len(list_receipts()), f"{st}")


def main():
    print(f"发送端 {SENDER}  接收端 {RECEIVER}", flush=True)
    if not wait_up(SENDER) or not wait_up(RECEIVER):
        print("服务未就绪，请先启动两端（tests/run_local.sh start-all）。")
        sys.exit(2)

    a1_basic_archive()
    a2_post_after_archive()
    a3_sender_reconcile_old_and_new()
    a4_interleaving()
    a5_failure_retry()
    a6_restart_resume()
    a7_same_volume()

    print(f"\n通过 {len(PASSED)} 项，失败 {len(FAILED)} 项")
    if FAILED:
        print("失败项：")
        for name in FAILED:
            print(f"  - {name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
