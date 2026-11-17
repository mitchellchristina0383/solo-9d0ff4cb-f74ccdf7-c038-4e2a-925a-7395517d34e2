# 部署完成通知 · 幂等送达 + 热库归档演示系统

发布值班员通过网页向审计端发送「部署完成通知」。网络超时时无法判断对方是否已记账，
盲目重发又会产生重复记录。本系统用 **FastAPI + SQLite** 实现两端（各自独立数据库），
以「**幂等键 + 回执核对**」解决该问题：

- 相同「环境 + 发布号」的同内容重试，两端都只记一次；
- 同键不同内容 → **409 冲突**；
- 丢回执 / 接收端不可达 / 发送端重启 → 状态保持 **待核**，**绝不虚报成功**，
  恢复后凭接收端回执核对定案（已入账→已确认；未入账→回退待发送，可安全重发）。

审计台账管理员还可在**接收端网页按 UTC 截止时间把旧回执归档到同一数据卷内的独立
SQLite 归档库**以缩小热库；归档全程不丢回执、不重复显示、不误报完成，归档后历史
发布的回执核对与幂等语义保持不变。

## 目录结构

```
├── docker-compose.yml      # 一键启动两端（归档库与热库同挂一个命名卷）
├── sender/                 # 发送端（值班网页 + 发送/核对逻辑），端口 8000
│   ├── Dockerfile
│   └── app/{main.py, static/index.html}
├── receiver/               # 审计接收端（回执台账 + 热库归档 + 故障注入），端口 8001
│   ├── Dockerfile
│   └── app/{main.py, static/index.html}
└── tests/
    ├── selftest.py         # 42 项送达/幂等/重启关键情形自测（仅标准库）
    ├── archive_selftest.py # 51 项归档自测：截止归档、幂等/冲突、交错、失败恢复、重启续做
    ├── stress_archive.py   # 归档高压并发补充测试（多线程争抢 + 计数自洽）
    └── run_local.sh        # 无 Docker 时的本地进程运行器
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- 发送端网页：http://localhost:8000 （填写环境、发布号、结果，提交并发送，查看接收状态）
- 接收端台账：http://localhost:8001 （回执台账、按环境/发布号查询、发起与查看归档）

两端各持有独立 SQLite 库（compose 命名卷 `sender_data` / `receiver_data`）。接收端的
热库 `receiver.db` 与归档库 `archive.db` **位于同一个 `receiver_data` 卷内**。

无 Docker 的本地环境（需 Python 3.11+ 及 fastapi/uvicorn/httpx）：

```bash
tests/run_local.sh start-all     # 两个独立进程 + 热库/归档库/发送库三个 db 文件
tests/run_local.sh stop-all
```

## 状态机（发送端）

| 状态 | 含义 | 进入条件 |
|---|---|---|
| 待发送 PENDING | 本地已登记，未发送或已确认对方未记账 | 登记成功；核对时接收端明确无此记录 |
| 待核 UNCERTAIN | 已发出但回执缺失/对方不可达，送达与否未知 | 发送前落库；超时、连接失败、对方 5xx |
| 已确认 ACKED | 收到接收端内容一致的回执 | 发送响应 2xx 且内容哈希一致；核对命中 |
| 冲突 CONFLICT | 同环境同发布号在接收端已存在不同内容 | 发送/核对时发现内容不一致（需人工处理） |

关键约束：**只有接收端回执能把记录变为「已确认」**。发送动作会先把「待核」落库再发 HTTP，
即使进程在发送途中崩溃，重启后也知道必须核对，而不是误报成功或误当未发。

## 演示场景

1. **正常送达**：网页填写 `prod / R2026-001 / 成功` → 提交 → 状态「已确认」，接收端台账出现一条。
2. **重复提交**：再次提交相同内容 → 发送端幂等去重、接收端仍只有一条。
3. **内容冲突**：同环境同发布号改选「失败」→ 提交即报 409 冲突，原记录不被篡改。
4. **入库后丢回执**：勾选「模拟接收端故障」再提交 → 状态变「待核」（接收端其实已入账）；
   点「重试」→ 接收端幂等去重，凭回执转「已确认」，仍只有一条。
5. **接收端不可用**：`docker compose stop receiver` → 发送/核对均保持「待核」（不虚报成功）；
   `docker compose start receiver` 后点「核对」→ 若对方无记录则回退「待发送」，可安全重发。
6. **发送端重启**：`docker compose restart sender` → 状态持久不丢；启动后自动核对所有待核记录，
   凭接收端回执结案（接收端不可达时保持待核）。
7. **热库归档**：接收端网页选择 UTC 截止时间 → 「归档截止时间前的回执」→ 热库计数下降、
   归档库计数上升，台账中旧回执标注「归档库」且仍可按环境/发布号查询；勾选故障注入可
   演示失败后在任务表点「重试/续做」收敛。归档后发送端对旧通知点「核对」仍得到原回执。

## API 摘要

发送端（:8000）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/notifications` | 登记（同键同内容幂等，同键不同内容 409） |
| GET | `/api/notifications` | 列表（含 `receipt_location`：hot/archive） |
| POST | `/api/notifications/{id}/send[?simulate=lose_response\|slow]` | 发送/重发 |
| POST | `/api/notifications/{id}/reconcile` | 凭接收端回执核对（回执在归档库也能定案） |
| POST | `/api/reconcile-all` | 核对全部待核/待发送 |

接收端（:8001）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/receipts` | 幂等入账；body 可带 `"simulate":"lose_response"` / `"slow"`；命中归档库返回 `location=archive` |
| GET | `/api/receipts?env=..&release_no=..` | 查单条：热库优先，未命中自动查归档库（回执带 `location`） |
| GET | `/api/receipts` | 台账列表：热库 + 归档库去重合并 |
| POST | `/api/archive/jobs` | 按 UTC `cutoff` 发起归档（`wait=true` 同步 / `false` 后台；可带故障注入） |
| GET | `/api/archive/jobs[/{id}]` | 归档任务列表/详情（含行级 `rows` 进度） |
| POST | `/api/archive/jobs/{id}/retry` | 续做/重试失败或中断的任务（已完成幂等） |
| GET | `/api/archive/stats` | 热库/归档库计数与归档库路径 |

## 热库归档设计

### 数据布局（同一数据卷）

```
receiver_data/（命名卷）
├── receiver.db        # 热库：receipts + archive_jobs + archive_pending
└── archive.db         # 归档库：receipts（额外 archived_at 列，UNIQUE(env, release_no)）
```

### 为什么不丢、不重、不误报

- 每条回执的搬迁严格分两步：**先在独立事务写归档库并提交，再从热库删除**；
  删除前还会回查归档库确认副本存在（双保险），因此任何时刻按幂等键查询，
  热库或归档库中至少、至多有一份可查。
- 热库内 `archive_pending` 暂存表记录每行 `PENDING → COPIED → DONE` 进度。
  归档与接收交错、写入失败或进程被杀死后，重试/重启都从暂存状态续做：
  已复制的不重复插入（归档库 `INSERT OR IGNORE` + `UNIQUE` 键），未删除的继续删除，
  最终收敛到单份可查记录。
- 只有任务下所有暂存行都达到 `DONE`，任务才置为 **DONE**；中途任何失败置
  **FAILED** 并写明错误，接口返回 5xx，**绝不误报归档完成**。
- 同一时刻只允许一个归档任务（库内 RUNNING 检查 + 进程内锁）；第二个请求得到 409。
- 接收端启动时自动把残留的 RUNNING 任务续做完（守护线程，不阻塞服务）。

### 归档后的语义保持

- **同键同内容重传**：先查归档库，命中则返回原回执（200，`deduplicated=true`，
  `location=archive`），不会重新写回热库；
- **同键不同内容**：归档库命中且哈希不一致 → 409 冲突，原回执不被篡改；
- **搬迁进行中的同键 POST**：若该键在暂存表中，返回 503 `retryable`，调用方稍后重试，
  绝不与归档 DELETE 争抢 UNIQUE 键而产生第二条；
- **全新键**：热库/归档库都没有 → 正常 201 入热库；
- **发送端核对旧通知**：`GET /api/receipts` 热库未命中即查归档库，内容一致即
  「已确认」，并把 `receipt_location=archive` 落回发送端记录；新通知继续入热库。

### 归档故障注入（演示/自测）

`POST /api/archive/jobs` 的 body 带 `"simulate":"fail_after_copy"`：在首批行已复制进
归档库、热库删除前令任务 FAILED，用于演示「回执不丢、状态可查、重试收敛」。

## 幂等与并发设计

- 两端均以 `UNIQUE(env, release_no)` 为幂等键，内容身份为 `{env, release_no, result}` 的 SHA-256；
- 写入路径 `BEGIN IMMEDIATE` 事务内「先查后插」，配合 WAL 与 busy_timeout：
  并发下同键只落一行，后到者走幂等返回或 409；
- 重发安全：接收端对同内容重试返回既有回执（200, `deduplicated`），不新增记录；
- 任一端写入失败都不会留下错误的成功状态：接收端写失败→5xx→发送端保持待核；
  发送端本地写失败→500 且无记录；拿到回执但本地更新失败→仍为待核，可靠核对补回。

## 自测

```bash
# Docker 模式（先 docker compose up --build）
python3 tests/selftest.py --lifecycle        # 含停接收端/重启发送端场景（钩子默认 docker compose）

# 无 Docker 本地模式
tests/run_local.sh test                      # 送达/幂等/重启全套（含生命周期钩子）
python3 tests/archive_selftest.py            # 归档专项（需先 start-all）
python3 tests/stress_archive.py              # 归档高压并发补充测试
```

送达链路覆盖：正常送达、同内容重试去重、同键冲突、丢回执后重发/核对、慢回执超时、
12 线程并发同内容、10 线程并发不同内容、接收端停机不虚报、发送端重启后待核持久与自动补核。

归档覆盖（`archive_selftest.py`，51 项）：UTC 截止时间归档与边界、热库收缩与合并列表
去重、归档后同键重传命中归档原回执/异内容仍 409/新键入热库、发送端核对归档旧通知与
新通知入账、4 线程 × 20 次 POST 与归档交错、故障注入失败后回执不丢且重试收敛 DONE、
进程重启后续做 RUNNING 任务、同卷布局与计数自洽。`stress_archive.py` 再以五线程混合
（归档/同键争抢/冲突请求/新写/读列表）验证无重复、无丢失、无非法响应。
