# PostgreSQL 行锁演练台

值班演练页：当一个 `UPDATE` 被 PostgreSQL 行锁挂住时，从网页上找出**真实**的持锁者，
并只解除本演练造成的这一次阻塞。

此外包含「**发布复核**」演练：在主库登记制品版本并取得带服务名与修订号的复核链接，
链接通过一个真实的 PostgreSQL **逻辑订阅库**核对登记记录，并可在网页上暂停/恢复该订阅。

## 发布复核演练

### 数据链路

```
            登记（写）                        逻辑复制（publication → subscription）
浏览器 ───────────────▶ 主库 db (PostgreSQL) ───────────────────────────────▶ 订阅库 db-replica
            ▲                    release_revision / release_service            release_revision（只读副本）
            │ 复核链接读取：先限时查订阅库，缺行/修订落后/读失败再限时查主库 ◀──┘
```

- 主库上 `release_revision(service, revision, artifact_version, created_at)` 保存每次登记，
  `release_service(service, next_revision)` 在行锁内分配修订号；**同一服务的修订号按主库
  提交顺序严格递增，回发旧制品也产生新修订**。
- 主库创建逻辑发布 `release_review_pub`，订阅库创建逻辑订阅 `release_review_sub`
  （含复制槽），应用启动时幂等创建，重启后复用、不重复建。
- 复核链接形如 `/review/{服务名}/{修订号}`，全部状态在 PostgreSQL 中，**应用重启后链接仍可用**。

### 复核读取规则（限时）

打开链接时：

1. 先以 `statement_timeout`（默认 1500ms）+ 连接超时读**逻辑订阅库**；
2. 订阅库缺行、最高修订落后于请求修订、或读取失败 → 以同样限时改查**主库**；
3. 页面显示达标记录的**制品版本、修订号、实际读取的库**，并列出每一端的最高修订与跳过原因；
4. 两端都无法读到达标记录时显示**无法确认**。

### 暂停/恢复真实订阅

- 复核页的「暂停订阅 / 恢复订阅」对订阅库执行真实的
  `ALTER SUBSCRIPTION ... DISABLE|ENABLE`。
- 暂停期间登记一版：同一链接先因订阅库落后而读到**主库**结果；恢复订阅并追平后，
  再打开链接即读到**逻辑订阅库**结果（页面在主库结果下会自动刷新等待切换）。

### 页面与接口

- 演练台首页（`/`）有「进入发布复核页」入口；`GET /review` 为登记/订阅管理页。
- 表单：`POST /review/register`（主库提交成功后 303 跳转到带服务名与修订号的链接）、
  `GET /review/{service}/{revision}`、`POST /review/subscription/{pause|resume}`。
- JSON API（自动化校验用）：
  - `POST /api/review/register`（表单字段 `service`、`artifact_version`）
  - `GET  /api/review/lookup/{service}/{revision}`
  - `GET  /api/review/subscription`、`POST /api/review/subscription/{pause|resume}`

## 行锁演练功能

- **真实数据库会话**：每轮演练在 PostgreSQL 中创建两个连接
  - 持锁会话：`BEGIN` 后执行 `SELECT ... FOR UPDATE` 锁住演练行（`idle in transaction`）；
  - 受阻更新：另一连接执行 `UPDATE lock_drill_row …`，被行锁挂住。
- **页面展示**：两端 PID、`backend_start`（会话启动时间）、事务开始时间、当前状态、
  `wait_event_type/wait_event`、`pg_blocking_pids()` 给出的**实际阻塞者**、
  双方持有的锁及阻塞关系明细表。
- **安全解除**
  - 只在「当前演练状态为 blocked，且数据库刚确认 `holder_pid ∈ pg_blocking_pids(waiter_pid)`」
    时开放按钮；
  - 提交时用**单条 SQL**（`_TERMINATE_IF_STILL_BLOCKING_SQL`）原子地复核
    「原会话仍存活（pid + backend_start + 记录中的 application_name）且仍阻塞原更新」，
    复核通过才 `pg_terminate_backend`，因此 pid 复用、会话改名、阻塞关系变化都不会误杀。
  - 页面令牌与轮次一一对应：旧页面（会话结束、服务重启、开始新一轮后）提交会被
    403/410 拒绝，绝不会关闭新会话。
- **以真实结果判定**：解锁后等待受阻连接真实执行 `COMMIT`；判定只看
  `UPDATE ... RETURNING` + `COMMIT` 的结果，并用独立连接复核行值。
  - 终止失败、命令返回成功但 `COMMIT` 未完成、或仍存在其他阻塞者时，判定为
    **未恢复**，并列出当前阻塞者。
  - 重复提交返回同一判定，不会再次终止任何会话。
- **清理**：`结束本轮演练`、开始新一轮、服务关闭都会按身份三元组终止本演练的两个会话；
  启动时清理上一进程遗留的 `lockdrill:drill:*` 会话。

## 运行（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`docker compose` 会启动三个容器：

| 服务 | 说明 |
| --- | --- |
| `db` | 主库 PostgreSQL 16，已设 `wal_level=logical`，承载行锁表与发布复核登记，并创建逻辑发布 |
| `db-replica` | 逻辑订阅库 PostgreSQL 16，由应用创建/管理逻辑订阅，可在网页暂停与恢复 |
| `web` | FastAPI 应用（固定单 worker，因为演练会话与页面令牌保存在进程内） |

应用启动时自动建表、建发布、建订阅（幂等，可重复启动）。

## 本地运行

需要两个开启了 `wal_level=logical` 的 PostgreSQL 实例：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql://drill:drill@localhost:5432/drill
export REPLICA_DATABASE_URL=postgresql://drill:drill@localhost:5433/drill
uvicorn app.main:app --port 8000
```

不配置 `REPLICA_DATABASE_URL` 时应用仍可启动，复核读取退化为只读主库，并在页面标注。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://drill:drill@localhost:5432/drill` | 主库连接串 |
| `REPLICA_DATABASE_URL` | 无 | 逻辑订阅库连接串；不配置则复核只读主库 |
| `REVIEW_PUBLICATION_NAME` | `release_review_pub` | 主库逻辑发布名 |
| `REVIEW_SUBSCRIPTION_NAME` | `release_review_sub` | 订阅库逻辑订阅名 |
| `REVIEW_STATEMENT_TIMEOUT_MS` | `1500` | 复核读取/订阅 DDL 的语句超时（毫秒），同时派生出等额秒级连接超时 |
| `REVIEW_PROVISION_ATTEMPTS` | `30` | 启动时创建订阅的重试次数（主库可能尚未可达） |
| `REVIEW_PROVISION_WAIT_SECONDS` | `1.0` | 每次创建订阅重试之间的等待秒数 |
| `PRIMARY_DB_LABEL` / `REPLICA_DB_LABEL` | `主库` / `逻辑订阅库` | 页面上实际读取库的显示名 |
| `DRILL_SESSION_PREFIX` | `lockdrill` | 会话 `application_name` 前缀 |
| `BLOCK_CONFIRM_SECONDS` | `5` | 等待数据库确认阻塞关系的时间 |
| `RELEASE_WAIT_SECONDS` | `10` | 终止后等待受阻更新真实结束的时间 |
| `REFUSED_WAIT_SECONDS` | `2` | 未终止时等待其自行结束的时间 |
| `DRILL_TTL_SECONDS` | `1800` | 超时自动结束的演练时长 |
| `DB_CONNECT_TIMEOUT` | `5` | 常规连接超时（秒） |

> 注意：生产库上使用 `pg_terminate_backend` 需要相应权限；演练请使用专用数据库/角色。
> 逻辑订阅要求两个实例均开启 `wal_level=logical`，且订阅库能以 `DATABASE_URL`
> 中的主机/端口回连主库（compose 网络内为主机名 `db`）。

## 测试

行锁测试需要一个真实 PostgreSQL（会创建 `lock_drill_test` 角色与数据库，请指向一次性实例）：

```bash
TEST_DATABASE_URL=postgresql://postgres@localhost/postgres pytest tests/test_drill.py -q
```

发布复核测试需要**两个**开启逻辑复制的 PostgreSQL（测试库 `review_test` 会被复用并自动
清理订阅/槽/发布，不执行 drop database）：

```bash
TEST_PRIMARY_URL=postgresql://postgres@localhost:55432/postgres \
TEST_REPLICA_URL=postgresql://postgres@localhost:55433/postgres \
pytest tests/test_review.py -q
```

覆盖：发布/订阅的幂等创建、修订号按服务递增且回发旧制品产生新修订、表单返回带服务名与
修订号的链接、订阅库优先读取、暂停后链接读主库、恢复追平后读订阅库、订阅库故障回退主库、
两端均无记录显示无法确认、非法输入、页面渲染、应用重启后链接仍可用且不重复建订阅。

## 接口

- 行锁：`GET /`、`POST /drill/start`、`GET /drill/{id}`
- 行锁：`POST /drill/{id}/release`、`POST /drill/{id}/end`（表单，含 `token`）
- 行锁 JSON：`GET /api/state`、`GET /api/drills/{id}`、`POST /api/drills/{id}/release|end`
- 发布复核页面：`GET /review`、`POST /review/register`、`GET /review/{service}/{revision}`、
  `POST /review/subscription/{pause|resume}`
- 发布复核 JSON：`POST /api/review/register`、`GET /api/review/lookup/{service}/{revision}`、
  `GET /api/review/subscription`、`POST /api/review/subscription/{pause|resume}`
- `GET /healthz`
