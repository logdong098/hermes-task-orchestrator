# Hermes Director 分布式调度 MVP 开发文档

## 1. 目标与非目标

### 目标

本项目提供一个轻量、可部署、可验证的控制平面，让一个 Telegram Director 统一管理多台机器上的开发 Agent，并支持直接执行或先规划后执行：

1. 用户只与一个 Director Bot 交互，能够创建、查询、取消任务并查看 Worker。
2. Coordinator 可靠保存任务，基于 Worker 注册、心跳和并发容量完成调度。
3. 复杂任务由 Coordinator 在本机调用 Claude Code 或 Codex 生成并持久化执行计划；简单任务可跳过 Planner。
4. Worker 根据能力领取待执行任务，调用本机配置的 Claude Code、Codex 或兼容命令。
5. 任务不会被两个 Worker 同时领取；Worker 掉线后任务可以重试或超时终止。
6. Telegram 完全可选，核心系统在无 Token、无真实 Agent 和无外部网络时可测试。
7. 初期用 SQLite 降低部署成本，同时让 API 层不直接依赖 SQL，便于替换 PostgreSQL/Redis。

### 非目标

- 不实现完整工作流 DAG、任务依赖、定时任务或分布式事务。
- 不实现 Hermes 内部审批逻辑，也不自动批准危险操作。
- 不用 Telegram 群聊承载机器间协议，不支持多个 Bot 互相调度。
- 不提供任意 shell 执行 API；Worker 只运行配置好的参数数组模板。
- 不承诺 SQLite 版本支持多 Coordinator 实例或跨区域强一致。
- 不在 MVP 中提供 Web 管理台、OAuth、组织/租户或计费系统。

## 2. 为什么不用 Telegram Bot-to-Bot 作为总线

Telegram 平台默认不会把一个 Bot 发出的消息作为 update 投递给另一个 Bot。即使把多个 Bot 放进同一个群组，Bot 隐私模式、更新过滤和平台的 Bot-to-Bot 限制也使其无法成为可靠的机器通信机制。依赖群消息还会把幂等、确认、重试、鉴权和状态恢复混进面向人的聊天界面。

因此本架构只保留一个面向授权用户的 Director Bot。Director、Coordinator 与 Worker 之间使用明确的 HTTP API、HMAC/Bearer 鉴权、SQLite 状态和通知 outbox。Telegram 是人机入口和结果出口，不是协作总线。

## 3. 总体架构

```text
授权用户
   │ Telegram 命令/普通文本
   ▼
Director Bot ── Bearer API ──► Coordinator / FastAPI ──► SQLite
   ▲                                  │                   │
   │ Telegram 结果推送                ├── 本机 Claude/Codex Planner
   │                                  │ HMAC API          │ 原子状态更新
   │                                  ▼                   │
   └── notification outbox ◄──── Worker A / B / C ◄──────┘
                                      │
                                      └── 默认或指定 Agent <execution_prompt>
```

Coordinator 是唯一控制平面。Worker 主动向 Coordinator 发起出站连接，因此通常无需向每台机器开放入站端口。Telegram Bot 同样主动长轮询 Telegram Bot API。

## 4. 组件职责

### Coordinator

- 暴露 FastAPI HTTP API 和 OpenAPI 文档。
- 校验 Director Bearer Token 与 Worker HMAC 签名。
- 保存 Worker 注册、最后心跳和能力元数据。
- 创建、查询、取消任务；实施任务状态机。
- 在 Worker 可领取前调用本机 Planner，保存 `plan` 和 `execution_prompt`。
- 用 SQLite `BEGIN IMMEDIATE` 串行化领取事务，防止重复领取。
- 维护 lease、执行超时、重试次数和失效任务。
- 在任务进入终态时写入 Telegram 通知 outbox。

### Worker

- `hermes-worker` 是唯一推荐的运行进程，内部按 Agent 选择执行适配器：`hermes` 走 Gateway，`codex` 与 `claude-code` 走本机 CLI。
- 使用稳定 `worker_id` 注册并周期心跳。
- 注册 `agent:<name>` 能力，并根据本地并发空位主动领取兼容任务。
- 将任务标记为 `running` 后，按 `resolved_execution_agent` 选择 Gateway 或固定命令模板。
- 只把 `{prompt}` 作为单独参数替换，不经过 shell 解释。
- 验证工作目录必须位于允许根目录内。
- 本地 CLI 实施超时并在取消时终止/杀死子进程；Gateway 执行保留远端停止确认和对账。
- 捕获 stdout/stderr，限制单路最大 2 MB，并回传终态。
- 启动本地 Agent 子进程前移除 `HERMES_*` 控制面密钥，避免 Worker/Gateway/Director 凭据泄露。

### Hermes Gateway Adapter（M1）

Gateway 是 Unified Worker 内部的执行适配器，不是第二个必需进程；`hermes-gateway-worker` 仅作为旧部署的兼容入口：

- 启动时从 Hermes API Server（默认 `http://127.0.0.1:8642`）发现或读取 Profile 列表，并为每个 Profile 注册独立 Coordinator route；共享 `/p/<profile>/...` 监听要求启用 `gateway.multiplex_profiles`，且每个命名 Profile 必须在自己的 `.env` 配置独立 `API_SERVER_KEY`。
- 每次执行都使用 Coordinator 返回的 `resolved_gateway_id/resolved_profile`，不会按当前 Desktop 前台连接或 profile-only 名称猜测路由。
- 每个 Profile 可使用独立 API key（`HERMES_GATEWAY_PROFILE_KEYS_JSON`）；凭据只在 Worker 主机注入。
- 通过 Hermes Runs API 创建、轮询、停止和重启后的 reconciliation；Coordinator 的 lease/attempt 仍是唯一重试依据。

Codex/Claude Code 本地进程属于非持久执行：Worker 崩溃后不尝试用 PID 恢复原进程，而是等待 Coordinator 租约过期后按普通重试处理。Hermes 已创建的远端 run 则使用持久化的 `remote_run_id` 对账，不创建重复 run。

M0/M1 不包含 Desktop Fleet Plugin、Desktop Proxy Executor 或跨 Gateway delegation；这些属于后续 M2/M4。Desktop 可关闭，生产任务仍应由后台 Worker 完成。

### Telegram Director

- 只处理 allowlist 中的 Telegram 数字用户 ID。
- 把命令转换为 Coordinator API 调用。
- 轮询通知 outbox，发送结果后 ACK。
- 没有 Token 时不启动；不影响其他组件。

### SQLite Repository

- 封装全部 SQL 和状态转换，API 路由不直接操作数据库。
- 每个操作使用短连接；启用 WAL、外键和 busy timeout。
- 未来可实现同方法签名的 PostgreSQL Repository，并通过应用工厂注入。

## 5. 部署拓扑

### 单机开发

Coordinator、mock Worker 和可选 Telegram 进程运行在同一台机器，SQLite 保存于本地 `data/hermes.db`。适合开发和功能验证。

### 多机推荐拓扑

```text
公网 Telegram API
       ▲
       │ HTTPS 出站
Director + Coordinator（中心节点）
       ▲
       │ 私网 HTTPS / VPN
       ├──────── Worker: Mac A
       ├──────── Worker: Linux B
       └──────── Worker: Server C
```

中心节点可通过 Tailscale、WireGuard 或内网地址提供 Coordinator。所有 Worker 只需访问中心节点。若必须经过公网，应在 Coordinator 前部署 TLS 反向代理，并限制来源网络；不要直接暴露明文 Uvicorn。

### 容器拓扑

Compose 默认只启动 Coordinator。`worker` 和 `telegram` 通过 profile 选择启用。真实 Worker 常在宿主机运行，以访问本机 Hermes 与工程目录；容器 Worker 示例默认运行 mock。

M1 的 Unified Worker 通过 `host.docker.internal:8642` 访问宿主机 Hermes API Server，仅适合开发或受控 Docker 网络。生产部署优先在 Gateway 宿主机直接运行，以保留 OS keychain、SSH agent 和本地凭据边界；Desktop 不在 readiness chain 中。

## 6. 任务生命周期

1. 用户向 Telegram 发送 `/new` 或普通文本；也可直接调用 API。
2. Director 根据 `planning_mode` 创建 `planning_pending` 或 `pending` 任务，原始 `prompt` 后续保持不变。
3. `plan` 模式下 Coordinator 领取本地规划租约，调用所选 Claude/Codex Planner。
4. 规划成功后持久化 `plan` 和组合后的 `execution_prompt`，任务转为 `pending`；`direct` 模式跳过这两步。
5. 有空闲容量且兼容 `execution_agent` 的 Worker 发起 claim；若该字段为空则 Worker 使用自身默认 Agent。
6. Worker 回报 `running`，由 `resolved_execution_agent` 对应的适配器启动本机 Agent 子进程或 Hermes Gateway run。
7. 心跳持续续租；Coordinator 同时返回待取消任务 ID。
8. Worker 回传终态，Coordinator 持久化结果并写入可选通知 outbox。

## 7. Agent 注册与心跳

Worker 注册字段：

| 字段 | 含义 |
| --- | --- |
| `worker_id` | 稳定且唯一的机器/实例标识 |
| `name` | 人类可读名称 |
| `max_concurrency` | Coordinator 允许该 Worker 同时持有的任务数 |
| `capabilities` | `agent:<name>` 列表，用于显式执行 Agent 的兼容领取；Unified Worker 可同时声明本地 Agent 与 `hermes` |
| `metadata` | 平台、Python 版本和 `default_agent` 等非敏感信息 |

重复注册采用 upsert，可用于进程重启。`registered_at` 保留首次时间，`last_heartbeat_at` 更新。

心跳携带 `running_task_ids`。Coordinator：

- 更新 Worker 最后心跳；
- 为该 Worker 实际拥有且处于 `claimed/running` 的任务续租；
- 返回处于 `cancel_requested` 的任务 ID；
- 不信任 Worker 对其他 Worker 任务的声明。

`last_heartbeat_at` 超过 `worker_stale_seconds` 后，查询结果显示 Worker 为 `offline`。在线状态是派生值，不单独持久化。

Unified Worker 的 Gateway route 只执行对应 route 支持的 `hermes`；Codex/Claude Code 则走同一 Worker 内的本地命令适配器。旧 `gateway-worker` 入口仍可用于迁移，但不再代表一套独立的控制环。

## 8. 任务协议

核心任务字段：

| 字段 | 说明 |
| --- | --- |
| `id` | UUID |
| `prompt` | 用户原始任务文本，创建后不修改 |
| `planner_agent` | Coordinator 本地用于生成计划的 Claude 或 Codex |
| `execution_agent` | 可选；为空时由 Worker 使用默认 Agent |
| `plan` | Planner 输出的执行计划 |
| `execution_prompt` | 原始任务与计划组合后的 Worker 输入 |
| `target_worker_id` | 可选；为空表示任意 Worker |
| `workdir` | 可选；相对 Worker 允许根目录的路径 |
| `timeout_seconds` | 单次任务执行上限 |
| `max_attempts` | 最大领取/执行次数 |
| `priority` | 越大越先领取；同优先级按创建时间 |
| `claimed_by` | 当前持有任务的 Worker |
| `attempt_count` | 每次成功 claim 加一 |
| `lease_expires_at` | 当前租约截止 Unix 秒数 |
| `result/error` | 成功输出或错误摘要 |
| `creator_user_id` | 来源用户，可空 |
| `telegram_chat_id` | 结果推送目标，可空 |
| `idempotency_key` | 可选创建幂等键；Telegram 使用 update ID |

Worker 请求 HMAC 签名消息为：

```text
<unix_timestamp>\n<random_nonce>\n<METHOD>\n<PATH>\n<SHA256(raw_body)>
```

签名使用 `HMAC-SHA256(shared_secret, message)`，通过以下 Header 发送：

- `X-Hermes-Worker-ID`
- `X-Hermes-Timestamp`
- `X-Hermes-Nonce`
- `X-Hermes-Signature`

Coordinator 校验签名和最大时钟偏差，并把 `(worker_id, nonce)` 原子写入 SQLite；时间窗口内的重复 nonce 返回冲突。路径中的 Worker ID 必须与 Header 一致，已领取任务也只能由其 owner 更新。

## 9. 状态机

```text
planning_pending ──local claim──► planning ──plan ready──► pending
       │                              │
       └──cancel──► cancelled         ├──failure──► failed
                                      └──timeout──► timed_out

pending ──claim──► claimed ──start──► running ──success──► succeeded
   │                 │                  │  └──error──────► failed
   │                 │                  │  └──timeout────► timed_out
   │                 │                  │
   └──cancel────────► cancelled         └──cancel request──► cancel_requested
                                                               │
                                                               └──worker ack/lease──► cancelled

claimed/running ──lease expiry + attempts remain──► pending
claimed/running ──lease expiry + attempts exhausted──► timed_out
failed(retryable) ──attempts remain──► pending
planning ──lease expiry + attempts remain──► planning_pending
```

终态为 `succeeded`、`failed`、`cancelled`、`timed_out`。终态结果回传是幂等的：同一 owner 重复上报只返回已有记录，不再次产生 outbox。

## 10. 重试、超时与取消

### 重试

- Planner 领取时独立增加 `planner_attempt_count`；普通非零退出、空计划和超时在规划预算未耗尽时回到 `planning_pending`。
- Planner 配置错误（未知 Agent、缺少命令、命令不存在或无权限）直接失败，避免无意义重试。
- claim 成功时 `attempt_count + 1`。
- Worker 报告 `failed + retryable=true` 且次数未耗尽时，任务回到 `pending`。
- lease 过期且次数未耗尽时也回到 `pending`。
- `max_attempts` 默认 2，最大 10。

### 超时

- Worker 使用任务超时与本地上限的较小值包裹子进程。
- Coordinator 根据 `started_at + timeout_seconds` 和 lease 做服务端兜底。
- 本地超时先 terminate，5 秒未退出则 kill，并回报 `timed_out`。
- API 会把任务超时截断到 Coordinator 配置的最大值。

### 取消

- `planning_pending`、`planning` 或 `pending` 任务立即变为 `cancelled`；活动 Planner 子进程会被终止。
- `claimed/running` 任务变为 `cancel_requested`。
- Worker 在下次心跳收到任务 ID，终止子进程并回报 `cancelled`。
- Worker 丢失时，`cancel_requested` 在 lease 过期后由维护逻辑收敛到 `cancelled`。

## 11. 结果回传与通知

Worker 回传终态、stdout、stderr 摘要和 `retryable`。Coordinator 先提交任务终态，再在同一 SQLite 事务写入唯一 `(task_id, channel)` outbox 记录，避免任务完成但通知事件丢失。

Telegram 进程轮询未 ACK 通知，发送成功后再 ACK。因此交付语义是“至少一次”：进程可能在 Telegram 已接收但 ACK 前崩溃，重启后会重复推送；不会因先 ACK 后发送而静默丢失。

## 12. Telegram 交互设计

| 输入 | 行为 |
| --- | --- |
| `/agents` | 列出 Worker ID、在线状态和并发 |
| `/tasks` | 列出最近任务、状态、当前阶段和 Worker |
| `/new [--planner claude|codex] [--agent <agent>] <任务>` | 创建任务并可指定两阶段 Agent |
| `@worker-a [-codex\|-cc\|--executor/--agent <agent>] <任务>` | 跳过 Planner，按默认或显式 Agent 定向执行 |
| `@Coordinator [@worker-a] [-codex\|-cc\|--executor/--agent <agent>] <任务>` | 先规划，再自动或定向执行 |
| `/status <UUID>` | 查询模式、阶段、attempt、事件和结果/错误 |
| `/cancel <UUID>` | 请求取消 |
| `/help` | 显示命令帮助 |
| 普通文本 | 等价于 `/new <文本>` |

只有 `HERMES_TELEGRAM_ALLOWED_USER_IDS` 中的用户可调用。命令支持群聊中的 `/command@botname` 形式。输出按 4000 字符分片，低于 Telegram 单消息上限。

`@worker <任务>` 让 Coordinator 选择在线 Worker（单 Worker 部署时就是唯一的 Unified Worker）；写成 `@worker-a` 则定向到具体 Worker ID。未指定 Agent 时使用 Worker 的 `default_agent`；`-codex` 和 `-cc` 只在 Worker mention 后的选项区生效，分别映射为 `codex` 和 `claude-code`。旧的 `--executor`/`--agent` 仍兼容；不支持的 Agent 不会静默 fallback。

## 13. 权限与安全

### 已实现边界

- Telegram 数字用户 ID allowlist，默认空列表时 Telegram 进程拒绝启动。
- Director API 使用独立 Bearer Token；Worker API 使用共享密钥 HMAC。
- 密钥为空时受保护 API 返回 `503`，不会降级为无鉴权。
- HMAC 覆盖方法、路径、原始 body 哈希和时间戳，并限制时钟偏差。
- HMAC 请求包含随机 nonce；Coordinator 持久化已用 nonce 并拒绝窗口内重放。
- 可用 `HERMES_WORKER_SECRETS_JSON` 为每个 Worker 绑定独立密钥；未配置时兼容共享密钥模式。
- Worker ID 绑定签名 Header、URL 和任务 owner。
- 命令采用参数数组，不使用 shell，prompt 不会被解释为 shell 语法。
- 配置中出现 `dangerously-bypass` 的参数会被拒绝。
- 工作目录必须解析到允许根目录内部，拒绝 `..` 逃逸和不存在目录。
- Worker 有本地并发与超时，Coordinator 有服务端并发、lease 与最大超时。
- 日志不记录 Token、Secret、HMAC Header 或完整 prompt；内部异常对 API/结果返回摘要。
- `.env`、数据库和缓存被 `.gitignore` 排除。

### 部署要求

- 为 Director 与 Worker 使用不同的高熵随机密钥，并定期轮换。
- Coordinator 以最小权限账户运行；每台 Worker 使用独立低权限系统账户。
- 允许目录应精确到需要操作的工程根，禁止设置为 `/` 或用户主目录。
- 生产网络使用 TLS/VPN、防火墙和访问日志；SQLite 文件需要受控权限和备份。
- Hermes 自身审批保持开启。此项目不添加任何绕过审批参数。

### 日志脱敏

`config.redacted_environment()` 可安全展示 `HERMES_*` 配置，名称含 `TOKEN/SECRET/KEY/PASSWORD` 的值替换为 `***`。当前运行日志只输出 Worker ID、任务 ID、受限工作目录和错误类别，不输出认证值。

## 14. 网络方案

优先级从高到低：

1. **Tailscale/WireGuard 私网**：中心节点只监听 VPN 地址，Worker 主动连接。
2. **同一内网/VPC**：安全组只允许 Worker 网段访问 Coordinator。
3. **公网 TLS 反向代理**：Caddy/Nginx 终止 TLS，限制来源 IP，并考虑双向 TLS。

Telegram 进程只需要出站访问 `api.telegram.org`。Worker 只需出站访问 Coordinator 以及 Hermes 自身所需服务。SQLite 不开放网络端口。

## 15. 数据模型

### `workers`

主键 `worker_id`；保存名称、最大并发、JSON 能力、JSON 元数据、首次注册和最后心跳时间。

### `tasks`

主键 UUID；保存任务输入、目标、状态、owner、执行限制、attempt、claim token、优先级、来源、幂等键、结果、时间戳与 lease。每次普通领取和对账领取都会轮换 claim token；Worker 的状态、进度、远端绑定、对账和结果写入必须携带当前 token，从而隔离复用同一 `worker_id` 的旧运行实例。领取索引覆盖 `status/target/priority/created_at`，Worker 活跃任务索引覆盖 `claimed_by/status`，非空幂等键具有唯一索引。

### `task_events`

追加写入任务创建、规划、领取、Worker 进度、远端 run、对账与终态事件；任务详情返回最近事件，完整历史通过独立 API 查询。

### `notifications`

自增主键；保存任务、channel、destination、JSON payload、创建与 ACK 时间。`UNIQUE(task_id, channel)` 防止同一终态重复入队。

### `worker_nonces`

以 `(worker_id, nonce)` 为主键，保存 nonce 使用时间。Worker 鉴权在执行业务路由前原子消费 nonce，并清理超过保留窗口的记录。

所有时间为 UTC 语义的 Unix 秒数，避免跨机器时区歧义。

## 16. API 设计

### 公共 API

| 方法 | 路径 | 鉴权 | 说明 |
| --- | --- | --- | --- |
| `GET` | `/healthz` | 无 | 存活检查 |

### Director API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/v1/workers` | Worker 列表与在线状态 |
| `POST` | `/api/v1/tasks` | 创建任务 |
| `GET` | `/api/v1/tasks` | 最近任务列表 |
| `GET` | `/api/v1/tasks/{id}` | 任务详情 |
| `GET` | `/api/v1/tasks/{id}/events` | 任务进度与状态事件 |
| `POST` | `/api/v1/tasks/{id}/cancel` | 取消任务 |
| `GET` | `/api/v1/notifications` | 未 ACK 通知 |
| `POST` | `/api/v1/notifications/{id}/ack` | ACK 通知 |

Header：`Authorization: Bearer <HERMES_DIRECTOR_API_KEY>`。

### Worker API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/v1/workers/register` | 注册/upsert |
| `POST` | `/api/v1/workers/{id}/heartbeat` | 心跳、续租、获取取消列表 |
| `POST` | `/api/v1/workers/{id}/tasks/claim` | 原子领取一个任务，无任务返回 `task: null` |
| `POST` | `/api/v1/tasks/{id}/status` | 当前只允许 `claimed -> running` |
| `POST` | `/api/v1/tasks/{id}/progress` | 回报当前阶段与受限详情 |
| `POST` | `/api/v1/tasks/{id}/remote-run` | 绑定 Gateway run/session 审计标识 |
| `POST` | `/api/v1/tasks/{id}/reconcile` | 进入远端结果对账状态 |
| `POST` | `/api/v1/tasks/{id}/result` | 回传终态和结果 |

Worker API 使用前述 HMAC Header。任务领取响应包含当前 claim token；后续任务写操作必须同时携带该 token，旧 claim 返回 `409`。请求/响应模型和约束可在 `/docs` 查看。

## 17. 配置说明

运行时配置全部来自 `HERMES_*` 环境变量；若当前目录存在 `.env`，代码会读取未在进程环境中设置的值。进程环境优先于 `.env`。

### Coordinator 关键配置

- `HERMES_DATABASE_PATH`：SQLite 路径。
- `HERMES_DIRECTOR_API_KEY`：Director Bearer Token，必需。
- `HERMES_WORKER_SHARED_SECRET`：Worker HMAC 共享密钥，必需。
- `HERMES_WORKER_SECRETS_JSON`：可选的 Worker ID 到独立密钥 JSON 映射；设置后禁用共享回退。
- `HERMES_WORKER_STALE_SECONDS`：Worker 离线判定阈值。
- `HERMES_TASK_LEASE_SECONDS`：任务租约。
- `HERMES_HMAC_MAX_CLOCK_SKEW_SECONDS`：签名时间窗口。
- `HERMES_DEFAULT/MAX_TASK_TIMEOUT_SECONDS`：任务默认/最大超时。
- `HERMES_DEFAULT_MAX_ATTEMPTS`：默认尝试次数。
- `HERMES_MAINTENANCE_INTERVAL_SECONDS`：后台 lease/超时扫描间隔。

### Worker 关键配置

- `HERMES_COORDINATOR_URL`、`HERMES_WORKER_ID/NAME`。
- `HERMES_WORKER_SHARED_SECRET`：必须与 Coordinator 一致。
- `HERMES_WORKER_DEFAULT_AGENT`：未指定任务 Agent 时使用的默认值；新部署建议为 `hermes`。
- `HERMES_WORKER_AGENTS_JSON`：Codex/Claude Code 等本地 Agent 的 argv 模板；必须包含独立 `{prompt}` 参数。`HERMES_AGENT_COMMANDS` 是兼容别名。
- `HERMES_WORKER_COMMAND`：默认 `hermes chat -q {prompt}` 的兼容配置；Unified Worker 的 Hermes 任务实际使用 Gateway。
- `HERMES_WORKER_ALLOWED_WORKDIR`：允许根目录。
- `HERMES_WORKER_CONCURRENCY`、本地超时、心跳和轮询间隔。
- `HERMES_GATEWAY_URL/ID/PROFILES`：Unified Worker 的 Hermes Gateway 路由配置；每个 Profile 的 key 只保存在 Worker 主机。

### Telegram 关键配置

- `HERMES_TELEGRAM_BOT_TOKEN`：BotFather Token。
- `HERMES_TELEGRAM_ALLOWED_USER_IDS`：逗号分隔的数字 ID。
- `HERMES_COORDINATOR_URL` 和 `HERMES_DIRECTOR_API_KEY`。
- `HERMES_TELEGRAM_OFFSET_PATH`：持久化 long polling offset 的文件。

`.env.example` 是可复制模板。`config.example.yaml` 仅展示结构，MVP 不加载 YAML，以避免额外依赖和双配置源。

## 18. 部署步骤

### Python 方式

1. 创建 Python 3.9+ 虚拟环境并安装 `.[dev]`。
2. 复制 `.env.example` 为 `.env`，生成并填写随机密钥。
3. Coordinator 节点运行 `make coordinator`。
4. 各 Worker 机器安装同一项目，配置唯一 ID、中心 URL、共享密钥、允许目录与命令，运行 `make worker`。
5. 如需 Telegram，配置 Token 和 allowlist 后运行 `make telegram`。
6. 使用 `/healthz`、`/agents` 和一个小任务做上线验证。

### Docker 方式

1. 在 shell 设置两个密钥，运行 `docker compose up --build coordinator`。
2. 测试可启用 `--profile worker`；真实 Worker 建议在目标宿主机运行。
3. 设置 Telegram Token/allowlist 后启用 `--profile telegram`。
4. 将 Coordinator 放到 TLS/VPN 后，并持久化 `hermes-data` volume。

### Unified Worker 安装与远程安全

Worker 所在机器安装项目和依赖后，配置 `HERMES_COORDINATOR_URL`、唯一的 `HERMES_WORKER_ID`、`HERMES_GATEWAY_ID`、`HERMES_GATEWAY_URL`、`HERMES_GATEWAY_PROFILES` 与 profile keys，再运行 `python -m hermes.worker`。Hermes API Server 默认端口为 `8642`；跨主机时使用 Tailscale/WireGuard 私网地址或 HTTPS/SSH 转发，禁止将 8642 直接暴露到公网。Coordinator 只接收 Worker HMAC 和 route 元数据，不接触 Gateway key。

`python -m hermes.gateway_worker` 仍作为旧配置的兼容入口，内部转交 Unified Worker；新部署不要同时启动两个入口使用同一个 Worker ID。

建议分别用 systemd（Linux）或 launchd（macOS）托管 Coordinator、Unified Worker 和 Telegram。服务应使用独立低权限用户、独立工作目录和受限环境文件；key 不应写入 unit/plist、镜像或日志。

SQLite 升级只执行 additive migration。升级前停止 Coordinator 或使用 `sqlite3 <db> ".backup '<copy>'"` 在线备份；恢复时先停止服务，不能只复制正在使用的主 `.db` 而忽略 WAL 文件。Compose 的 `hermes-data` volume 也应在升级前导出并定期验证恢复。

## 19. 测试方案

测试使用标准库 `unittest`、FastAPI ASGI transport 和 mock 子进程，不依赖外网：

- 状态机：正常成功、非法转换、取消收敛、终态通知。
- 原子领取：两个线程并发 claim，同一任务只有一个结果。
- 心跳：在线/离线阈值、续租与取消 ID。
- 重试/超时：首次 lease 失效回队列，次数耗尽进入 `timed_out`。
- 鉴权：缺失/错误 Director Token、错误和正确 HMAC。
- Worker：工作目录逃逸、危险参数拒绝、mock 命令执行和结果回传。
- Telegram：无 Token、无网络的命令与 allowlist mock。
- 端到端：随机端口启动真实 Uvicorn，HTTP 创建任务，独立 Worker 进程执行 mock，再 HTTP 校验结果。
- M0 路由：旧数据库迁移、route 注册/心跳、精确 Gateway/Profile claim、无 fallback、resolved route 审计、并发 claim。
- M1 Gateway：fake Hermes API Server 的多 Profile discover/run/stop/reconcile、独立 profile key、无 Desktop GUI 的完整闭环。

执行：

```bash
make test
make lint
make format-check
make e2e
```

## 20. 扩展路线

1. 抽象 Repository Protocol，增加 PostgreSQL 实现和数据库迁移工具。
2. 使用 Redis Streams/NATS 取代轮询，保留数据库作为事实源。
3. 密钥轮换、mTLS 和集中式 nonce/replay cache。
4. 能力标签、资源标签、亲和/反亲和和负载感知调度。
5. 完整审计日志、流式输出和增量 Telegram 消息更新。
6. 幂等创建键、Webhook Telegram 模式、通知去重标识。
7. 多 Coordinator leader election、PostgreSQL `SKIP LOCKED` 和水平扩展。
8. Web UI、RBAC、团队/租户和任务模板。
9. Worker 沙箱、容器化执行、CPU/内存/磁盘配额和制品存储。
10. Hermes 审批事件桥接，让用户在 Telegram 查看审批请求，但仍由 Hermes 安全策略裁决。

## 21. 已知限制

- SQLite 适合单 Coordinator；不支持多实例共享同一数据库文件。
- 调度采用短轮询，不是实时消息队列；空闲时会产生少量请求。
- 若使用兼容的共享 Worker 密钥模式，任一 Worker 泄露会影响整个 Worker 信任域；生产应配置每 Worker 独立密钥或 mTLS。
- Worker 进程崩溃后只能依赖 lease 恢复，正在运行的 Hermes 子进程可能成为孤儿；可进一步使用进程组/cgroup 管理。
- 取消是协作式且受心跳间隔影响，不保证毫秒级；不可中断的系统调用可能延迟退出。
- stdout/stderr 直接存入 SQLite，单路截断 2 MB；大型制品应使用对象存储。
- 通知为至少一次，极端崩溃窗口可能重复推送。
- Telegram 使用 long polling；单 Token 只能有一个活跃轮询消费者。
- Telegram allowlist 只有用户级控制，尚无命令级 RBAC 或群聊范围限制。
- `config.example.yaml` 尚非运行时配置源。
- SQLite nonce 与 Telegram offset 均由单 Coordinator/单 Bot 进程管理；水平扩展时需迁移到共享存储。
