# hermes

`hermes` 是一个可运行的分布式开发任务调度 MVP：用户通过 Telegram Director Bot 或 API 提交任务，简单任务可直接交给 Worker，复杂任务可先由 Coordinator 在本机调用 Claude Code 或 Codex 生成执行计划，再可靠地分配给 Worker。用户可以指定执行 Agent；未指定时由领取任务的 Worker 使用本机默认 Agent。

核心 Coordinator 与 Worker 不依赖 Telegram。没有 Bot Token、没有真实 Hermes CLI 时，也可以使用内置 mock 完成测试和端到端流程。

详细设计见 [docs/architecture.md](docs/architecture.md)。

## 组件

- `coordinator`：FastAPI + SQLite，负责认证 API、任务状态机，并在本机运行 Claude/Codex Planner。
- `worker`：按 Agent 能力领取任务，以无 shell的参数数组启动本机 Agent，支持并发、超时和取消。
- `telegram`：单个 Director Bot，提供 `/agents`、`/tasks`、`/new`、`/status`、`/cancel`、`/help`，并推送结果。
- `mock_hermes`：不访问外部服务的假 Hermes 命令，仅用于开发和测试。

M0/M1 还提供 `gateway-worker`：它是无 GUI 的 Hermes Gateway 适配 Worker，直接访问 Worker 所在机器上的 Hermes API Server（默认 `8642`），按 `(gateway_id, profile)` 注册和领取任务。Hermes Desktop 不是运行依赖；M2 Desktop Fleet Plugin、M3 自动路由与 `task_attempts` 历史审计、M4 Bridge/delegation 当前均未实现。

## 从零启动

要求 Python 3.9+。真实 Worker 还需要本机已安装可用的 `hermes` 命令。

```bash
cd /Users/log/Documents/hermes
python3 -m venv .venv
.venv/bin/python -m pip install --no-build-isolation -e '.[dev]'
cp .env.example .env
```

`.env.example` 是配置入口。Coordinator、普通 Command Worker 和 Gateway Worker 可以使用各自机器上的 `.env`；不要把真实 key 写入 Git 或复制到 Coordinator。

仓库同时提供 `setup.py/setup.cfg` 兼容入口；`--no-build-isolation` 会复用虚拟环境中已有的 setuptools，因此上述 editable 安装兼容 pip 21.2.4，不要求先升级 pip，也不会为了构建隔离环境重复下载 setuptools。如果受限网络只能使用环境中已有的运行时包，也可执行 `.venv/bin/python -m pip install -r requirements-dev.txt`，随后使用 Makefile（其命令会设置 `PYTHONPATH=src`）。

开发环境使用上面的 `-e` 安装。生产机器建议使用非 editable 安装，并为每个角色准备独立目录和 `.env`：

```bash
python3 -m venv /opt/hermes/.venv
/opt/hermes/.venv/bin/python -m pip install --no-build-isolation /path/to/hermes
install -m 600 /path/to/role-specific.env /opt/hermes/.env
```

安装后会提供四个命令：`hermes-coordinator`、`hermes-worker`、`hermes-gateway-worker`、`hermes-telegram`。

推荐按角色部署：

| 机器/进程 | 必需配置 | 启动命令 |
| --- | --- | --- |
| Coordinator | 数据库、Director key、Worker secret、Planner 命令 | `hermes-coordinator` |
| Command Worker | Coordinator URL、Worker ID/secret、Agent 命令和允许目录 | `hermes-worker` |
| Gateway Worker | Coordinator URL、Worker ID/secret、Gateway ID/URL、Profiles 和各 Profile key | `hermes-gateway-worker` |
| Telegram（可选） | Coordinator URL、Director key、Bot Token、用户 allowlist | `hermes-telegram` |

所有长期进程启动时都从其当前工作目录读取 `.env`。同一台机器运行多个角色时，使用不同工作目录和不同 `.env`，避免把 Gateway Profile key 放到 Coordinator 环境。

生成两个不同的随机密钥，并写入 `.env`：

```bash
.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(32))'
.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(32))'
```

至少修改这些配置：

```dotenv
HERMES_DIRECTOR_API_KEY=<第一个随机值>
HERMES_WORKER_SHARED_SECRET=<第二个随机值>
HERMES_WORKER_ALLOWED_WORKDIR=/Users/log/Documents/hermes
```

Coordinator 默认使用本机 Codex 规划；也可以将默认 Planner 改为 Claude：

```dotenv
HERMES_PLANNER_DEFAULT_AGENT=codex
HERMES_PLANNER_COMMANDS_JSON={"claude":["claude","--permission-mode","plan","-p","{prompt}"],"codex":["codex","exec","--sandbox","read-only","{prompt}"]}
```

默认模板把 Claude 固定为 `plan` 权限模式、把 Codex 固定为 `read-only` sandbox，Planner 只负责读取上下文并输出执行计划。覆盖命令时应保留等价的只读限制；真正的文件修改只在 Worker 执行阶段发生。

Worker 未收到 `execution_agent` 时使用默认命令。要允许用户显式选择 Claude/Codex，请配置能力映射：

```dotenv
HERMES_WORKER_DEFAULT_AGENT=codex
HERMES_WORKER_AGENTS_JSON={"claude":["claude","-p","{prompt}"],"codex":["codex","exec","{prompt}"]}
```

Coordinator 主机必须安装并能运行所配置的 Planner CLI；Worker 主机只需安装自己映射中的执行 Agent。`HERMES_WORKER_COMMAND` 继续作为默认 Agent 的兼容入口，无需一次性改造旧部署。

Planner 子进程不会继承名称中含 `KEY`、`SECRET`、`TOKEN` 或 `PASSWORD` 的 `HERMES_*` 变量，避免读取 Coordinator/Worker/Gateway 凭据。Planner 登录请使用 Codex/Claude 自身的登录状态或其原生 provider 环境变量，不要复用 Hermes 控制面密钥。

现有 SQLite 数据库启动时会原地增加规划字段，不会重建表，也不会把已经存在的 `pending/claimed/running` 任务重新送去规划；这些旧任务继续使用原始 `prompt` 执行。

终端 1 启动 Coordinator：

```bash
make coordinator
```

终端 2 启动真实 Worker：

```bash
make worker
```

### M1 Gateway Worker（无 Hermes Desktop）

先启用 Hermes 的多 Profile 共享监听：

```bash
hermes config set gateway.multiplex_profiles true
```

默认 Profile 在 `~/.hermes/.env` 配置 API Server；每个命名 Profile 在 `~/.hermes/profiles/<profile>/.env` 配置自己的 key。例如：

```dotenv
# ~/.hermes/.env
API_SERVER_ENABLED=true
API_SERVER_HOST=127.0.0.1
API_SERVER_PORT=8642
API_SERVER_KEY=<default-profile-key>

# ~/.hermes/profiles/architect/.env
API_SERVER_KEY=<architect-profile-key>

# ~/.hermes/profiles/coder/.env
API_SERVER_KEY=<coder-profile-key>
```

然后启动 Hermes Gateway（不同 Hermes 版本的入口可能不同，以 `hermes gateway` 为准），并确认 Runs API 可用：

```bash
hermes gateway
curl -sS -H 'Authorization: Bearer <architect-profile-key>' \
  http://127.0.0.1:8642/p/architect/v1/capabilities
```

M1 使用 `/p/<profile>/v1/runs`、`/p/<profile>/v1/runs/<run_id>` 和对应的 `/stop` 接口。Gateway Worker 只需要访问 Coordinator 和该 API Server：

```bash
HERMES_GATEWAY_WORKER_ID=mac-hermes-gateway \
HERMES_GATEWAY_ID=mac-hermes \
HERMES_GATEWAY_KIND=local \
HERMES_GATEWAY_URL=http://127.0.0.1:8642 \
HERMES_GATEWAY_PROFILES=architect,coder \
HERMES_GATEWAY_DEFAULT_PROFILE=default \
HERMES_GATEWAY_PROFILE_KEYS_JSON='{"architect":"<architect-key>","coder":"<coder-key>"}' \
hermes-gateway-worker
```

每个 Profile 使用独立 key；`target_gateway_id` 与 `target_profile` 必须同时指定。任务被 Coordinator 精确解析后，Gateway Worker 会把 resolved Profile 传给 Hermes API，不会把同 Gateway 的其他 Profile 当作 fallback。M1 Gateway Worker 只宣告并接受 `execution_agent=hermes`，`HERMES_GATEWAY_DEFAULT_AGENT` 必须保持 `hermes`；Claude/Codex 应由 Command Worker 执行。关闭 Desktop 不影响这条生产路径。

`HERMES_GATEWAY_PROFILES` 建议在生产环境显式配置。若留空并依赖 `GET /api/profiles` 自动发现，则 `HERMES_GATEWAY_TOKEN` 必须设置为 `HERMES_GATEWAY_DEFAULT_PROFILE`（默认名为 `default`）的 key；命名 Profile 的请求仍必须使用 `HERMES_GATEWAY_PROFILE_KEYS_JSON` 中各自的 key，缺失时 Gateway Worker 会拒绝执行，不会回退到默认 key。

如果 Coordinator 在另一台机器上，请把 `HERMES_COORDINATOR_URL` 设置为其 Tailscale/WireGuard 私网地址（例如 `https://100.x.y.z`），不要把 Worker API 暴露到公网。Hermes API Server 也应只绑定 localhost 或 VPN 网卡，并通过 HTTPS/SSH 隧道保护跨主机流量。

没有真实 Hermes 时启动模拟 Worker：

```bash
make mock-worker
```

## 用 curl 创建和查询任务

`.env` 不会自动导出给 shell，因此先取出 Director API Key，或直接把下方占位符替换为实际值。

```bash
export DIRECTOR_KEY='<HERMES_DIRECTOR_API_KEY 的值>'
curl -sS -X POST http://127.0.0.1:8000/api/v1/tasks \
  -H "Authorization: Bearer ${DIRECTOR_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"请总结当前目录结构","timeout_seconds":120}'
```

API 默认保持兼容，`planning_mode=auto` 会先规划。简单任务可显式跳过 Planner：

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/tasks \
  -H "Authorization: Bearer ${DIRECTOR_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"修复登录页文案","planning_mode":"direct","target_worker_id":"worker-a"}'
```

`planning_mode` 可取 `auto`、`plan`、`direct`；`direct` 不能同时指定 `planner_agent`。

指定 Planner 和执行 Agent：

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/tasks \
  -H "Authorization: Bearer ${DIRECTOR_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"实现并测试健康检查接口","planner_agent":"claude","execution_agent":"codex"}'
```

`execution_agent` 省略或为 `null` 时，任务可由任意匹配 Worker 领取，并由该 Worker 使用自己的默认 Agent。

指定 Gateway/Profile：

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/tasks \
  -H "Authorization: Bearer ${DIRECTOR_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"检查 architect 工作区","target_gateway_id":"mac-hermes","target_profile":"architect","execution_agent":"hermes"}'
```

记下响应中的任务 ID：

```bash
curl -sS http://127.0.0.1:8000/api/v1/tasks/<TASK_ID> \
  -H "Authorization: Bearer ${DIRECTOR_KEY}"
```

任务详情包含最近事件；完整进度事件可单独查询：

```bash
curl -sS http://127.0.0.1:8000/api/v1/tasks/<TASK_ID>/events \
  -H "Authorization: Bearer ${DIRECTOR_KEY}"
```

Gateway 网络或轮询短暂失败时，任务会进入 `reconciling`，保留当前远端 run 并继续对账。超过 `HERMES_RECONCILIATION_GRACE_SECONDS` 后才按最终状态收敛；重试间隔由 `HERMES_RECONCILIATION_BACKOFF_SECONDS` 控制。每次普通领取或对账领取都会生成新的 claim token，旧进程即使复用同一个 `worker_id` 也不能更新新 claim。取消已请求但远端结果始终无法确认时，最终显示为 `timed_out` 并保留“结果未知”诊断，不会虚假显示为已取消。

取消任务：

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/tasks/<TASK_ID>/cancel \
  -H "Authorization: Bearer ${DIRECTOR_KEY}"
```

查看 Worker：

```bash
curl -sS http://127.0.0.1:8000/api/v1/workers \
  -H "Authorization: Bearer ${DIRECTOR_KEY}"
```

交互式 OpenAPI 文档位于 `http://127.0.0.1:8000/docs`，健康检查为 `GET /healthz`。

## 启动 Telegram Director

在 BotFather 创建一个 Bot，把 Token 和授权用户的数字 ID 写入 `.env`：

```dotenv
HERMES_TELEGRAM_BOT_TOKEN=<BotFather Token>
HERMES_TELEGRAM_ALLOWED_USER_IDS=123456789,987654321
```

然后启动：

```bash
make telegram
```

不配置 Token 时不要启动该进程；Coordinator、Worker、测试和 mock 流程不受影响。

Telegram Bot 默认收不到其他 Bot 发送的消息。因此本项目不把多个 Bot 的群聊当作协作总线，而是只使用一个面向人的 Director Bot；机器间通信全部通过 Coordinator 的认证 API。

创建任务既支持兼容的 `/new` 参数，也支持更短的 `@` 指令：

```text
/new [--planner claude|codex] [--agent <agent>] <任务描述>
@worker-a 修复登录页文案并运行相关测试
@Coordinator 分析这个开发计划并给出执行方案
@Coordinator @worker-a 分析并由指定 Worker 实现登录模块
```

`@worker-a` 使用 `direct` 模式，跳过 Planner，但任务仍由 Coordinator 持久化、鉴权、调度和跟踪。`@Coordinator` 使用 `plan` 模式；它与 Worker 的先后顺序可以互换。当前 P0/P1 一条任务只允许一个 Worker，`->`、多 Worker DAG 和工作流编排暂未实现。

M1 Telegram 路由语法为：

```text
/new --planner codex --gateway mac-hermes --profile architect --executor hermes 检查工作区
```

`--executor` 是执行器；旧的 `--agent` 仍作为兼容别名。`--gateway` 和 `--profile` 必须成对出现。

使用 `/tasks` 查看最近任务及当前阶段，使用 `/status <任务ID>` 查看规划/执行模式、阶段标记、attempt、最近事件和最终结果。P1 采用按需查询与终态通知，不会持续编辑原 Telegram 消息。

## 测试和验证

测试不需要 Telegram Token、真实 Hermes 或外部网络：

```bash
make test
make lint
make format-check
make e2e
```

Makefile 会优先使用仓库内的 `.venv/bin/python`；也可以显式执行 `PYTHON=.venv/bin/python make test`。未创建 `.venv` 时才回退到系统 `python3`。

`make e2e` 会运行两条真实 HTTP 进程链路：Command Worker mock，以及直接调用 profile-scoped Runs API 的 Gateway Worker mock。第二条链路不会启动或依赖 Hermes Desktop GUI。

## Docker Compose

先在当前 shell 设置密钥：

```bash
export HERMES_DIRECTOR_API_KEY='<随机值>'
export HERMES_WORKER_SHARED_SECRET='<另一个随机值>'
docker compose up --build coordinator
```

同时启动内置 mock Worker：

```bash
mkdir -p workspace
docker compose --profile worker up --build
```

Compose 中的 `gateway-worker` profile 适合 Gateway 运行在 Docker 宿主机且监听 `8642` 的开发环境：

```bash
export HERMES_GATEWAY_PROFILE_KEYS_JSON='{"default":"<profile-key>"}'
docker compose --profile gateway-worker up --build
```

Compose 使用 `host.docker.internal:8642`（Linux 通过 `host-gateway` 映射）。Hermes API 若只绑定 `127.0.0.1`，Linux 容器通常无法访问；Linux 优先在宿主机直接运行 Gateway Worker，或把 API Server 只绑定到受防火墙保护的 Docker bridge/VPN 地址，并相应设置 `HERMES_GATEWAY_URL`。不要为了容器访问直接把 `8642` 裸露到所有网络。生产环境在宿主机运行也能避免 keychain、SSH agent 和 Hermes 本地凭据进入容器。Compose 不启动、不依赖 Hermes Desktop。

### systemd / launchd

Coordinator、Command Worker、Gateway Worker 和 Telegram 应分别作为长期运行服务；每个服务使用独立低权限系统用户、独立 `.env` 和工作目录。Linux Gateway Worker 示例：

```ini
[Unit]
Description=Hermes Gateway Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=hermes
Group=hermes
WorkingDirectory=/opt/hermes
EnvironmentFile=/etc/hermes/gateway-worker.env
ExecStart=/opt/hermes/.venv/bin/python -m hermes.gateway_worker
Restart=on-failure
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

保存为 `/etc/systemd/system/hermes-gateway-worker.service` 后执行 `systemctl daemon-reload && systemctl enable --now hermes-gateway-worker`。Coordinator 和 Command Worker 只需替换 `EnvironmentFile` 与 `ExecStart`。

macOS 使用 launchd 时，将 `/opt/hermes/.venv/bin/hermes-gateway-worker` 放入 `ProgramArguments`，把 `WorkingDirectory` 指向只包含该角色 `.env` 的受限目录，并设置 `KeepAlive=true`。`.env` 权限应为 `600`；不要把 key 直接写进 unit、plist 或日志。修改 plist 后使用 `launchctl bootstrap` 加载，升级时先 `launchctl bootout`，备份数据库/配置，重新安装包后再加载。

### 数据库备份与迁移

升级会对 SQLite 执行 additive migration，不删除既有任务。停机或使用 SQLite 在线备份后再升级：

```bash
sqlite3 data/hermes.db ".backup 'data/hermes.db.backup-$(date +%Y%m%d%H%M%S)'"
```

恢复前先停止 Coordinator，再替换数据库并启动；不要在 WAL 活跃时只复制主 `.db` 文件。定期备份 `.db`、`.db-wal` 和 `.db-shm`，并验证恢复副本。Compose 使用 `hermes-data` volume，升级前应执行 `docker run --rm -v hermes-data:/data -v "$PWD":/backup alpine tar czf /backup/hermes-data.tgz /data`。

### 远程连接安全基线

- 推荐 Tailscale/WireGuard 私网；Worker 仅出站访问 Coordinator。
- HTTPS 反向代理或 SSH 本地转发用于跨网络访问；不要直接公开 Uvicorn `8000` 或 Hermes API `8642`。
- 每台 Worker 使用独立 HMAC secret；Gateway 每个 Profile 使用独立 key。
- Coordinator 只保存 route 标识和能力，不保存 Gateway/SSH/OAuth 凭据。
- Hermes Desktop 连接注册表和凭据不被本项目读取；Desktop 退出不影响 headless Worker。

基础 Docker 镜像不安装 Codex/Claude CLI，因此 Compose 默认使用内置 mock Planner，只用于安装验证和本地联调。生产 Coordinator 应优先在已安装并登录 Planner CLI 的宿主机运行；若必须容器化，请构建包含所选 Planner CLI 和受控认证挂载的派生镜像，并通过 `.env` 覆盖 `HERMES_PLANNER_DEFAULT_AGENT` 与 `HERMES_PLANNER_COMMANDS_JSON`。不要把 Planner 登录凭据烘焙进镜像。

启用 Telegram profile：

```bash
export HERMES_TELEGRAM_BOT_TOKEN='<BotFather Token>'
export HERMES_TELEGRAM_ALLOWED_USER_IDS='123456789'
docker compose --profile telegram up --build
```

生产 Worker 通常直接运行在目标机器上，以便访问该机器已安装的 Hermes CLI 和允许的工程目录。示例容器 Worker 默认使用 `mock_hermes`；如需容器内运行真实 Hermes，必须自行构建包含 Hermes CLI 的受控镜像。

## 配置原则

- 运行时以环境变量为准；进程启动时会读取当前目录的 `.env`。
- `.env.example` 给出全部变量，`config.example.yaml` 展示同一配置的结构化视图，但 MVP 不直接加载 YAML。
- Coordinator 不设置 Director Key 或 Worker Secret 时仍可提供健康检查，但受保护 API 返回 `503`，避免静默无鉴权运行。
- 不要提交 `.env`、Bot Token、共享密钥或真实任务输出数据库。

## 目录结构

```text
src/hermes/
  coordinator.py    FastAPI 应用与 API 鉴权
  gateway_adapter.py Hermes profile-scoped Runs API 客户端
  gateway_worker.py 无 GUI Gateway Worker
  planner.py        Coordinator 本地规划子进程与计划产物
  storage.py        SQLite Repository、原子领取和状态机
  worker.py         Worker API 客户端和子进程执行器
  telegram_bot.py   Telegram 长轮询与命令处理
  security.py       HMAC 签名与校验
  config.py         环境变量配置和脱敏工具
  models.py         API 模型和任务状态
tests/              单元与最小集成测试
scripts/e2e_mock.py 本地真实 HTTP mock 端到端验证
scripts/e2e_gateway_mock.py 无 Desktop 的 Gateway HTTP 端到端验证
docs/architecture.md 详细中文开发文档
```

## 安全提醒

Worker 从不使用 shell 拼接任务，不接受 `dangerously-bypass` 类参数，并把工作目录限制在配置根目录下。但 Hermes 本身仍可能执行高权限操作；应继续使用 Hermes 原生审批机制、最小权限系统账户、独立工程目录和受控网络。不要把本 MVP 直接裸露到公网。
