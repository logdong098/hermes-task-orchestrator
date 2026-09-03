# hermes

`hermes` 是一个可运行的分布式开发任务调度 MVP：用户通过 Telegram Director Bot 或 API 提交任务，复杂任务由主控 Codex 强制使用 `codex-with-chatgpt` Skill 生成执行计划，再可靠地分配给 Worker。Worker 在指定目录调用 Claude Code（`cc`）或其他配置的执行 Agent；简单任务可以显式跳过规划。

核心 Coordinator 与 Worker 不依赖 Telegram。没有 Bot Token、没有真实 Hermes CLI 时，也可以使用内置 mock 完成测试和端到端流程。

详细设计见 [docs/architecture.md](docs/architecture.md)。

## 组件

- `coordinator`：FastAPI + SQLite，负责认证 API、任务状态机，以及外部 Codex with ChatGPT 规划协议。
- `worker`：单一 Worker 进程，按默认或任务指定的 Agent 分流；Hermes 任务走 Gateway，Codex/Claude Code 任务启动本机 CLI，并统一负责心跳、取消、状态和结果回传。
- `telegram`：单个 Director Bot，提供 `/agents`、`/tasks`、`/new`、`/status`、`/cancel`、`/help`，并推送结果。
- `mock_hermes`：不访问外部服务的假 Hermes 命令，仅用于开发和测试。

`gateway-worker` 仍作为迁移期兼容入口，但内部复用同一个 Unified Worker 控制环。Hermes Desktop 不是运行依赖；M2 Desktop Fleet Plugin、M3 自动路由与 `task_attempts` 历史审计、M4 Bridge/delegation 当前均未实现。

## 从零启动

要求 Python 3.9+。真实 Worker 还需要本机已安装可用的执行 Agent；按本文示例为 Claude Code（`cc`）。

```bash
cd /Users/log/Documents/hermes
python3 -m venv .venv
.venv/bin/python -m pip install --no-build-isolation -e '.[dev]'
cp .env.example .env
```

`.env.example` 是配置入口。Coordinator、Unified Worker 和 Telegram 可以使用各自机器上的 `.env`；不要把真实 key 写入 Git 或复制到 Coordinator。

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
| Coordinator | 数据库、Director key、Worker secret | `hermes-coordinator` |
| Codex 主控 | Coordinator URL、Director key、当前项目的 Codex with ChatGPT 连接 | 由 Codex 会话执行 Skill |
| Unified Worker | Coordinator URL、Worker ID/secret、Agent 命令、Gateway URL/Profiles 和各 Profile key | `hermes-worker` |
| 兼容 Gateway Worker | 同上，仅保留旧配置/入口兼容 | `hermes-gateway-worker` |
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

Coordinator 不启动本地 Planner CLI。所有非 `direct` 任务都固定进入 `codex-with-chatgpt` 规划队列；主控 Codex 使用该 Skill 完成 `INIT → PLAN`，再把计划回写 Coordinator：

```text
POST /api/v1/planner/tasks/claim
POST /api/v1/tasks/<TASK_ID>/plan
```

主控必须实际执行 `codex-with-chatgpt` Skill；Hermes 不会把普通 `codex exec`、ChatGPT 登录态或本地 Planner 输出伪装成该 Skill。规划结果保存到任务的 `plan` 字段，并组合成 Worker 的 `execution_prompt`。

主控和 Worker 可以使用不同的本地目录：ChatGPT 通过当前 Codex workspace 读取项目，Worker 在 `HERMES_WORKER_ALLOWED_WORKDIR` 下解析任务的 `workdir`，并把该目录作为 `cc` 的 `cwd`。例如 Worker 根目录是 `/srv/code` 时，Telegram 使用 `/new --worker worker-a --workdir project-a --executor cc 实现功能`，实际执行目录就是 `/srv/code/project-a`。两份代码不会由 Hermes 自动同步；如果主控和 Worker 是两份独立副本，需要自行保持版本/文件一致，或使用共享文件系统/版本同步。`workdir` 只允许位于 Worker 根目录内，越界或不存在会被拒绝。

Unified Worker 未收到 `execution_agent` 时使用 `HERMES_WORKER_DEFAULT_AGENT`。本地 Codex/Claude Code 使用 Agent 命令映射；Hermes 使用 Gateway 配置：

```dotenv
HERMES_WORKER_DEFAULT_AGENT=cc
HERMES_WORKER_AGENTS_JSON={"cc":["claude","-p","{prompt}"],"codex":["codex","exec","{prompt}"]}
HERMES_GATEWAY_URL=http://127.0.0.1:8642
HERMES_GATEWAY_ID=local-hermes
HERMES_GATEWAY_PROFILES=default
HERMES_GATEWAY_DEFAULT_PROFILE=default
HERMES_GATEWAY_PROFILE_KEYS_JSON={"default":"<profile-key>"}
```

旧部署也可将同样格式配置在 `HERMES_AGENT_COMMANDS`；`hermes-worker` 会将它作为兼容别名读取。`claude`/`cc` 是 `claude-code` 的兼容别名，新的稳定 Agent ID 是 `claude-code`。

Windows 上 npm 安装的 Claude Code 可能是 `claude.cmd`。Worker 会在创建子进程前用系统 PATH 解析命令 shim，避免 `create_subprocess_exec("claude")` 无法启动。若使用 headless Claude Code 并需要跳过交互式 allow 审批，可在 Worker 机器的 `.env` 中配置：

```dotenv
HERMES_AGENT_COMMANDS={"cc":["claude","-p","{prompt}","--dangerously-skip-permissions"]}
```

该选项只应在受控 Worker 工作目录启用，并遵循 Claude Code 当前版本的 deny 规则与安全策略。

Coordinator 主机不需要安装 Planner CLI；主控 Codex 需要能使用当前项目的 `codex-with-chatgpt` Skill。Worker 主机只需安装自己映射中的执行 Agent。`HERMES_WORKER_COMMAND` 继续作为默认 Agent 的兼容入口。

现有 SQLite 数据库启动时会原地增加规划字段，不会重建表，也不会把已经存在的 `pending/claimed/running` 任务重新送去规划；这些旧任务继续使用原始 `prompt` 执行。

终端 1 启动 Coordinator：

```bash
make coordinator
```

终端 2 启动真实 Worker：

```bash
make worker
```

### Unified Worker 与 Hermes Gateway（无 Hermes Desktop）

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

M1 使用 `/p/<profile>/v1/runs`、`/p/<profile>/v1/runs/<run_id>` 和对应的 `/stop` 接口。Unified Worker 同时访问 Coordinator、该 API Server 和本机工程目录：

```bash
HERMES_WORKER_ID=mac-unified-worker \
HERMES_WORKER_DEFAULT_AGENT=hermes \
HERMES_GATEWAY_ID=mac-hermes \
HERMES_GATEWAY_KIND=local \
HERMES_GATEWAY_URL=http://127.0.0.1:8642 \
HERMES_GATEWAY_PROFILES=architect,coder \
HERMES_GATEWAY_DEFAULT_PROFILE=architect \
HERMES_GATEWAY_PROFILE_KEYS_JSON='{"architect":"<architect-key>","coder":"<coder-key>"}' \
hermes-worker
```

每个 Profile 使用独立 key；`target_gateway_id` 与 `target_profile` 必须同时指定。任务被 Coordinator 精确解析后，Unified Worker 会把 resolved Profile 传给 Hermes API，不会把同 Gateway 的其他 Profile 当作 fallback。没有 Gateway/Profile 定位时，只有单个 Profile 或显式默认 Profile 才会执行默认 Hermes 任务，避免随机选择错误 Profile。关闭 Desktop 不影响这条生产路径。

`--planner` 不选择本地 CLI；规划始终由主控 Codex 使用 `codex-with-chatgpt` Skill 完成。执行任务时由同一个 Unified Worker 按默认配置或显式覆盖选择后端：

```text
@<worker-id> <任务>             # 使用 Worker 默认 Agent
@<worker-id> -codex <任务>      # 本次使用 Codex
@<worker-id> -cc <任务>         # 本次使用 Claude Code
@<worker-id> --executor claude-code <任务>  # 长参数兼容写法
```

如果把 `--executor codex` 指向 Gateway/Profile 路由，或指定了 Worker 不支持的 Agent，任务会保持 `pending` 并显示路由不支持该 Agent 的原因，不会悄悄 fallback 到其他 Agent。

`HERMES_GATEWAY_PROFILES` 建议在生产环境显式配置。若留空并依赖 `GET /api/profiles` 自动发现，则 `HERMES_GATEWAY_TOKEN` 必须设置为 `HERMES_GATEWAY_DEFAULT_PROFILE`（默认名为 `default`）的 key；命名 Profile 的请求仍必须使用 `HERMES_GATEWAY_PROFILE_KEYS_JSON` 中各自的 key，缺失时 Unified Worker 会拒绝执行，不会回退到默认 key。

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

指定执行 Agent（规划固定由主控 Codex Skill 完成）：

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/tasks \
  -H "Authorization: Bearer ${DIRECTOR_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"实现并测试健康检查接口","planner_agent":"codex-with-chatgpt","execution_agent":"cc"}'
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

交互式 OpenAPI 文档位于 `http://127.0.0.1:8000/docs`，健康检查为 `GET /healthz`。规划控制器使用 Director key 调用规划领取、租约续期和计划回写接口。

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
/new [--planner codex-with-chatgpt] [--worker <id>] [--workdir <目录>] [--agent <agent>] <任务描述>
@worker 修复登录页文案并运行相关测试
@worker -cc 修复登录页文案并运行相关测试
@worker -codex 修复登录页文案并运行相关测试
@worker-a 修复登录页文案并运行相关测试  # 也可以指定具体 Worker ID
@Coordinator 分析这个开发计划并给出执行方案
@Coordinator @worker-a 分析并由指定 Worker 实现登录模块
```

`@worker` 使用 `direct` 模式，跳过 Planner，由 Coordinator 选择在线 Worker；在单 Worker 部署中就是唯一的 Unified Worker。也可以写具体 Worker ID（如 `@worker-a`）。任务仍由 Coordinator 持久化、鉴权、调度和跟踪；未指定 Agent 时使用 Worker 默认配置，`-codex`、`-cc` 或 `--executor`（兼容别名 `--agent`）会覆盖本次任务。`@Coordinator` 使用 `plan` 模式；它与 Worker 的先后顺序可以互换。当前 P0/P1 一条任务只允许一个 Worker，`->`、多 Worker DAG 和工作流编排暂未实现。

M1 Telegram 路由语法为：

```text
/new --planner codex-with-chatgpt --gateway mac-hermes --profile architect --executor hermes 检查工作区
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

`make e2e` 会运行两条真实 HTTP 进程链路：本地 Agent mock，以及直接调用 profile-scoped Runs API 的 Unified Worker mock。第二条链路不会启动或依赖 Hermes Desktop GUI。

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

Compose 中的 `gateway-worker` profile 仍可作为旧 Gateway 入口兼容测试；新部署建议把 Gateway 配置合并到 `worker` profile：

```bash
export HERMES_GATEWAY_PROFILE_KEYS_JSON='{"default":"<profile-key>"}'
docker compose --profile gateway-worker up --build
```

Compose 使用 `host.docker.internal:8642`（Linux 通过 `host-gateway` 映射）。Hermes API 若只绑定 `127.0.0.1`，Linux 容器通常无法访问；Linux 优先在宿主机直接运行 Unified Worker，或把 API Server 只绑定到受防火墙保护的 Docker bridge/VPN 地址，并相应设置 `HERMES_GATEWAY_URL`。不要为了容器访问直接把 `8642` 裸露到所有网络。生产环境在宿主机运行也能避免 keychain、SSH agent 和 Hermes 本地凭据进入容器。Compose 不启动、不依赖 Hermes Desktop。

### systemd / launchd

Coordinator、Unified Worker 和 Telegram 应分别作为长期运行服务；每个服务使用独立低权限系统用户、独立 `.env` 和工作目录。Linux Unified Worker 示例：

```ini
[Unit]
Description=Hermes Unified Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=hermes
Group=hermes
WorkingDirectory=/opt/hermes
EnvironmentFile=/etc/hermes/worker.env
ExecStart=/opt/hermes/.venv/bin/python -m hermes.worker
Restart=on-failure
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

保存为 `/etc/systemd/system/hermes-worker.service` 后执行 `systemctl daemon-reload && systemctl enable --now hermes-worker`。Coordinator 和 Telegram 只需替换 `EnvironmentFile` 与 `ExecStart`。

macOS 使用 launchd 时，将 `/opt/hermes/.venv/bin/hermes-worker` 放入 `ProgramArguments`，把 `WorkingDirectory` 指向只包含该角色 `.env` 的受限目录，并设置 `KeepAlive=true`。`.env` 权限应为 `600`；不要把 key 直接写进 unit、plist 或日志。修改 plist 后使用 `launchctl bootstrap` 加载，升级时先 `launchctl bootout`，备份数据库/配置，重新安装包后再加载。

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

基础 Docker 镜像不安装 Codex/Claude CLI；Compose 只启动 Coordinator 和可选 mock Worker。生产环境需要让主控 Codex 使用当前项目的 `codex-with-chatgpt` Skill，并通过规划 API 把 PLAN 回写 Coordinator。不要把 ChatGPT 连接凭据或 Planner 会话状态烘焙进镜像。

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
  gateway_worker.py 旧 Gateway Worker 兼容入口
  planner.py        Codex with ChatGPT 规划协议与回写客户端
  storage.py        SQLite Repository、原子领取和状态机
  worker.py         Unified Worker 控制环、Agent 分流和子进程执行器
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
