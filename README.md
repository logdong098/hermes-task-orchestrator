# hermes

`hermes` 是一个可运行的分布式 Hermes Agent 调度 MVP：用户通过一个 Telegram Director Bot 提交和管理任务，Coordinator 将任务可靠地分配给多台机器上的 Worker，Worker 在受限工作目录内调用本机 `hermes chat -q`，再把状态和结果回传。

核心 Coordinator 与 Worker 不依赖 Telegram。没有 Bot Token、没有真实 Hermes CLI 时，也可以使用内置 mock 完成测试和端到端流程。

详细设计见 [docs/architecture.md](docs/architecture.md)。

## 组件

- `coordinator`：FastAPI + SQLite，提供注册、心跳、任务、领取、取消、结果和通知 API。
- `worker`：轮询领取任务，以无 shell 的参数数组启动本机 Hermes CLI，支持并发、超时和取消。
- `telegram`：单个 Director Bot，提供 `/agents`、`/new`、`/status`、`/cancel`、`/help`，并推送结果。
- `mock_hermes`：不访问外部服务的假 Hermes 命令，仅用于开发和测试。

## 从零启动

要求 Python 3.9+。真实 Worker 还需要本机已安装可用的 `hermes` 命令。

```bash
cd /Users/log/Documents/hermes
python3 -m venv .venv
.venv/bin/python -m pip install --no-build-isolation -e '.[dev]'
cp .env.example .env
```

仓库同时提供 `setup.py/setup.cfg` 兼容入口；`--no-build-isolation` 会复用虚拟环境中已有的 setuptools，因此上述 editable 安装兼容 pip 21.2.4，不要求先升级 pip，也不会为了构建隔离环境重复下载 setuptools。如果受限网络只能使用环境中已有的运行时包，也可执行 `.venv/bin/python -m pip install -r requirements-dev.txt`，随后使用 Makefile（其命令会设置 `PYTHONPATH=src`）。

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

终端 1 启动 Coordinator：

```bash
make coordinator
```

终端 2 启动真实 Worker：

```bash
make worker
```

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

记下响应中的任务 ID：

```bash
curl -sS http://127.0.0.1:8000/api/v1/tasks/<TASK_ID> \
  -H "Authorization: Bearer ${DIRECTOR_KEY}"
```

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

## 测试和验证

测试不需要 Telegram Token、真实 Hermes 或外部网络：

```bash
make test
make lint
make format-check
make e2e
```

`make e2e` 会在随机本地端口启动真实 Uvicorn Coordinator，创建任务，启动一次性 mock Worker，等待执行并校验最终结果。

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
  storage.py        SQLite Repository、原子领取和状态机
  worker.py         Worker API 客户端和子进程执行器
  telegram_bot.py   Telegram 长轮询与命令处理
  security.py       HMAC 签名与校验
  config.py         环境变量配置和脱敏工具
  models.py         API 模型和任务状态
tests/              单元与最小集成测试
scripts/e2e_mock.py 本地真实 HTTP mock 端到端验证
docs/architecture.md 详细中文开发文档
```

## 安全提醒

Worker 从不使用 shell 拼接任务，不接受 `dangerously-bypass` 类参数，并把工作目录限制在配置根目录下。但 Hermes 本身仍可能执行高权限操作；应继续使用 Hermes 原生审批机制、最小权限系统账户、独立工程目录和受控网络。不要把本 MVP 直接裸露到公网。
