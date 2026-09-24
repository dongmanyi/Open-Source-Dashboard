# 快速开始

[文档首页](README.md) · [架构与数据口径](architecture.md) · [API 参考](api.md) · [运行与维护](operations.md)

项目支持 Docker Compose 和本机开发两种运行方式。首次体验推荐 Docker Compose。

## 环境要求

Docker Compose：

- Docker Engine
- Docker Compose

本机开发：

- Node.js 20+
- PostgreSQL
- Redis

## Docker Compose

### 1. 配置环境变量

在仓库根目录执行：

```bash
cp .env.docker.example .env
```

Windows PowerShell：

```powershell
Copy-Item .env.docker.example .env
```

编辑根目录 `.env`，至少填写：

```env
GITHUB_TOKEN=YOUR_GITHUB_PERSONAL_ACCESS_TOKEN
POSTGRES_PASSWORD=your_postgres_password
```

如需调整宿主机端口：

```env
BACKEND_PORT=3000
FRONTEND_PORT=8080
```

根目录 `.env.docker.example` 供 Docker Compose 使用。本机启动后端时请使用 `backend/.env.local.example`。

### 2. 启动服务

```bash
docker compose up -d --build
```

Compose 会依次启动 PostgreSQL、执行数据库迁移、启动 Redis、后端和前端。数据库迁移失败时，后端不会启动。

默认地址：

- 前端：`http://localhost:8080`
- 后端：`http://localhost:3000`

### 3. 检查状态

```bash
docker compose ps
docker compose logs migrate
docker compose logs backend
docker compose logs frontend
```

验证前后端代理：

```bash
curl http://localhost:3000/api/v1/organization/sigs
curl http://localhost:8080/api/v1/organization/sigs
```

### 4. 首次数据回填

服务默认不会在启动时执行历史回填。如果页面能打开但暂无图表数据，执行：

```bash
docker compose exec backend node run_graphql_backfill.js 30
```

需要在回填后清空 Redis 时，显式追加：

```bash
docker compose exec backend node run_graphql_backfill.js 30 --flush-cache
```

更多数据修复方式见[运行与维护](operations.md)。

### 5. 停止服务

仅停止并删除容器和网络，保留数据卷：

```bash
docker compose down
```

以下命令会删除数据库和 Redis 数据卷，并删除本地构建镜像，只适合可丢弃的测试环境：

```bash
docker compose down -v --rmi local
```

## 本机开发

### 1. 初始化数据库

```bash
createdb oss_dashboard
psql -d oss_dashboard -f db/schema.sql
psql -d oss_dashboard -f db/seed.sql
psql -d oss_dashboard -f db/contributors_schema.sql
```

可选的物化视图：

```bash
psql -d oss_dashboard -f db/views.sql
```

从旧版本升级已有数据库时，不要重新执行完整 schema；请按顺序运行迁移：

```bash
psql -d oss_dashboard -f db/migrations/001_github_custom_property_sigs.sql
psql -d oss_dashboard -f db/migrations/002_repository_organization_membership.sql
psql -d oss_dashboard -f db/migrations/003_organization_ingestion_freshness.sql
psql -d oss_dashboard -f db/migrations/004_snapshot_generation.sql
```

执行 `002` 后，启动后端或运行 `npm run sync-repository-sigs`，以 GitHub 当前仓库列表刷新组织成员状态。

### 2. 启动后端

```bash
cd backend
cp .env.local.example .env
npm install
npm start
```

Windows PowerShell：

```powershell
cd backend
Copy-Item .env.local.example .env
npm install
npm start
```

在 `backend/.env` 中配置 PostgreSQL、Redis、GitHub Token 和 Repository Custom Property 名称。

### 3. 启动前端

在另一个终端中执行：

```bash
cd frontend
npm install
npm run dev
```

按 Vite 输出的地址打开仪表板。开发服务器会将 `/api` 请求代理到本机后端。
