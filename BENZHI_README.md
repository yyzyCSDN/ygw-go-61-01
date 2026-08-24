# SessionStore

SessionStore 是一个分布式会话状态存储服务：会话按节点分片保存，支持创建、
续期、读取与淘汰；会话携带版本号与 TTL，请求通过粘性路由打到持有会话的节点，
变更同步到镜像，断线重连后按版本恢复。

## 构建与运行

```bash
# 离线构建（vendor 已随仓库提供）
go build -mod=vendor -o sessionstore ./cmd/sessionstore

# 本地启动
./sessionstore
```

服务默认监听 `:8080`，可通过环境变量覆盖：

- `SESSIONSTORE_ADDR`：监听地址
- `SESSIONSTORE_TTL`：默认会话 TTL（如 `30m`）
- `SESSIONSTORE_READ_TIMEOUT`：读取超时
- `SESSIONSTORE_EVICT_INTERVAL`：过期淘汰扫描间隔
- `SESSIONSTORE_CLEAN_INTERVAL`：清理扫描间隔
- `SESSIONSTORE_CLEAN_BATCH`：清理批次大小
- `SESSIONSTORE_NODES`：节点 ID 列表（逗号分隔）
- `SESSIONSTORE_NODE_ADDRS`：节点地址列表（逗号分隔）

## 接口

- `GET /`：会话监控页面
- `GET /api/healthz`：健康检查
- `POST /api/sessions`：创建会话（body：`{"id":"...","ttl_seconds":3600}`）
- `POST /api/sessions/{id}/renew`：续期
- `GET /api/sessions/{id}`：读取
- `POST /api/sessions/{id}/evict`：强制淘汰
- `POST /api/sessions/{id}/migrate`：迁移到新节点（body：`{"node":"node-b"}`）
- `GET /api/sessions`：列出会话
- `GET /api/monitor`：监控数据
- `POST /api/maintenance/rebalance`：触发分片重平衡

## 测试

```bash
go test ./...
go vet ./...
```

## Docker

```bash
bash build_benzhi_docker.sh
docker run --rm -p 8080:8080 sessionstore:local
```
