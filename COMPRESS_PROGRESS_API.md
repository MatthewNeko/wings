# 压缩进度查询 API 文档

## 概述

`POST /files/compress` 默认仍然同步打包并把生成的压缩包信息返回给调用方（行为不变）。
当请求体带上 `"background": true` 时，Wings 会把打包放到后台执行，立即返回任务状态，
随后由面板轮询进度接口显示压缩进度。这样大目录不再需要长时间挂住一个 HTTP 请求，
用户也能中途取消。

一次压缩请求只会产出一个 `archive-{时间}.tar.gz`，因此这里没有批次概念，
进度只有一个任务、一条按字节计算的进度条。

## API 端点

### 1. 启动后台压缩

**端点**: `POST /api/servers/:uuid/files/compress`

**请求体**:
```json
{
  "root": "/",
  "files": ["plugins", "server.properties"],
  "background": true
}
```

**响应** `202 Accepted`：返回与进度接口相同的任务状态。

不带 `background`（或为 `false`）时走原有的同步分支，响应 `200` 与压缩包信息，
老版本面板不受影响。磁盘空间预检不足时依旧返回 `409`。

### 2. 查询压缩进度

**端点**: `GET /api/servers/:uuid/files/compress-progress?job_id=<id>`

省略 `job_id` 时返回该服务器下的所有任务：`{ "jobs": [ ... ] }`。

**响应示例**:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "root": "/",
  "status": "running",
  "files": ["plugins", "server.properties"],
  "total_files": 2,
  "archive_name": "",
  "bytes": 471859200,
  "total_bytes": 1048576000,
  "progress": 0.45,
  "speed": 20971520,
  "timestamp": 1678800005,
  "elapsed_seconds": 23
}
```

**任务状态**: `pending`、`running`、`completed`、`failed`、`cancelled`。

**字段说明**:
- `bytes` / `total_bytes`：已经写入归档的源文件字节数与预计总量。注意统计的是**压缩前**
  的源数据，否则 gzip 之后的体积永远只有总量的零头，进度条会看起来卡在半路。
- `total_bytes`：来自打包前对源目录的一次遍历，预算 `5` 秒。超大目录树遍历不完时返回偏小的值，
  甚至返回 `0`，此时面板应展示不确定态的进度条而不是一个假的百分比。
- `progress`：`bytes / total_bytes`，上限 `1`；任务完成后固定为 `1`。
- `speed`：最近一次采样的写入速度，单位字节/秒，每秒刷新。
- `archive_name`：压缩包名，只在任务成功后出现；失败与取消时保持为空。
- `error`：失败原因，成功时不返回该字段。

### 3. 取消压缩

**端点**: `DELETE /api/servers/:uuid/files/compress-progress/:job_id`

取消会中断正在写入的归档，并**删除已经写了一半的 `archive-*.tar.gz`**，
避免文件列表里留下一个看起来正常、实际打不开的压缩包。

**状态码**: `200` 已取消；`400` 缺少任务 ID；`403` 该任务不属于此服务器；`404` 任务不存在或已被清理。

## 注意事项

1. **任务保留时间**：任务结束后仍会被查询 `3` 分钟，之后从内存中清理，进度接口返回 `404`。
2. **不跟随请求生命周期**：后台任务不挂在 HTTP 请求的 context 上，面板断开或请求超时都不会
   让打包中途停下，否则会留下一堆没人知道成因的半截归档文件。要停止只能显式调用取消接口。
3. **并发**：每个任务各自写一个带时间戳的归档文件，互不覆盖；同一批文件被重复打包是允许的。
4. **认证**：与其他服务器文件接口一致，使用面板下发的服务器 JWT，进度接口只返回该服务器自己的任务。
5. **面板侧**：`/api/client/servers/:uuid/files/compress` 透传 `background`，
   `/api/client/servers/:uuid/files/compress-progress` 与
   `/api/client/servers/:uuid/files/compress-progress/{job_id}` 对应进度查询与取消。
   老版本 Wings 不认识 `background`，会直接返回打包完成的文件对象，面板据此降级为无进度条的普通压缩。
