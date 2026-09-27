# 批量解压与进度查询 API 文档

## 概述

`POST /files/decompress` 在只传 `file` 时仍然同步解压单个压缩包（返回 `204`，行为不变）。
当传入 `files` 数组时，Wings 会把它作为一个后台批次执行，立即返回批次状态，
随后由面板轮询进度接口显示解压进度。这样一整个目录的大模型包不会再长时间占用一个 HTTP 请求。

批次内的压缩包按传入顺序**依次**解压，互不干扰：某一个失败不会中断其余文件，
失败原因记录在该文件的状态里。只有取消操作会终止整个批次。

## API 端点

### 1. 启动批量解压

**端点**: `POST /api/servers/:uuid/files/decompress`

**请求体**:
```json
{
  "root": "/",
  "files": ["modpack-a.zip", "modpack-b.tar.gz"]
}
```

**响应** `202 Accepted`：返回与进度接口相同批次状态。

### 2. 查询解压进度

**端点**: `GET /api/servers/:uuid/files/decompress-progress?batch_id=<id>`

省略 `batch_id` 时返回该服务器下的所有批次：`{ "batches": [ ... ] }`。

**响应示例**:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "root": "/",
  "status": "running",
  "file_name": "modpack-b.tar.gz",
  "file_index": 1,
  "total_files": 2,
  "bytes": 47185920,
  "total_bytes": 104857600,
  "progress": 0.45,
  "speed": 2097152,
  "files": [
    { "name": "modpack-a.zip", "status": "completed", "bytes": 52428800, "total": 52428800 },
    { "name": "modpack-b.tar.gz", "status": "running", "bytes": 47185920, "total": 52428800 }
  ],
  "timestamp": 1678800005,
  "elapsed_seconds": 23
}
```

**批次与文件状态**: `pending`、`running`、`completed`、`failed`、`cancelled`。

**字段说明**:
- `bytes` / `total_bytes`：所有文件已写出与预计写出的字节数，取自压缩包内的文件清单。
- `progress`：优先按字节计算；当无法预先枚举归档内容时（超大归档超过 5 秒的遍历预算），
  退化为按已完成的压缩包数量计算。批次结束后固定为 `1`。
- `speed`：最近一次采样的写入速度，单位字节/秒。
- `files[].error`：该压缩包失败的原因，成功时不返回该字段。

### 3. 取消批次

**端点**: `DELETE /api/servers/:uuid/files/decompress-progress/:batch_id`

已解压出来的文件保留不动，只跳过队列中尚未开始的压缩包。

**状态码**: `200` 已取消；`400` 缺少批次 ID；`403` 该批次不属于此服务器；`404` 批次不存在或已被清理。

## 注意事项

1. **批次保留时间**：批次结束后仍会被查询 `3` 分钟，之后从内存中清理，进度接口会返回 `404`。
2. **磁盘空间**：空间不足时不会中止批次，而是让剩余压缩包依次失败并在 `files[].error` 中说明，
   这样用户能确切知道哪些解压成功、哪些没有。
3. **认证**：与其他服务器文件接口一致，使用面板下发的服务器 JWT，进度接口只返回该服务器自己的批次。
4. **面板侧**：`/api/client/servers/:uuid/files/decompress` 与
   `/api/client/servers/:uuid/files/decompress-progress` 对应转发了上述接口。
