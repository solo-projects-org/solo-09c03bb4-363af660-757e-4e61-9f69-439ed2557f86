# 泵站启泵试验 · 水锤压力峰谷分析服务

面向泵站调试员的启泵试验复盘工具：把记录仪导出的**单次试验 CSV** 导入
FastAPI 服务，采样与多级摘要落盘到 **SQLite**；复盘时给定起止时刻和桶宽，
按桶返回首尾点与压力最高 / 最低点，快速定位水锤造成的峰谷，空桶自动标记缺测。

## 功能与约定

- **CSV 导入**（`POST /tests/import`，Swagger 可直接上传）
  - 首行表头，自动识别时间戳列与压力列，支持中英文表头
    （`timestamp_ms / ts / 时间戳 …`、`pressure_kpa / pressure / 压力(kPa) …`），
    UTF-8 与 GBK 编码均可
  - 毫秒时间戳必须为整数且**严格递增**，重复或倒序整次拒绝（HTTP 400）
  - 压力为有限数值，单位 kPa；单事务写入，失败不留半成品
  - 逐行解析、分批入库，内存占用与文件大小无关，百万级采样可在
    256 MiB 容器内导入
- **分桶查询**（`GET /tests/{id}/query`）
  - 从 `start_ts` 起按 `bucket_width_ms` 分桶，**含左端、不含右端**，
    末桶截到 `end_ts`
  - 每桶按时序返回首点、尾点、压力最高点、最低点；**同一点只列一次**，
    角色合并到 `roles`；极值并列时取时间戳最早的点
  - 桶内无采样时 `missing=true`、`points=[]`
  - **流式响应**：单趟完成取数与序列化，桶 JSON 片段经容量受限的缓冲
    （默认内存 8 MiB，超出落临时文件）增量发回客户端，不在内存中堆积
    全部桶或整份响应——10 万个非空桶、每桶四点各不相同时，也能在
    256 MiB 容器内一次返回完整 JSON，且照常给出 `Content-Length`
- **多级摘要加速**（核心）
  - level 1 为最细索引块（默认每 128 个采样一块），其上逐级合并，
    每块保存首尾点、最高 / 最低点（含时间戳）、采样数与原始 rowid 区间
  - 查询时从最粗层级开始：**完整落在桶内的块直接采用摘要**；
    只有桶边界切开 level 1 最细块时，才按 rowid 区间回读原始采样
  - 块检索按各层最大块跨度裁剪扫描下界，逐桶检索开销与试验长度无关
  - 响应中的 `metrics` 如实回显：`blocks_by_level`（各级摘要命中数）
    与 `raw_rows_read`（回读原始行数，边界不切块时为 0）

## 目录结构

```
app/
  config.py      环境变量配置（库路径、块大小、上传/桶数上限、流式缓冲）
  db.py          SQLite 连接、建表（tests / samples / blocks）
  ingest.py      CSV 流式解析、严格校验、分批导入
  summaries.py  多级摘要构建与区间递归查询
  service.py     分桶编排、流式序列化（点去重与角色合并）
  schemas.py     Pydantic 请求 / 响应模型
  main.py        FastAPI 路由
sample_data/     样例 CSV（由 scripts/generate_sample.py 生成）
scripts/
  generate_sample.py   样例生成（含 100 万点长记录）
  query_long_demo.sh   长记录端到端演示：生成 -> 导入 -> 10 万桶查询
tests/           pytest 测试（24 项）
Dockerfile, docker-compose.yml
```

## Docker Compose 启动

```bash
docker compose up --build
```

- 服务：<http://localhost:8000>
- Swagger UI：<http://localhost:8000/docs>
- 健康检查：`GET /health`
- SQLite 数据保存在命名卷 `pump-data`（容器内 `/data/pump_tests.db`）
- **内存上限 256 MiB**（`mem_limit` / `deploy.resources.limits.memory`）：
  导入与 10 万桶流式查询都在该上限内运行

## 复现样例试验

样例 `sample_data/pump_start_demo.csv` 为 2 万点、10 ms 间隔的合成启泵曲线
（静压 → 启泵爬升 → 衰减振荡，含一次低压深谷），可重新生成：

```bash
python3 scripts/generate_sample.py
```

在 Swagger（`/docs`）中的操作步骤：

1. `POST /tests/import`：`name` 填 `demo-001`，`file` 选择
   `sample_data/pump_start_demo.csv`，执行。响应返回试验 id、时间范围与各级块数。
2. `GET /tests`：确认试验已入库。
3. `GET /tests/{id}/query`：
   - `start_ts=1731300000000`、`end_ts=1731300019999`、`bucket_width_ms=1000`
     → 200 个 1 秒桶，逐桶查看峰谷；
   - 改成 `bucket_width_ms=1280`（与块边界对齐）再试，
     可见 `metrics.raw_rows_read = 0`，全程只走摘要；
   - 换 `start_ts=1731299990000&end_ts=1731299995000` 可看到全部桶 `missing=true`。

小样本 `sample_data/small_demo.csv`（60 点）适合在 Swagger 里肉眼逐桶核对。

### curl 示例

```bash
curl -X POST http://localhost:8000/tests/import \
  -F "name=demo-001" \
  -F "file=@sample_data/pump_start_demo.csv;type=text/csv"

curl "http://localhost:8000/tests/1/query?start_ts=1731300000000&end_ts=1731300019999&bucket_width_ms=1000"
```

## 长记录 · 10 万桶流式查询演示

`scripts/generate_sample.py --long` 生成 `sample_data/pump_start_long.csv`
（100 万点、1 ms 间隔、约 16.7 分钟，带确定性抖动，绝大多数桶的首/尾/
最高/最低点互不相同）。按 10 ms 桶宽查询整段即得 **100 000 个非空桶**，
响应约 32 MB，流式返回，服务端内存恒定。

一键演示（自动生成、导入并查询，已入库的同名试验直接复用不重导）：

```bash
docker compose up --build -d
scripts/query_long_demo.sh
```

或分步手动执行：

```bash
# 1. 生成长记录（约 21 MB；已加入 .gitignore）
python3 scripts/generate_sample.py --long

# 2. 导入（约 100 万行，数十秒；256 MiB 容器内可完成）
curl -X POST http://localhost:8000/tests/import \
  -F "name=long-001" \
  -F "file=@sample_data/pump_start_long.csv;type=text/csv"

# 3. 一次查询 10 万个非空桶（1 ms 采样 + 10 ms 桶宽，每桶 10 点）
curl "http://localhost:8000/tests/1/query?start_ts=1731300000000&end_ts=1731301000000&bucket_width_ms=10" \
  -o buckets_100k.json

# 4. 对照：128 ms 桶宽与块边界对齐时全程只走摘要（raw_rows_read = 0）
curl "http://localhost:8000/tests/1/query?start_ts=1731300000000&end_ts=1731300128000&bucket_width_ms=128"
```

## 本地开发（无 Docker）

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest tests/ -q
PUMP_DB_PATH=./data/dev.db python3 -m uvicorn app.main:app --reload --port 8000
```

## 关键环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PUMP_DB_PATH` | `/data/pump_tests.db` | SQLite 文件路径 |
| `PUMP_BLOCK_SIZE` | `128` | 每级摘要块大小（点数 / 子块数） |
| `PUMP_MAX_UPLOAD_BYTES` | `209715200` | 单次上传字节上限（200 MB） |
| `PUMP_MAX_BUCKETS` | `100000` | 单次查询桶数上限，防止桶宽过小 |
| `PUMP_STREAM_SPOOL_BYTES` | `8388608` | 流式查询桶片段的内存缓冲上限，超出落临时文件 |
| `PUMP_STREAM_CHUNK_BYTES` | `65536` | 流式回读缓冲文件的发送块大小 |
| `PUMP_IMPORT_BATCH_SIZE` | `10000` | 导入时每批写入 SQLite 的采样行数 |
