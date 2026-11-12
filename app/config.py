"""运行参数，全部可用环境变量覆盖。"""

import os
from pathlib import Path

# SQLite 数据库文件位置（容器内默认挂载到 /data 卷）
DB_PATH = Path(os.getenv("PUMP_DB_PATH", "/data/pump_tests.db"))

# 每级摘要的块大小（采样点数 / 子块数），level 1 为最细摘要
BLOCK_SIZE = int(os.getenv("PUMP_BLOCK_SIZE", "128"))

# 单次上传 CSV 的最大字节数，避免误传超大文件打满内存
MAX_UPLOAD_BYTES = int(os.getenv("PUMP_MAX_UPLOAD_BYTES", str(200 * 1024 * 1024)))

# 一次分桶查询允许的最大桶数，防止桶宽过小导致结果爆炸
MAX_BUCKETS = int(os.getenv("PUMP_MAX_BUCKETS", "100000"))

# 流式查询：桶 JSON 片段在内存中缓冲的字节上限，超出自动滚动到临时文件
STREAM_SPOOL_BYTES = int(os.getenv("PUMP_STREAM_SPOOL_BYTES", str(8 * 1024 * 1024)))

# 流式查询：回读缓冲文件发给客户端的块大小
STREAM_CHUNK_BYTES = int(os.getenv("PUMP_STREAM_CHUNK_BYTES", str(64 * 1024)))

# 导入时每批写入 SQLite 的采样行数（控制内存峰值）
IMPORT_BATCH_SIZE = int(os.getenv("PUMP_IMPORT_BATCH_SIZE", "10000"))

# 时间戳列 / 压力列允许的表头（小写比较，支持中文表头）
TIMESTAMP_COLUMNS = (
    "timestamp",
    "timestamp_ms",
    "ts",
    "time_ms",
    "t_ms",
    "时间戳",
    "毫秒时间戳",
    "时间",
)
PRESSURE_COLUMNS = (
    "pressure",
    "pressure_kpa",
    "pressurekpa",
    "p",
    "压力",
    "压力kpa",
    "压力(kpa)",
    "压力（kpa）",
)
