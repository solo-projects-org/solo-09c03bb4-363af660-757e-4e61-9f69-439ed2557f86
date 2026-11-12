"""分桶查询业务逻辑。

10 万个非空桶、每桶 4 个特征点各不相同时，完整响应模型在内存中可达
数百 MB（Pydantic 对象树 + 字典树 + JSON 字符串三份拷贝），超出
256 MiB 容器上限。因此这里不在内存中组装整个响应：

- 单趟遍历所有桶，把每个桶的 JSON 片段增量写入 SpooledTemporaryFile
  （内存占用 capped by PUMP_STREAM_SPOOL_BYTES，超出自动落临时文件）；
- 同时累计 missing_bucket_count 与 metrics（读取统计）；
- 调用方拿到 头部字节 + 片段文件 + 尾部字节 后流式发给客户端。

序列化格式与 FastAPI 返回 QueryResponse 完全一致（字段顺序相同、
紧凑分隔符、ensure_ascii=False），响应结构保持不变。
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from typing import BinaryIO, NamedTuple

from . import config
from .summaries import Stats, level_time_spans, range_summary

_ROLE_ORDER = ("first", "last", "max", "min")
# 角色 -> Stats 中对应的 (ts 字段, pressure 字段)
_ROLE_FIELDS = {
    "first": ("first_ts", "first_pressure"),
    "last": ("last_ts", "last_pressure"),
    "max": ("max_pressure_ts", "max_pressure"),
    "min": ("min_pressure_ts", "min_pressure"),
}

# 与 Starlette JSONResponse 相同的序列化参数：紧凑分隔符、不转义非 ASCII
_JSON_KWARGS = {"ensure_ascii": False, "allow_nan": False, "separators": (",", ":")}


class QueryStream(NamedTuple):
    """一次分桶查询的流式响应体：头部 + 桶片段文件 + 尾部。"""

    header: bytes  # 到 "buckets":[ 为止（含），此前为全部标量字段与 metrics
    body: BinaryIO  # 各桶 JSON 片段（逗号分隔），已 seek 到起点；用毕需 close()
    trailer: bytes  # 收尾的 ]}
    content_length: int  # 三段字节数之和，用于 Content-Length


def _points_from_stats(stats: Stats) -> list[dict]:
    """把四点统计展开为按时序、去重后的点字典；同一点合并多个角色。"""
    by_ts: dict[int, dict] = {}
    for role in _ROLE_ORDER:
        ts_field, p_field = _ROLE_FIELDS[role]
        ts = getattr(stats, ts_field)
        pressure = getattr(stats, p_field)
        if ts is None:
            continue
        entry = by_ts.setdefault(ts, {"ts": ts, "pressure": pressure, "roles": []})
        entry["roles"].append(role)
    return [by_ts[ts] for ts in sorted(by_ts)]


def _bucket_fragment(
    bucket_index: int, b_start: int, b_end: int, stats: Stats | None
) -> bytes:
    """序列化单个桶，字段顺序与 BucketOut 一致。"""
    if stats is None or stats.sample_count == 0:
        payload = {
            "bucket_index": bucket_index,
            "start_ts": b_start,
            "end_ts": b_end,
            "sample_count": 0,
            "missing": True,
            "points": [],
        }
    else:
        payload = {
            "bucket_index": bucket_index,
            "start_ts": b_start,
            "end_ts": b_end,
            "sample_count": stats.sample_count,
            "missing": False,
            "points": _points_from_stats(stats),
        }
    return json.dumps(payload, **_JSON_KWARGS).encode("utf-8")


def prepare_query_stream(
    conn: sqlite3.Connection,
    test_id: int,
    start_ts: int,
    end_ts: int,
    bucket_width_ms: int,
) -> QueryStream:
    """从 start_ts 起按固定桶宽分桶（左含右不含），末桶截到 end_ts。

    单趟完成全部桶的取数与序列化：桶片段写入容量受限的缓冲文件，
    内存中不保留完整响应。取数沿用多级摘要策略（完整落在桶内的
    最粗块直接采用，仅桶边界切开最细块时回读原始采样）。
    """
    test = conn.execute("SELECT * FROM tests WHERE id = ?", (test_id,)).fetchone()
    if test is None:
        raise KeyError(f"试验 {test_id} 不存在")

    span = end_ts - start_ts
    bucket_count = (span + bucket_width_ms - 1) // bucket_width_ms
    if bucket_count > config.MAX_BUCKETS:
        raise ValueError(
            f"桶数 {bucket_count} 超过上限 {config.MAX_BUCKETS}，请加大桶宽或缩小时间范围"
        )

    metrics = {"blocks_used": 0, "blocks_by_level": {}, "raw_rows_read": 0}
    missing_count = 0
    # 各层最大块跨度：给逐桶块检索加 start_ts 下界，避免扫描量随桶数平方增长
    spans = level_time_spans(conn, test_id)
    # 桶片段缓冲：内存中最多 STREAM_SPOOL_BYTES，超出自动滚动到临时文件
    spool = tempfile.SpooledTemporaryFile(
        max_size=config.STREAM_SPOOL_BYTES, mode="w+b"
    )
    try:
        separator = b""
        for i in range(bucket_count):
            b_start = start_ts + i * bucket_width_ms
            b_end = min(b_start + bucket_width_ms, end_ts)  # 末桶截到终点
            stats = range_summary(
                conn,
                test_id,
                b_start,
                b_end,
                test["max_level"],
                config.BLOCK_SIZE,
                metrics,
                spans,
            )
            if stats is None or stats.sample_count == 0:
                missing_count += 1
            spool.write(separator)
            spool.write(_bucket_fragment(i, b_start, b_end, stats))
            separator = b","

        # 头部字段顺序与 QueryResponse 一致；metrics 之后才是 buckets 数组
        header_obj = {
            "test_id": test_id,
            "test_name": test["name"],
            "start_ts": start_ts,
            "end_ts": end_ts,
            "bucket_width_ms": bucket_width_ms,
            "bucket_count": bucket_count,
            "missing_bucket_count": missing_count,
            "metrics": metrics,
        }
        # 去掉末尾的 "}"，续上 "buckets":[ 与片段文件、尾部 "]}" 拼成完整响应
        header = (
            json.dumps(header_obj, **_JSON_KWARGS)[:-1] + ',"buckets":['
        ).encode("utf-8")
        trailer = b"]}"
        body_size = spool.seek(0, 2)  # 定位到末尾取大小
        spool.seek(0)
        return QueryStream(
            header=header,
            body=spool,
            trailer=trailer,
            content_length=len(header) + body_size + len(trailer),
        )
    except Exception:
        spool.close()
        raise
