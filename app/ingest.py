"""记录仪 CSV 导入与校验。

要求：
- 首行表头，需能识别出时间戳列与压力列（支持中英文表头，见 config）
- 毫秒时间戳为整数且严格递增（重复或倒序即拒绝整次导入）
- 压力为有限浮点数，单位 kPa

解析与写入全程流式进行：逐行校验、按批入库，内存占用与文件大小无关，
长记录（百万级采样）也能在 256 MiB 容器内导入。
"""

from __future__ import annotations

import csv
import io
import math
import sqlite3
from typing import Iterator

from . import config
from .summaries import build_summaries


class CsvValidationError(ValueError):
    """CSV 内容不满足导入契约。"""


def _resolve_columns(header: list[str]) -> tuple[int, int]:
    ts_idx = p_idx = -1
    for i, name in enumerate(header):
        key = name.strip().lower()
        if ts_idx == -1 and key in config.TIMESTAMP_COLUMNS:
            ts_idx = i
        if p_idx == -1 and key in config.PRESSURE_COLUMNS:
            p_idx = i
    missing = []
    if ts_idx == -1:
        missing.append("时间戳列")
    if p_idx == -1:
        missing.append("压力列")
    if missing:
        raise CsvValidationError(
            "表头缺少 " + "、".join(missing) + f"；实际表头：{header}"
        )
    return ts_idx, p_idx


def _parse_int_ms(raw: str, line_no: int) -> int:
    raw = raw.strip()
    if not raw:
        raise CsvValidationError(f"第 {line_no} 行时间戳为空")
    try:
        # 拒绝 1.5、1.0 这类非整数毫秒时间戳
        value = int(raw)
    except ValueError:
        raise CsvValidationError(
            f"第 {line_no} 行时间戳不是整数毫秒：{raw!r}"
        ) from None
    return value


def _parse_pressure(raw: str, line_no: int) -> float:
    raw = raw.strip()
    if not raw:
        raise CsvValidationError(f"第 {line_no} 行压力为空")
    try:
        value = float(raw)
    except ValueError:
        raise CsvValidationError(f"第 {line_no} 行压力不是数值：{raw!r}") from None
    if not math.isfinite(value):
        raise CsvValidationError(f"第 {line_no} 行压力不是有限值：{raw!r}")
    return value


def _iter_rows(text_stream: io.TextIOBase) -> Iterator[tuple[int, float]]:
    """逐行解析并严格校验，yield (ts_ms, pressure_kpa)。"""
    reader = csv.reader(text_stream)
    try:
        header = next(reader)
    except StopIteration:
        raise CsvValidationError("CSV 为空，缺少表头") from None
    if not header or not any(cell.strip() for cell in header):
        raise CsvValidationError("CSV 表头为空")

    ts_idx, p_idx = _resolve_columns(header)
    width = len(header)

    prev_ts: int | None = None
    line_no = 1
    for row in reader:
        line_no += 1
        if not row or not any(cell.strip() for cell in row):
            continue  # 容忍空行
        if len(row) < width:
            raise CsvValidationError(
                f"第 {line_no} 行列数 {len(row)} 少于表头列数 {width}"
            )
        ts = _parse_int_ms(row[ts_idx], line_no)
        pressure = _parse_pressure(row[p_idx], line_no)
        if prev_ts is not None and ts <= prev_ts:
            raise CsvValidationError(
                f"第 {line_no} 行时间戳 {ts} 未严格递增（上一行为 {prev_ts}）"
            )
        prev_ts = ts
        yield ts, pressure


def iter_csv_samples(content: bytes) -> Iterator[tuple[int, float]]:
    """流式解析 CSV，逐条 yield (ts_ms, pressure_kpa)，校验规则与 parse_csv 相同。"""
    try:
        # 部分记录仪在 Windows 下导出 GBK；先快速探测编码，再流式解码
        content.decode("utf-8-sig")
        encoding = "utf-8-sig"
    except UnicodeDecodeError:
        encoding = "gbk"
    stream = io.TextIOWrapper(io.BytesIO(content), encoding=encoding, newline="")
    return _iter_rows(stream)


def parse_csv(content: bytes) -> list[tuple[int, float]]:
    """解析并严格校验 CSV，返回 (ts_ms, pressure_kpa) 序列。"""
    samples = list(iter_csv_samples(content))
    if not samples:
        raise CsvValidationError("CSV 没有任何有效采样行")
    return samples


def import_test(
    conn: sqlite3.Connection,
    name: str,
    filename: str,
    content: bytes,
    block_size: int | None = None,
) -> dict:
    """完整导入一次试验：采样入库 + 多级摘要，单事务完成。

    采样逐行解析、按 IMPORT_BATCH_SIZE 分批写入，任何校验错误都会
    触发回滚，不留半成品。
    """
    block_size = block_size or config.BLOCK_SIZE

    try:
        cur = conn.execute(
            "INSERT INTO tests (name, source_filename, sample_count, start_ts, end_ts, max_level) "
            "VALUES (?, ?, 0, 0, 0, 0)",
            (name, filename),
        )
    except sqlite3.IntegrityError as exc:
        raise CsvValidationError(f"试验名称 {name!r} 已存在") from exc
    test_id = cur.lastrowid

    insert_sql = "INSERT INTO samples (test_id, ts, pressure) VALUES (?, ?, ?)"
    batch: list[tuple] = []
    sample_count = 0
    first_ts: int | None = None
    last_ts: int | None = None
    for ts, pressure in iter_csv_samples(content):
        if first_ts is None:
            first_ts = ts
        last_ts = ts
        batch.append((test_id, ts, pressure))
        sample_count += 1
        if len(batch) >= config.IMPORT_BATCH_SIZE:
            conn.executemany(insert_sql, batch)
            batch.clear()
    if batch:
        conn.executemany(insert_sql, batch)
    if sample_count == 0:
        raise CsvValidationError("CSV 没有任何有效采样行")

    max_level = build_summaries(conn, test_id, block_size)
    conn.execute(
        "UPDATE tests SET sample_count = ?, start_ts = ?, end_ts = ?, max_level = ? "
        "WHERE id = ?",
        (sample_count, first_ts, last_ts, max_level, test_id),
    )

    block_counts = dict(
        conn.execute(
            "SELECT level, COUNT(*) FROM blocks WHERE test_id = ? GROUP BY level",
            (test_id,),
        ).fetchall()
    )
    imported_at = conn.execute(
        "SELECT imported_at FROM tests WHERE id = ?", (test_id,)
    ).fetchone()[0]
    return {
        "id": test_id,
        "name": name,
        "source_filename": filename,
        "sample_count": sample_count,
        "start_ts": first_ts,
        "end_ts": last_ts,
        "max_level": max_level,
        "imported_at": imported_at,
        "block_counts": {str(k): v for k, v in sorted(block_counts.items())},
    }
