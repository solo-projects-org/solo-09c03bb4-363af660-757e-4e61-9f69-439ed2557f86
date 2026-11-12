#!/usr/bin/env python3
"""生成启泵试验样例 CSV（合成数据，模拟记录仪导出格式）。

运行：
    python3 scripts/generate_sample.py          # 生成两个短样例
    python3 scripts/generate_sample.py --long   # 额外生成 100 万点长记录

输出：
    sample_data/pump_start_demo.csv   2 万点、含启泵水锤振荡与尖峰深谷
    sample_data/small_demo.csv        60 点小样本，便于在 Swagger 里肉眼核对
    sample_data/pump_start_long.csv   100 万点、1 ms 间隔长记录（--long，
                                      用于 10 万桶流式查询演示；文件较大，
                                      已加入 .gitignore）
列：timestamp_ms,pressure_kpa；毫秒时间戳严格递增。
"""

from __future__ import annotations

import csv
import math
import os
import sys
from pathlib import Path

BASE_TS = 1_731_300_000_000  # 固定基准，保证样例可复现
OUT_DIR = Path(__file__).resolve().parent.parent / "sample_data"

# 长记录：1 ms 采样、共 100 万点（约 16.7 分钟）；
# 按 10 ms 桶宽查询整段即得 100 000 个非空桶
LONG_COUNT = 1_000_000
LONG_INTERVAL_MS = 1


def pressure_at(t_ms: int) -> float:
    """模拟启泵过程：静压 -> 启泵爬升 -> 水锤衰减振荡，含一次低压深谷。"""
    t = t_ms
    if t < 2000:
        # 启泵前管网静压，微小波动
        p = 100.0 + 1.5 * math.sin(2 * math.pi * 0.5 * t / 1000.0)
    elif t < 6000:
        # 启泵后近似线性爬升
        p = 100.0 + 220.0 * (t - 2000) / 4000.0
    else:
        dt = t - 6000
        steady = 320.0
        oscillation = 60.0 * math.exp(-dt / 2500.0) * math.sin(
            2 * math.pi * 2.0 * dt / 1000.0
        )
        # 启泵瞬间约 50ms 处的一次低压水锤深谷
        dip = -45.0 * math.exp(-((dt - 50) / 25.0) ** 2)
        p = steady + oscillation + dip
    return round(p, 3)


def pressure_at_long(i: int) -> float:
    """长记录用压力：基础曲线 + 低频涟漪 + 确定性抖动。

    抖动只由采样序号决定（可复现），幅值盖过局部趋势，使每个 10 ms 桶的
    首点、尾点、最高点、最低点几乎总是四个不同的点——对应流式查询
    要处理的最重响应形态。
    """
    t_ms = i * LONG_INTERVAL_MS
    ripple = 2.0 * math.sin(2 * math.pi * 0.9 * t_ms / 1000.0)
    jitter = (((i * 2654435761) ^ (i >> 7)) % 2000) / 1000.0 - 1.0
    return round(pressure_at(t_ms) + ripple + 1.5 * jitter, 3)


def write_csv(path: Path, count: int, interval_ms: int, pressure_fn) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "pressure_kpa"])
        for i in range(count):
            ts = BASE_TS + i * interval_ms
            writer.writerow([ts, pressure_fn(i * interval_ms)])
    print(f"wrote {path} ({count} rows)")


def write_long(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "pressure_kpa"])
        for i in range(LONG_COUNT):
            writer.writerow([BASE_TS + i * LONG_INTERVAL_MS, pressure_at_long(i)])
    print(f"wrote {path} ({LONG_COUNT} rows)")


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    if "--long" in sys.argv:
        write_long(OUT_DIR / "pump_start_long.csv")
    else:
        write_csv(OUT_DIR / "pump_start_demo.csv", 20_000, 10, pressure_at)
        write_csv(OUT_DIR / "small_demo.csv", 60, 100, pressure_at)


if __name__ == "__main__":
    main()
