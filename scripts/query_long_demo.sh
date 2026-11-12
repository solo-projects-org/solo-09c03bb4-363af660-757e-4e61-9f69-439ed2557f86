#!/usr/bin/env bash
# 长记录端到端演示：生成 100 万点 CSV -> 导入 -> 一次查询 10 万个非空桶。
#
# 前提：服务已启动（docker compose up，容器内存上限 256 MiB）。
# 用法：
#   scripts/query_long_demo.sh                # 完整跑一遍
#   BASE_URL=http://localhost:8000 TEST_NAME=long-001 scripts/query_long_demo.sh
#
# 环境变量：
#   BASE_URL   服务地址（默认 http://localhost:8000）
#   TEST_NAME  试验名称（默认 long-001；已存在则跳过导入，直接查询）
#   OUT_JSON   查询结果保存路径（默认 /tmp/buckets_100k.json）
set -euo pipefail
cd "$(dirname "$0")/.."

BASE_URL="${BASE_URL:-http://localhost:8000}"
TEST_NAME="${TEST_NAME:-long-001}"
OUT_JSON="${OUT_JSON:-/tmp/buckets_100k.json}"
CSV="sample_data/pump_start_long.csv"
BASE_TS=1731300000000
SPAN_MS=1000000          # 100 万点 × 1 ms
BUCKET_WIDTH_MS=10       # 10 ms 桶宽 -> 恰好 100 000 个非空桶

# 1. 长记录不存在则生成（约 21 MB）
if [ ! -f "$CSV" ]; then
    echo "==> 生成长记录 $CSV"
    python3 scripts/generate_sample.py --long
fi

# 2. 导入（已入库的同名试验直接复用，不重导）
TEST_ID="$(python3 - "$BASE_URL" "$TEST_NAME" <<'PY'
import json, sys, urllib.request
base, name = sys.argv[1], sys.argv[2]
with urllib.request.urlopen(base + "/tests") as r:
    tests = json.load(r)
print(next((t["id"] for t in tests if t["name"] == name), ""))
PY
)"

if [ -z "$TEST_ID" ]; then
    echo "==> 导入 $CSV 为试验 $TEST_NAME（约 100 万行，需几十秒）"
    curl -sf -X POST "$BASE_URL/tests/import" \
        -F "name=$TEST_NAME" \
        -F "file=@$CSV;type=text/csv" | python3 -m json.tool
else
    echo "==> 试验 $TEST_NAME 已入库（id=$TEST_ID），跳过导入"
fi

# 已入库时取一次 id（导入分支的响应里也有，但统一再查一次最稳）
if [ -z "$TEST_ID" ]; then
    TEST_ID="$(python3 - "$BASE_URL" "$TEST_NAME" <<'PY'
import json, sys, urllib.request
base, name = sys.argv[1], sys.argv[2]
with urllib.request.urlopen(base + "/tests") as r:
    tests = json.load(r)
print(next(t["id"] for t in tests if t["name"] == name))
PY
)"
fi

# 3. 一次查询整段试验：100 000 个非空桶，每桶首/尾/最高/最低点
END_TS=$((BASE_TS + SPAN_MS))
echo "==> 查询 10 万个桶：GET $BASE_URL/tests/$TEST_ID/query"
echo "    start_ts=$BASE_TS end_ts=$END_TS bucket_width_ms=$BUCKET_WIDTH_MS"
time curl -sf \
    "$BASE_URL/tests/$TEST_ID/query?start_ts=$BASE_TS&end_ts=$END_TS&bucket_width_ms=$BUCKET_WIDTH_MS" \
    -o "$OUT_JSON"

# 4. 汇总：桶数、缺测数、读取统计、四点各不相同的桶占比
python3 - "$OUT_JSON" <<'PY'
import json, sys
with open(sys.argv[1], "rb") as f:
    body = json.load(f)
distinct4 = sum(
    1 for b in body["buckets"]
    if len({r for p in b["points"] for r in p["roles"]}) == 4
    and len(b["points"]) == 4
)
print(f"响应已保存到 {sys.argv[1]}")
print(f"  bucket_count         = {body['bucket_count']}")
print(f"  missing_bucket_count = {body['missing_bucket_count']}")
print(f"  metrics              = {body['metrics']}")
print(f"  四点各不相同的桶     = {distinct4} / {body['bucket_count']}")
PY
