#!/usr/bin/env bash
# 缺件（缺口）同步端到端演示（curl 版，配合 docker compose）：
#   先收到某站第 2 条修改、第 1 条缺件 -> 时钟不虚高、分析继续用旧计数
#   -> 缺件期间本地更正 -> 补齐缺件 -> 按原规则列冲突并从分析取数排除
#   -> 重复/乱序投递不重复入库 -> 补齐后再同步不重复传输
#
# 进程级“重启后补齐”的可运行演示见 scripts/demo_gap_restart.py（无需 Docker）。
#
# 用法：先 `docker compose up -d --build`，再 `bash scripts/demo_gap.sh`
# 本脚本只用 alpha(8001) 与 charlie(8003)；如需干净数据先 `docker compose down -v`。
set -euo pipefail

A="${ALPHA_URL:-http://localhost:8001}"   # 数据员营地（接收方）
C="${CHARLIE_URL:-http://localhost:8003}" # 对站营地（产生更正的一方）
TMPD="$(mktemp -d)"
trap 'rm -rf "$TMPD"' EXIT

j() { python3 -m json.tool; }
sf() { # $1=url $2=python 表达式（d 为 status JSON）
  curl -s "$1/status" | python3 -c "import json,sys; d=json.load(sys.stdin); print($2)";
}

echo "================ 0. 营地健康检查 ================"
curl -s "$A/healthz" | j
curl -s "$C/healthz" | j

echo
echo "================ 1. charlie 产生两条更正 ================"
echo "-- charlie-1：R001 计数 -> 70（稍后故意扣留，制造缺件）"
curl -s -X POST "$C/records/R001/corrections" -H 'Content-Type: application/json' \
  -d '{"count": 70}' | j
echo "-- charlie-2：R002 计数 -> 80（先投递这条）"
curl -s -X POST "$C/records/R002/corrections" -H 'Content-Type: application/json' \
  -d '{"count": 80}' | j

echo
echo "================ 2. 只把第 2 条投给 alpha（第 1 条缺件） ================"
curl -s "$C/sync/pull" > "$TMPD/full.json"
python3 -c '
import json
d = json.load(open("'$TMPD'/full.json"))
op2 = [o for o in d["ops"] if o["id"] == "charlie-2"]
json.dump({"ops": op2, "resolutions": []}, open("'$TMPD'/op2.json", "w"))
'
curl -s -X POST "$A/sync/batch" -H 'Content-Type: application/json' \
  --data-binary "@$TMPD/op2.json" | j

echo
echo "alpha 向量时钟（缺件，不得出现 charlie:2）：$(sf "$A" 'd["version_vv"]')"
echo "alpha 收件缺口（第 1 条未到，第 2 条先入缺口表）：$(sf "$A" 'd["inbound_gaps"]')"

echo
echo "================ 3. 缺件期间：分析继续提供旧计数，且允许本地更正 ================"
curl -s "$A/records/R001" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["record_id"], "count=", d["count"], "conflicted=", d["conflicted"], "(仍为种子旧值 32)")'
echo "-- alpha 在缺件窗口内对 R002 本地更正 -> 11"
curl -s -X POST "$A/records/R002/corrections" -H 'Content-Type: application/json' \
  -d '{"count": 11}' | j

echo
echo "================ 4. 重复投递不重复入库（charlie-2 再投一次） ================"
curl -s -X POST "$A/sync/batch" -H 'Content-Type: application/json' \
  --data-binary "@$TMPD/op2.json" | j

echo
echo "================ 5. 按收件水位补拉缺件（与 /sync/run 同一套水位逻辑） ================"
WM="$(sf "$A" 'json.dumps(d["inbound_watermarks"])')"
echo "alpha 当前收件水位：$WM"
curl -s -G "$C/sync/pull" --data-urlencode "watermarks=$WM" > "$TMPD/missing.json"
echo "charlie 按该水位返回的待补载荷："
cat "$TMPD/missing.json" | j
curl -s -X POST "$A/sync/batch" -H 'Content-Type: application/json' \
  --data-binary "@$TMPD/missing.json" | j

echo
echo "================ 6. 补齐后：冲突按原规则产生，分析取数排除 ================"
echo "alpha 缺口（应为空）：$(sf "$A" 'd["inbound_gaps"]')"
echo "alpha 向量时钟：$(sf "$A" 'd["version_vv"]')"
curl -s "$A/conflicts" | j
curl -s "$A/analysis/counts" | python3 -c '
import json, sys
rows = json.load(sys.stdin)
ids = {r["record_id"] for r in rows}
print(f"分析可用计数 {len(rows)} 条")
assert "R002" not in ids, "冲突记录必须从分析取数排除"
assert "R001" in ids, "已补齐且无冲突的 R001 应正常提供"
print("R002（本地11 vs charlie80 冲突）已排除；R001 缺件补齐后正常取数")
'

echo
echo "================ 7. 补齐后再次同步：已收更正不重复传输 ================"
WM2="$(sf "$A" 'json.dumps(d["inbound_watermarks"])')"
echo "按最新水位 $WM2 再拉（ops/resolutions 应为空）："
curl -s -G "$C/sync/pull" --data-urlencode "watermarks=$WM2" | j

echo
echo "缺件同步演示完成。进程级重启补齐演示：python3 scripts/demo_gap_restart.py"
echo "Swagger 入口：$A/docs 与 $C/docs"
