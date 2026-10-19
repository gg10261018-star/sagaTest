#!/usr/bin/env bash
# 現場 Demo：正常兌換 → 重送 → 製造故障 → 觀察背景補償 → 查流水
set -euo pipefail
BASE=${BASE:-http://localhost:8080}
H='Content-Type: application/json'
RID=$(date +%s)

step() { printf '\n\033[1;36m▶ %s\033[0m\n' "$1"; }

step "1. 正常兌換（扣 100 點、扣 1 庫存、發道具）"
curl -s -X POST "$BASE/redeem" -H "$H" -d "{\"request_id\":\"ok-$RID\",\"user_id\":1001,\"item_id\":1,\"cost\":100}"

step "2. 同一個 request_id 重送 → 不會重複扣點"
curl -s -X POST "$BASE/redeem" -H "$H" -d "{\"request_id\":\"ok-$RID\",\"user_id\":1001,\"item_id\":1,\"cost\":100}"
curl -s "$BASE/users/1001/points"

step "3. 製造故障：發道具永遠失敗、退點的補償前 2 次失敗"
curl -s -X POST "$BASE/admin/chaos" -H "$H" -d '{"step":"grant_item","exec_failures":-1}' >/dev/null
curl -s -X POST "$BASE/admin/chaos" -H "$H" -d '{"step":"deduct_points","comp_failures":2}' >/dev/null
curl -s -X POST "$BASE/redeem" -H "$H" -d "{\"request_id\":\"fail-$RID\",\"user_id\":1001,\"item_id\":1,\"cost\":100}"

step "4. 等背景 worker 以指數退避重試補償"
for i in $(seq 1 15); do
  status=$(curl -s "$BASE/sagas/fail-$RID" | grep -o '"status": "[a-z_]*"' | head -1)
  echo "  第 ${i} 秒：$status"
  [[ "$status" == *compensated* ]] && break
  sleep 1
done

step "5. 點數流水：扣 100 → 退 100"
curl -s "$BASE/sagas/fail-$RID/ledger"

step "6. 查證每一步的真實狀態"
curl -s "$BASE/sagas/fail-$RID/probe"

step "7. 恢復正常"
curl -s -X POST "$BASE/admin/chaos" -H "$H" -d '{"step":"grant_item","exec_failures":0}' >/dev/null
curl -s "$BASE/users/1001/points"
curl -s "$BASE/items/1/stock"
