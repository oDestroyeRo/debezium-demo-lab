#!/bin/sh
set -eu

checkout_id="${1:-checkout-$(date +%s)}"

curl -sS -X POST "http://localhost:18080/checkouts/${checkout_id}/complete" \
  -H "Content-Type: application/json" \
  -d '{"shop_id":"shop_77","total_amount":2490}'

printf "\n"
