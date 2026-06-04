#!/bin/sh
set -eu

docker compose exec mongo mongosh orders --quiet --eval '
print("orders.orders");
db.orders.find().sort({_id: -1}).limit(5).forEach(printjson);
print("orders.outbox_events");
db.outbox_events.find().sort({_id: -1}).limit(5).forEach(printjson);
'

docker compose exec mongo mongosh fulfillment --quiet --eval '
print("fulfillment.processed_events");
db.processed_events.find().sort({_id: -1}).limit(5).forEach(printjson);
print("fulfillment.fulfillments");
db.fulfillments.find().sort({_id: -1}).limit(5).forEach(printjson);
'
