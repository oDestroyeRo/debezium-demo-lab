#!/bin/sh
set -eu

curl -sS http://localhost:18083/connectors/mongodb-outbox/status
printf "\n"
