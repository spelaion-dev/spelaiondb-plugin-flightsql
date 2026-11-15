#!/usr/bin/env bash
# Starts what the tests need and waits until it answers: CI runs this before the tests
# (the shared workflow looks for it), and so can you.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f testing/compose.yaml up -d --wait
# Dremio starts with no user: make the tests' one (an error when it is there already).
curl -s -X PUT http://127.0.0.1:19047/apiv2/bootstrap/firstuser \
  -H 'Content-Type: application/json' -H 'Authorization: _dremionull' \
  -d '{"userName":"spelaion","firstName":"Spelaion","lastName":"Test","email":"test@example.invalid","createdAt":1700000000000,"password":"spelaion123"}' \
  > /dev/null || true
