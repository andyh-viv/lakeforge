#!/usr/bin/env bash
# Usage: source lf.sh; then use $H, $A, WH, CID and helper `api METHOD PATH [JSON]`.
H=${LF_URL:-http://localhost:8080}
TOK=${LF_TOKEN_FILE:-/tmp/lakeforge.tok}
if ! curl -sf "$H/api/2.0/sql/warehouses" -H "Authorization: Bearer $(cat $TOK 2>/dev/null)" >/dev/null 2>&1; then
  curl -s -X POST $H/api/2.0/lakeforge/login -H 'content-type: application/json' \
    -d '{"username":"admin@lakeforge.local","password":"admin"}' |
    python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])" > $TOK
fi
A="Authorization: Bearer $(cat $TOK)"
api() { # api METHOD PATH [JSON]
  if [ $# -ge 3 ]; then curl -s -X "$1" "$H$2" -H "$A" -H 'content-type: application/json' -d "$3"; else curl -s -X "$1" "$H$2" -H "$A"; fi; echo; }
WH=$(api GET /api/2.0/sql/warehouses | python3 -c "import sys,json;d=json.load(sys.stdin)['warehouses'];print(d[0]['id'] if d else '')")
CID=$(api GET /api/2.0/clusters/list | python3 -c "import sys,json;d=[c for c in json.load(sys.stdin).get('clusters',[]) if c.get('state') in ('PENDING','RUNNING')];print(d[0]['cluster_id'] if d else '')")
sql() { api POST /api/2.0/sql/statements "{\"warehouse_id\":\"$WH\",\"statement\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1"),\"wait_timeout\":\"30s\"}"; }
