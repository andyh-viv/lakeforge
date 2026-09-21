#!/usr/bin/env bash
# UC + Lakebase smoke against a running local lakeforge-api (admin creds).
set -u
cd "$(dirname "$0")"
source ./lib.sh
pass=0; fail=0
check() { # check NAME CMD_OUTPUT EXPECTED_SUBSTRING
  if echo "$2" | grep -q -- "$3"; then pass=$((pass+1)); echo "ok   $1"; else fail=$((fail+1)); echo "FAIL $1"; echo "     got: $(echo "$2" | head -c 400)"; fi
}

# --- cluster (needed for SQL)
if [ -z "$CID" ]; then
  CID=$(api POST /api/2.0/clusters/create '{"cluster_name":"uc","spark_version":"forge","node_type_id":"local","num_workers":1}' | python3 -c "import sys,json;print(json.load(sys.stdin)['cluster_id'])")
fi
for _ in $(seq 1 60); do
  st=$(api GET "/api/2.0/clusters/get?cluster_id=$CID" | python3 -c "import sys,json;print(json.load(sys.stdin)['state'])")
  [ "$st" = RUNNING ] && break; sleep 1
done
echo "cluster $CID $st"
[ -z "$WH" ] && WH=$(api POST /api/2.0/sql/warehouses '{"name":"wh","cluster_size":"Small"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")

# --- basic SQL through the guarded path
check "create schema" "$(sql 'CREATE SCHEMA IF NOT EXISTS main.uc_smoke')" '"SUCCEEDED"'
sql 'DROP TABLE IF EXISTS main.uc_smoke.users' >/dev/null
check "create table" "$(sql 'CREATE TABLE main.uc_smoke.users (id INT, email STRING, region STRING)')" '"SUCCEEDED"'
check "insert" "$(sql "INSERT INTO main.uc_smoke.users VALUES (1,'a@x.com','EU'),(2,'b@y.com','US')")" '"SUCCEEDED"'
check "select" "$(sql 'SELECT count(*) FROM main.uc_smoke.users')" '"2"'
check "session fn" "$(sql 'SELECT current_user() AS u, current_catalog() AS c')" 'admin@lakeforge.local'
check "uc table mirrored" "$(api GET /api/2.1/unity-catalog/tables/main.uc_smoke.users)" '"full_name":"main.uc_smoke.users"'

# --- grants / effective permissions
api POST /api/2.0/preview/scim/v2/Users '{"userName":"bob@x.com","password":"bobpass1"}' >/dev/null
check "grant" "$(api PATCH /api/2.1/unity-catalog/permissions/table/main.uc_smoke.users '{"changes":[{"principal":"users","add":["SELECT"]}]}')" 'SELECT'
check "effective perms" "$(api GET /api/2.1/unity-catalog/effective-permissions/table/main.uc_smoke.users)" 'privilege_assignments'
check "sql show grants" "$(sql 'SHOW GRANTS ON TABLE main.uc_smoke.users')" '"users","SELECT","TABLE","main.uc_smoke.users"'
check "sql grant" "$(sql 'GRANT MODIFY, USE SCHEMA ON SCHEMA main.uc_smoke TO `bob@x.com`')" '"SUCCEEDED"'
check "sql grant visible via REST" "$(api GET /api/2.1/unity-catalog/permissions/schema/main.uc_smoke)" 'USE_SCHEMA'
check "sql show grants inherited" "$(sql 'SHOW GRANTS `bob@x.com` ON TABLE main.uc_smoke.users')" '"MODIFY","SCHEMA","main.uc_smoke"'
check "sql revoke" "$(sql 'REVOKE MODIFY ON SCHEMA main.uc_smoke FROM `bob@x.com`')" '"SUCCEEDED"'
check "sql revoke applied" "$(api GET /api/2.1/unity-catalog/permissions/schema/main.uc_smoke | grep -c MODIFY)" '^0$'
check "sql alter owner" "$(sql 'ALTER TABLE main.uc_smoke.users OWNER TO `admin@lakeforge.local`')" '"SUCCEEDED"'
check "sql grant bad privilege rejected" "$(sql 'GRANT FLY ON TABLE main.uc_smoke.users TO bob')" 'PARSE_SYNTAX_ERROR'

# --- policies
check "create mask fn" "$(sql "CREATE OR REPLACE FUNCTION main.uc_smoke.mask_email(e STRING) RETURNS STRING RETURN CASE WHEN is_account_group_member('admins') THEN e ELSE '***' END")" '"SUCCEEDED"'
check "set column mask" "$(api PUT /api/2.0/lakeforge/unity-catalog/tables/main.uc_smoke.users/column-masks '{"column":"email","function_name":"main.uc_smoke.mask_email"}')" 'mask'
check "masked select (admin sees clear)" "$(sql 'SELECT email FROM main.uc_smoke.users ORDER BY id LIMIT 1')" 'a@x.com'

# --- tags / comments / constraints
check "tag table" "$(api POST /api/2.1/unity-catalog/entity-tag-assignments '{"entity_type":"tables","entity_name":"main.uc_smoke.users","tag_key":"pii","tag_value":"true"}')" 'pii'
check "constraint" "$(api POST /api/2.1/unity-catalog/constraints '{"full_name_arg":"main.uc_smoke.users","constraint":{"primary_key_constraint":{"name":"pk_users","child_columns":["id"]}}}')" 'pk_users'

# --- lineage / audit / system tables
check "table lineage" "$(api GET '/api/2.0/lineage-tracking/table-lineage?table_name=main.uc_smoke.users')" 'upstreams\|downstreams'
check "audit events" "$(api GET '/api/2.0/lakeforge/audit?limit=5')" 'events'
check "system schemas" "$(api GET /api/2.1/unity-catalog/metastores/lakeforge-metastore/systemschemas)" 'information_schema'
check "information_schema.tables" "$(sql "SELECT table_name FROM main.information_schema.tables WHERE table_schema = 'uc_smoke'")" 'users'
check "system.access.audit" "$(sql 'SELECT count(*) AS n FROM system.access.audit')" '"SUCCEEDED"'
check "system.query.history" "$(sql 'SELECT count(*) AS n FROM system.query.history')" '"SUCCEEDED"'
check "temp table credential" "$(api POST /api/2.0/unity-catalog/temporary-table-credentials "{\"table_id\":\"$(api GET /api/2.1/unity-catalog/tables/main.uc_smoke.users | python3 -c 'import sys,json;print(json.load(sys.stdin)["table_id"])')\",\"operation\":\"READ\"}")" 'expiration_time'

# --- lakebase
check "lb backend" "$(api GET /api/2.0/lakeforge/lakebase/backend)" '"emulated":true'
check "lb create instance" "$(api POST /api/2.0/database/instances '{"name":"lb-smoke","capacity":"CU_2"}')" '"state":"STARTING"'
check "lb bad capacity" "$(api POST /api/2.0/database/instances '{"name":"lb-bad","capacity":"CU_3"}')" 'capacity must be'
sleep 4
check "lb available" "$(api GET /api/2.0/database/instances/lb-smoke)" '"state":"AVAILABLE"'
UID_=$(api GET /api/2.0/database/instances/lb-smoke | python3 -c 'import sys,json;print(json.load(sys.stdin)["uid"])')
check "lb findByUid" "$(api GET "/api/2.0/database/instances:findByUid?uid=$UID_")" '"name":"lb-smoke"'
check "lb role" "$(api POST /api/2.0/database/instances/lb-smoke/roles '{"name":"admin@lakeforge.local","identity_type":"USER"}')" 'DATABRICKS_SUPERUSER'
check "lb credential" "$(api POST /api/2.0/database/credentials '{"instance_names":["lb-smoke"],"request_id":"r1"}')" 'expiration_time'
check "lb catalog" "$(api POST /api/2.0/database/catalogs '{"name":"lbcat","database_instance_name":"lb-smoke","database_name":"appdb"}')" '"database_instance_name":"lb-smoke"'
check "lb catalog in UC" "$(api GET /api/2.1/unity-catalog/catalogs/lbcat)" 'CATALOG_DATABASE'
api DELETE /api/2.0/database/synced_tables/lbcat.default.users >/dev/null
check "lb synced table" "$(api POST /api/2.0/database/synced_tables '{"name":"lbcat.default.users","spec":{"source_table_full_name":"main.uc_smoke.users","primary_key_columns":["id"],"scheduling_policy":"TRIGGERED"}}')" 'PROVISIONING'
check "lb synced bad pk" "$(api POST /api/2.0/database/synced_tables '{"name":"lbcat.default.users2","spec":{"source_table_full_name":"main.uc_smoke.users","primary_key_columns":["nope"]}}')" 'not found'
sleep 4
check "lb synced online" "$(api GET /api/2.0/database/synced_tables/lbcat.default.users)" 'ONLINE_TRIGGERED_UPDATE'
check "lb patch" "$(api PATCH '/api/2.0/database/instances/lb-smoke?update_mask=stopped' '{"stopped":true}')" '"state":"UPDATING"'
api PUT /api/2.1/unity-catalog/metastores/lakeforge-metastore/systemschemas/lakebase >/dev/null
check "system.lakebase.instances" "$(sql 'SELECT name, state FROM system.lakebase.instances')" 'lb-smoke'
check "lb delete blocked" "$(api DELETE /api/2.0/database/instances/lb-smoke)" 'force=true'
check "lb delete force" "$(api DELETE '/api/2.0/database/instances/lb-smoke?force=true&purge=true')" '{}'
check "lb gone" "$(api GET /api/2.0/database/instances/lb-smoke)" 'RESOURCE_DOES_NOT_EXIST'

# --- non-admin enforcement
BT=$(curl -s -X POST $H/api/2.0/lakeforge/login -H 'content-type: application/json' -d '{"username":"bob@x.com","password":"bobpass1"}' | python3 -c "import sys,json;print(json.load(sys.stdin).get('access_token',''))")
bsql() { curl -s -X POST $H/api/2.0/sql/statements -H "Authorization: Bearer $BT" -H 'content-type: application/json' -d "{\"warehouse_id\":\"$WH\",\"statement\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1"),\"wait_timeout\":\"30s\"}"; echo; }
sql 'CREATE TABLE IF NOT EXISTS main.uc_smoke.secret (id INT)' >/dev/null
check "bob select denied (no SELECT)" "$(bsql 'SELECT * FROM main.uc_smoke.secret')" 'PERMISSION_DENIED\|INSUFFICIENT_PERMISSIONS'
check "bob select granted table" "$(bsql 'SELECT id FROM main.uc_smoke.users ORDER BY id LIMIT 1')" '"data_array":\[\["1"\]\]'
api PATCH /api/2.1/unity-catalog/permissions/catalog/main '{"changes":[{"principal":"users","add":["USE_CATALOG"]}]}' >/dev/null
api PATCH /api/2.1/unity-catalog/permissions/schema/main.uc_smoke '{"changes":[{"principal":"users","add":["USE_SCHEMA"]}]}' >/dev/null
api PATCH /api/2.1/unity-catalog/permissions/function/main.uc_smoke.mask_email '{"changes":[{"principal":"users","add":["EXECUTE"]}]}' >/dev/null
check "bob select masked" "$(bsql 'SELECT email FROM main.uc_smoke.users ORDER BY id LIMIT 1')" '\*\*\*'
check "bob insert denied" "$(bsql "INSERT INTO main.uc_smoke.users VALUES (3,'c@z.com','EU')")" 'PERMISSION_DENIED\|INSUFFICIENT_PERMISSIONS'
check "bob write system denied" "$(bsql "INSERT INTO system.access.audit SELECT * FROM system.access.audit")" 'PERMISSION_DENIED\|INSUFFICIENT_PERMISSIONS\|read-only'

echo "passed=$pass failed=$fail"
[ $fail -eq 0 ]
