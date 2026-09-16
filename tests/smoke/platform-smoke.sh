#!/usr/bin/env bash
# End-to-end smoke test of the Lakeforge control plane REST API.
set -u
H=${LF_URL:-http://localhost:8080}
PASS=0; FAIL=0
step() { printf '\n== %s\n' "$*"; }
check() { # name, condition
  if eval "$2"; then PASS=$((PASS+1)); echo "  ok   $1"; else FAIL=$((FAIL+1)); echo "  FAIL $1"; fi
}
j() { python3 -c "import sys,json; d=json.load(sys.stdin); print(eval('d'+sys.argv[1]))" "$1"; }

step "login"
TOK=$(curl -s -X POST $H/api/2.0/lakeforge/login -H 'content-type: application/json' -d '{"username":"admin@lakeforge.local","password":"admin"}' | j "['access_token']")
check "jwt issued" '[ -n "$TOK" ]'
A="Authorization: Bearer $TOK"

step "PAT"
PAT=$(curl -s -X POST $H/api/2.0/token/create -H "$A" -H 'content-type: application/json' -d '{"comment":"smoke","lifetime_seconds":3600}' | j "['token_value']")
check "pat created" '[[ "$PAT" == dapi* ]]'
A="Authorization: Bearer $PAT"
ME=$(curl -s $H/api/2.0/preview/scim/v2/Me -H "$A" | j "['userName']")
check "scim me" '[ "$ME" = admin@lakeforge.local ]'

step "workspace + notebook"
curl -s -X POST $H/api/2.0/workspace/mkdirs -H "$A" -d '{"path":"/Users/admin@lakeforge.local/smoke"}' >/dev/null
SRC=$(printf '# Databricks notebook source\nprint("hello from nb")\nx = 21 * 2\ndisplay(spark.sql("select 1 as a, 2 as b"))\n# COMMAND ----------\ndbutils.notebook.exit(str(x))\n' | base64 -w0)
R=$(curl -s -X POST $H/api/2.0/workspace/import -H "$A" -H 'content-type: application/json' -d "{\"path\":\"/Users/admin@lakeforge.local/smoke/nb\",\"format\":\"SOURCE\",\"language\":\"PYTHON\",\"content\":\"$SRC\",\"overwrite\":true}")
check "notebook import" '[ "$R" = "{}" ]'
N=$(curl -s "$H/api/2.0/workspace/list?path=/Users/admin@lakeforge.local/smoke" -H "$A" | j "['objects'][0]['object_type']")
check "workspace list" '[ "$N" = NOTEBOOK ]'
EXP=$(curl -s "$H/api/2.0/workspace/export?path=/Users/admin@lakeforge.local/smoke/nb&format=SOURCE" -H "$A" | j "['content']" | base64 -d)
check "notebook export roundtrip" 'echo "$EXP" | grep -q "hello from nb"'

step "dbfs"
curl -s -X POST $H/api/2.0/dbfs/put -H "$A" -H 'content-type: application/json' -d "{\"path\":\"dbfs:/tmp/smoke/hello.txt\",\"contents\":\"$(echo -n 'hello dbfs' | base64)\",\"overwrite\":true}" >/dev/null
RD=$(curl -s "$H/api/2.0/dbfs/read?path=/tmp/smoke/hello.txt" -H "$A" | j "['data']" | base64 -d)
check "dbfs put/read" '[ "$RD" = "hello dbfs" ]'
LS=$(curl -s "$H/api/2.0/dbfs/list?path=/tmp/smoke" -H "$A" | j "['files'][0]['path']")
check "dbfs list" '[ "$LS" = /tmp/smoke/hello.txt ]'
curl -s -X PUT "$H/api/2.0/fs/files/tmp/smoke/files-api.txt" -H "$A" -H 'content-type: application/octet-stream' --data-binary 'via files api' >/dev/null
FR=$(curl -s "$H/api/2.0/fs/files/tmp/smoke/files-api.txt" -H "$A")
check "files api" '[ "$FR" = "via files api" ]'

step "secrets"
curl -s -X POST $H/api/2.0/secrets/scopes/create -H "$A" -d '{"scope":"smoke"}' >/dev/null
curl -s -X POST $H/api/2.0/secrets/put -H "$A" -d '{"scope":"smoke","key":"k","string_value":"s3cret"}' >/dev/null
SV=$(curl -s "$H/api/2.0/secrets/get?scope=smoke&key=k" -H "$A" | j "['value']" | base64 -d)
check "secret roundtrip (encrypted at rest)" '[ "$SV" = s3cret ]'
check "secret not stored in clear" '! grep -q s3cret .lakeforge/lakeforge.db'

step "cluster"
CID=$(curl -s -X POST $H/api/2.0/clusters/create -H "$A" -H 'content-type: application/json' -d '{"cluster_name":"smoke","spark_version":"forge","num_workers":1,"autotermination_minutes":30}' | j "['cluster_id']")
check "cluster create" '[ -n "$CID" ]'
for i in $(seq 1 60); do ST=$(curl -s "$H/api/2.0/clusters/get?cluster_id=$CID" -H "$A" | j "['state']"); [ "$ST" = RUNNING ] && break; sleep 1; done
check "cluster RUNNING ($ST after ${i}s)" '[ "$ST" = RUNNING ]'

step "sql statements"
WH=$(curl -s $H/api/2.0/sql/warehouses -H "$A" | j "['warehouses'][0]['id']")
check "default warehouse" '[ -n "$WH" ]'
R=$(curl -s -X POST $H/api/2.0/sql/statements -H "$A" -H 'content-type: application/json' -d "{\"warehouse_id\":\"$WH\",\"statement\":\"CREATE OR REPLACE TABLE main.default.smoke_t AS SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) AS t(id, name)\",\"wait_timeout\":\"30s\"}")
CS=$(echo "$R" | j "['status']['state']")
check "create table" '[ "$CS" = SUCCEEDED ]' || echo "$R"
R=$(curl -s -X POST $H/api/2.0/sql/statements -H "$A" -H 'content-type: application/json' -d "{\"warehouse_id\":\"$WH\",\"statement\":\"SELECT count(*) AS n, max(name) AS m FROM main.default.smoke_t\",\"wait_timeout\":\"30s\"}")
CN=$(echo "$R" | j "['result']['data_array'][0][0]")
check "select count" '[ "$CN" = 3 ]' || echo "$R"
T=$(curl -s "$H/api/2.1/unity-catalog/tables/main.default.smoke_t" -H "$A" | j "['name']")
check "table registered in catalog" '[ "$T" = smoke_t ]'
TS=$(curl -s "$H/api/2.1/unity-catalog/tables?catalog_name=main&schema_name=default" -H "$A" | j "['tables'][0]['full_name']")
check "list tables" '[ "$TS" = main.default.smoke_t ]'

step "command execution (1.2 API)"
CTX=$(curl -s -X POST $H/api/1.2/contexts/create -H "$A" -H 'content-type: application/json' -d "{\"clusterId\":\"$CID\",\"language\":\"python\"}" | j "['id']")
check "context created" '[ -n "$CTX" ]'
CMD=$(curl -s -X POST $H/api/1.2/commands/execute -H "$A" -H 'content-type: application/json' -d "{\"clusterId\":\"$CID\",\"contextId\":\"$CTX\",\"language\":\"python\",\"command\":\"print(6*7)\\nspark.sql('select id, name from main.default.smoke_t order by id').toPandas().to_dict('records')\"}" | j "['id']")
for i in $(seq 1 60); do R=$(curl -s "$H/api/1.2/commands/status?clusterId=$CID&contextId=$CTX&commandId=$CMD" -H "$A"); S=$(echo "$R" | j "['status']"); [ "$S" = Finished ] || [ "$S" = Error ] && break; sleep 1; done
check "command finished ($S)" '[ "$S" = Finished ]' || echo "$R"
check "command output has 42 + rows" 'echo "$R" | grep -q 42 && echo "$R" | grep -qE "name.: .a."' || echo "$R"

step "notebook run (jobs)"
JID=$(curl -s -X POST $H/api/2.1/jobs/create -H "$A" -H 'content-type: application/json' -d "{\"name\":\"smoke-job\",\"tasks\":[{\"task_key\":\"nb\",\"existing_cluster_id\":\"$CID\",\"notebook_task\":{\"notebook_path\":\"/Users/admin@lakeforge.local/smoke/nb\"}},{\"task_key\":\"sql\",\"depends_on\":[{\"task_key\":\"nb\"}],\"sql_task\":{\"warehouse_id\":\"$WH\",\"query\":{\"query_text\":\"SELECT count(*) FROM main.default.smoke_t\"}}}]}" | j "['job_id']")
check "job created" '[ -n "$JID" ]'
RID=$(curl -s -X POST $H/api/2.1/jobs/run-now -H "$A" -d "{\"job_id\":$JID}" | j "['run_id']")
for i in $(seq 1 90); do R=$(curl -s "$H/api/2.1/jobs/runs/get?run_id=$RID" -H "$A"); LC=$(echo "$R" | j "['state']['life_cycle_state']"); [ "$LC" = TERMINATED ] || [ "$LC" = INTERNAL_ERROR ] && break; sleep 1; done
RS=$(echo "$R" | j "['state'].get('result_state')")
check "job run SUCCESS ($LC/$RS after ${i}s)" '[ "$RS" = SUCCESS ]' || echo "$R" | head -c 1500
TRID=$(echo "$R" | j "['tasks'][0]['run_id']")
OUT=$(curl -s "$H/api/2.1/jobs/runs/get-output?run_id=$TRID" -H "$A")
check "notebook exit value 42" 'echo "$OUT" | grep -q "\"result\": *\"42\"\|\"result\":\"42\""' || echo "$OUT" | head -c 600

step "mlflow"
EXP=$(curl -s -X POST $H/api/2.0/mlflow/experiments/create -H "$A" -d '{"name":"/smoke-exp"}' | j "['experiment_id']")
RUN=$(curl -s -X POST $H/api/2.0/mlflow/runs/create -H "$A" -d "{\"experiment_id\":\"$EXP\",\"run_name\":\"r1\"}" | j "['run']['info']['run_id']")
curl -s -X POST $H/api/2.0/mlflow/runs/log-metric -H "$A" -d "{\"run_id\":\"$RUN\",\"key\":\"rmse\",\"value\":0.5,\"step\":1}" >/dev/null
curl -s -X POST $H/api/2.0/mlflow/runs/log-parameter -H "$A" -d "{\"run_id\":\"$RUN\",\"key\":\"alpha\",\"value\":\"0.1\"}" >/dev/null
M=$(curl -s "$H/api/2.0/mlflow/runs/get?run_id=$RUN" -H "$A" | j "['run']['data']['metrics'][0]['value']")
check "mlflow metric logged" '[ "$M" = 0.5 ]'
SR=$(curl -s -X POST $H/api/2.0/mlflow/runs/search -H "$A" -d "{\"experiment_ids\":[\"$EXP\"],\"filter\":\"metrics.rmse < 1 and params.alpha = '0.1'\"}" | j "['runs'][0]['info']['run_id']")
check "mlflow search filter" '[ "$SR" = "$RUN" ]'
curl -s -X PUT "$H/api/2.0/mlflow-artifacts/artifacts/$EXP/$RUN/artifacts/model/MLmodel" -H "$A" --data-binary 'flavors: {}' >/dev/null
curl -s -X POST $H/api/2.0/mlflow/registered-models/create -H "$A" -d '{"name":"smoke_model"}' >/dev/null
MV=$(curl -s -X POST $H/api/2.0/mlflow/model-versions/create -H "$A" -d "{\"name\":\"smoke_model\",\"source\":\"runs:/$RUN/model\",\"run_id\":\"$RUN\"}" | j "['model_version']['version']")
check "model version 1" '[ "$MV" = 1 ]'
curl -s -X POST $H/api/2.0/mlflow/registered-models/alias -H "$A" -d '{"name":"smoke_model","alias":"champion","version":"1"}' >/dev/null
AL=$(curl -s "$H/api/2.0/mlflow/registered-models/alias?name=smoke_model&alias=champion" -H "$A" | j "['model_version']['version']")
check "alias resolves" '[ "$AL" = 1 ]'

step "scim + permissions"
UID_=$(curl -s -X POST $H/api/2.0/preview/scim/v2/Users -H "$A" -H 'content-type: application/scim+json' -d '{"userName":"bob@example.com","displayName":"Bob"}' | j "['id']")
check "scim user create" '[ -n "$UID_" ]'
GID=$(curl -s -X POST $H/api/2.0/preview/scim/v2/Groups -H "$A" -d "{\"displayName\":\"analysts\",\"members\":[{\"value\":\"$UID_\"}]}" | j "['id']")
check "scim group create" '[ -n "$GID" ]'
F=$(curl -s "$H/api/2.0/preview/scim/v2/Users?filter=userName%20eq%20%22bob@example.com%22" -H "$A" | j "['totalResults']")
check "scim filter" '[ "$F" = 1 ]'
P=$(curl -s -X PUT "$H/api/2.0/permissions/clusters/$CID" -H "$A" -d '{"access_control_list":[{"group_name":"analysts","permission_level":"CAN_ATTACH_TO"}]}' | j "['access_control_list']")
check "permissions put" 'echo "$P" | grep -q CAN_ATTACH_TO'

step "pipelines (DLT)"
PSRC=$(printf -- '-- Databricks notebook source\nCREATE OR REFRESH LIVE TABLE bronze AS SELECT * FROM main.default.smoke_t;\n-- COMMAND ----------\nCREATE OR REFRESH LIVE TABLE silver AS SELECT id*10 AS id10, name FROM LIVE.bronze WHERE id > 1;\n' | base64 -w0)
curl -s -X POST $H/api/2.0/workspace/import -H "$A" -H 'content-type: application/json' -d "{\"path\":\"/Users/admin@lakeforge.local/smoke/dlt\",\"format\":\"SOURCE\",\"language\":\"SQL\",\"content\":\"$PSRC\",\"overwrite\":true}" >/dev/null
PID=$(curl -s -X POST $H/api/2.0/pipelines -H "$A" -H 'content-type: application/json' -d "{\"name\":\"smoke-dlt\",\"catalog\":\"main\",\"target\":\"dlt\",\"libraries\":[{\"notebook\":{\"path\":\"/Users/admin@lakeforge.local/smoke/dlt\"}}],\"clusters\":[{\"existing_cluster_id\":\"$CID\"}]}" | j "['pipeline_id']")
check "pipeline created" '[ -n "$PID" ]'
UPD=$(curl -s -X POST "$H/api/2.0/pipelines/$PID/updates" -H "$A" -d '{"full_refresh":true}' | j "['update_id']")
for i in $(seq 1 90); do R=$(curl -s "$H/api/2.0/pipelines/$PID/updates/$UPD" -H "$A"); US=$(echo "$R" | j "['update']['state']"); case $US in COMPLETED|FAILED|CANCELED) break;; esac; sleep 1; done
check "pipeline update COMPLETED ($US after ${i}s)" '[ "$US" = COMPLETED ]' || curl -s "$H/api/2.0/pipelines/$PID/events" -H "$A" | head -c 1500
R=$(curl -s -X POST $H/api/2.0/sql/statements -H "$A" -H 'content-type: application/json' -d "{\"warehouse_id\":\"$WH\",\"statement\":\"SELECT count(*) FROM main.dlt.silver\",\"wait_timeout\":\"30s\"}")
SN=$(echo "$R" | j "['result']['data_array'][0][0]")
check "silver has 2 rows" '[ "$SN" = 2 ]' || echo "$R" | head -c 500

step "repos"
RP=$(curl -s -X POST $H/api/2.0/repos -H "$A" -H 'content-type: application/json' -d '{"url":"https://github.com/octocat/Hello-World.git","provider":"gitHub","path":"/Repos/admin@lakeforge.local/hello"}')
RPID=$(echo "$RP" | j ".get('id')")
check "repo cloned" '[ -n "$RPID" ] && [ "$RPID" != None ]' || echo "$RP"
RF=$(curl -s "$H/api/2.0/workspace/list?path=/Repos/admin@lakeforge.local/hello" -H "$A" | j "['objects'][0]['path']")
check "repo files synced to workspace" '[ "$RF" = /Repos/admin@lakeforge.local/hello/README ]' || echo "$RF"

step "serving (external model config, no key)"
SE=$(curl -s -X POST $H/api/2.0/serving-endpoints -H "$A" -H 'content-type: application/json' -d '{"name":"smoke-ext","config":{"served_entities":[{"name":"gpt","external_model":{"name":"gpt-4o-mini","provider":"openai","task":"llm/v1/chat","openai_config":{"openai_api_key":"{{secrets/smoke/k}}"}}}]}}' | j "['name']")
check "endpoint created" '[ "$SE" = smoke-ext ]'
sleep 1
ES=$(curl -s $H/api/2.0/serving-endpoints/smoke-ext -H "$A" | j "['state']['ready']")
check "external endpoint READY" '[ "$ES" = READY ]'

step "cleanup"
curl -s -X POST $H/api/2.0/clusters/delete -H "$A" -d "{\"cluster_id\":\"$CID\"}" >/dev/null
printf "\npassed=%d failed=%d\n" $PASS $FAIL
[ $FAIL -eq 0 ]
