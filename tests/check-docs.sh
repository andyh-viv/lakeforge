#!/usr/bin/env bash
# Self-test for scripts/check-docs.sh (LF-026 acceptance: "the script itself").
#
# Builds throwaway fixture trees and asserts the checker reports exactly the
# drift that is present - with EXACT counts, not just "a heading appeared".
# Fixtures are used so the test never depends on the real repo's current drift.
#
# Covered cases:
#   - dynamic `format!` routes (loop expansion)
#   - `nest()` mount prefixes (bare subroutes mounted)
#   - non-/api/ routes (/health, /ajax-api/...)
#   - adjacent optionals `[a][b]`
#   - unsupported notation (ellipsis) -> NOT COMPARED, never a false finding
#   - multiple stale entries with an exact count (not the 1/1 display bug)
#   - removed-parent-with-live-child (must be stale, not "family")
#   - full-title strikethrough done markers vs a partial ~~ in a live title
#   - failure path: an undeclared root exits nonzero, never "OK"
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CHECK="$ROOT/scripts/check-docs.sh"

pass=0; fail=0
ok()   { pass=$((pass + 1)); echo "  ok   $1"; }
bad()  { fail=$((fail + 1)); echo "  FAIL $1"; }
check_contains() { # name, haystack, needle
  case "$2" in
    *"$3"*) ok "$1" ;;
    *) bad "$1 (expected to find: $3)"; echo "--- output was:"; printf '%s\n' "$2" | sed 's/^/    /' ;;
  esac
}
check_absent() { # name, haystack, needle
  case "$2" in
    *"$3"*) bad "$1 (should NOT report: $3)"; echo "--- output was:"; printf '%s\n' "$2" | sed 's/^/    /' ;;
    *) ok "$1" ;;
  esac
}
# extract a summary metric value, e.g. metric "$OUT" undocumented
metric() {
  printf '%s\n' "$1" | sed -n "s/.*[[:space:]]$2=\([0-9][0-9]*\).*/\1/p" | head -1
}
check_metric() { # name, haystack, key, expected
  got="$(metric "$2" "$3")"
  [ "$got" = "$4" ] && ok "$1 ($3=$4)" \
                     || bad "$1 (expected $3=$4, got '$got')"
}

# Base skeleton: one API source file, an issue referenced by a change.
mkbase() { # dir
  local d="$1"
  mkdir -p "$d/crates/lakeforge-api/src/api" "$d/docs" "$d/openspec/changes/demo"
  cat > "$d/docs/issues.md" <<'MD'
# Issues

### LF-100 A referenced issue
MD
  cat > "$d/openspec/changes/demo/proposal.md" <<'MD'
# demo
Covers LF-100.
MD
}
writesrc() { # dir file content-via-stdin
  local d="$1" f="$2"
  cat > "$d/crates/lakeforge-api/src/api/$f"
}
writedoc() { # dir content-via-stdin
  cat > "$1/docs/api-surface.md"
}

echo "== fixture 1: dynamic format! routes (loop expansion) =="
D1="$(mktemp -d)"; mkbase "$D1"
writesrc "$D1" jobs.rs <<'RS'
pub fn router() -> Router {
    let mut r = Router::new();
    for v in ["2.0", "2.1"] {
        r = r.route(&format!("/api/{v}/jobs/list"), get(list));
    }
    r
}
RS
writedoc "$D1" <<'MD'
# API

| GET | `/api/2.{0,1}/jobs/list` | |
MD
OUT1="$(bash $CHECK --root "$D1" 2>&1)"; RC1=$?
check_metric "dynamic routes are not reported undocumented" "$OUT1" undocumented 0
check_metric "dynamic routes are not reported stale" "$OUT1" stale-in-docs 0
check_contains "reports the route check" "$OUT1" "check-docs: routes"

echo "== fixture 2: nest() mount prefixes =="
D2="$(mktemp -d)"; mkbase "$D2"
writesrc "$D2" catalog.rs <<'RS'
pub fn router() -> Router {
    let uc = Router::new().route("/catalogs", get(list)).route("/catalogs/{name}", get(get));
    Router::new().nest("/api/2.0/unity-catalog", uc)
}
RS
writedoc "$D2" <<'MD'
# API

| GET | `/catalogs[/{name}]` | |
MD
OUT2="$(bash $CHECK --root "$D2" 2>&1)"; RC2=$?
check_metric "nested routes resolve to their mounted paths" "$OUT2" undocumented 0
check_metric "no bare /catalogs is reported stale" "$OUT2" stale-in-docs 0

echo "== fixture 3: non-/api/ routes (/health, /ajax-api) =="
D3="$(mktemp -d)"; mkbase "$D3"
writesrc "$D3" mod.rs <<'RS'
pub fn router() -> Router {
    Router::new().route("/health", get(h)).route("/ajax-api/2.0/mlflow/x", get(x))
}
RS
writedoc "$D3" <<'MD'
# API

| GET | `/health` | |
| GET | `/ajax-api/2.0/mlflow/x` | |
MD
OUT3="$(bash $CHECK --root "$D3" 2>&1)"; RC3=$?
check_metric "non-/api/ routes are not undocumented" "$OUT3" undocumented 0
check_metric "non-/api/ routes are not stale" "$OUT3" stale-in-docs 0

echo "== fixture 4: adjacent optionals [a][b] =="
D4="$(mktemp -d)"; mkbase "$D4"
writesrc "$D4" foo.rs <<'RS'
pub fn router() -> Router {
    Router::new().route("/api/x/a", get(a)).route("/api/x/b", get(b))
}
RS
writedoc "$D4" <<'MD'
# API

| GET | `/api/x/[a][b]` | |
MD
OUT4="$(bash $CHECK --root "$D4" 2>&1)"; RC4=$?
# The old expander produced only /api/x/ and /api/x/ab, misreporting /api/x/a
# and /api/x/b as undocumented.  Correct expansion yields all four variants.
check_metric "adjacent optionals cover /api/x/a and /api/x/b" "$OUT4" undocumented 0
check_metric "the /api/x and /api/x/ab variants are documented-not-registered" "$OUT4" stale-in-docs 2

echo "== fixture 5: ellipsis is NOT COMPARED, not drift =="
D5="$(mktemp -d)"; mkbase "$D5"
writesrc "$D5" foo.rs <<'RS'
pub fn router() -> Router {
    Router::new().route("/api/2.0/things", get(t)).route("/api/2.0/things/{id}", get(t2))
}
RS
writedoc "$D5" <<'MD'
# API

| GET | `/api/2.0/things…` | |
MD
OUT5="$(bash $CHECK --root "$D5" 2>&1)"; RC5=$?
check_metric "ellipsis-covered routes are not undocumented" "$OUT5" undocumented 0
check_metric "ellipsis-covered routes are not stale" "$OUT5" stale-in-docs 0
check_contains "ellipsis is reported as NOT COMPARED" "$OUT5" "NOT COMPARED"

echo "== fixture 6: multiple stale entries counted exactly =="
D6="$(mktemp -d)"; mkbase "$D6"
writesrc "$D6" foo.rs <<'RS'
pub fn router() -> Router {
    Router::new()
}
RS
writedoc "$D6" <<'MD'
# API

| GET | `/api/2.0/gone-a` | |
| GET | `/api/2.0/gone-b` | |
| GET | `/api/2.0/gone-c` | |
MD
OUT6="$(bash $CHECK --root "$D6" 2>&1)"; RC6=$?
check_metric "three stale entries are counted, not collapsed to 1" "$OUT6" stale-in-docs 3
check_contains "lists each stale route" "$OUT6" "/api/2.0/gone-a"
check_contains "lists each stale route" "$OUT6" "/api/2.0/gone-b"
check_contains "lists each stale route" "$OUT6" "/api/2.0/gone-c"

echo "== fixture 7: removed parent with a live child is stale =="
D7="$(mktemp -d)"; mkbase "$D7"
writesrc "$D7" foo.rs <<'RS'
pub fn router() -> Router {
    Router::new().route("/api/items/{id}", get(one))
}
RS
writedoc "$D7" <<'MD'
# API

| GET | `/api/items` | |
| GET | `/api/items/{id}` | |
MD
OUT7="$(bash $CHECK --root "$D7" 2>&1)"; RC7=$?
check_metric "removed /api/items is reported stale (not hidden as family)" "$OUT7" stale-in-docs 1
check_contains "the removed parent is the stale entry" "$OUT7" "/api/items"
check_metric "the live child is not undocumented" "$OUT7" undocumented 0

echo "== fixture 8: done-marker detection (finding 8) =="
D8="$(mktemp -d)"; mkbase "$D8"
cat > "$D8/docs/issues.md" <<'MD'
# Issues

### LF-101 A live issue with a partial ~~old~~ strike
### ~~LF-102 A done issue with id inside the strike~~ (done — PR #1)
### LF-103 ~~A done issue with the title struck~~
MD
writedoc "$D8" <<'MD'
# API

| GET | `/api/2.0/x` | |
MD
writesrc "$D8" foo.rs <<'RS'
pub fn router() -> Router { Router::new().route("/api/2.0/x", get(x)) }
RS
OUT8="$(bash $CHECK --root "$D8" 2>&1)"; RC8=$?
check_contains "the partially-struck live title is still uncovered" "$OUT8" "LF-101"
check_absent "a full-title strikethrough (id inside) is done" "$OUT8" "LF-102"
check_absent "a full-title strikethrough (title struck) is done" "$OUT8" "LF-103"

echo "== fixture 9: failure path - undeclared root is nonzero, not OK =="
OUT9="$(bash $CHECK --root /nonexistent-check-docs-fixture 2>&1)"; RC9=$?
if [ "$RC9" != "0" ]; then ok "undeclared root exits nonzero (got $RC9)"
else bad "undeclared root should exit nonzero"; fi
check_absent "undeclared root does not report OK" "$OUT9" "OK (no findings)"
check_contains "undeclared root names the missing root" "$OUT9" "root directory not found"

echo "== fixture 10: clean tree passes --strict =="
D10="$(mktemp -d)"; mkbase "$D10"
writesrc "$D10" foo.rs <<'RS'
pub fn router() -> Router {
    Router::new().route("/api/2.0/thing", get(t)).route("/api/2.0/thing/{id}", get(one))
}
RS
writedoc "$D10" <<'MD'
# API

| GET | `/api/2.0/thing[/{id}]` | |
MD
OUT10="$(bash $CHECK --root "$D10" --strict 2>&1)"; RC10=$?
[ "$RC10" = "0" ] && ok "--strict passes on a clean tree" || bad "--strict should pass (got $RC10): $OUT10"
check_contains "clean tree reports OK" "$OUT10" "OK (no findings)"

rm -rf "$D1" "$D2" "$D3" "$D4" "$D5" "$D6" "$D7" "$D8" "$D10"
echo
printf 'passed=%d failed=%d\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
