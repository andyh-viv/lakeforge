#!/usr/bin/env bash
# Self-test for scripts/check-docs.sh (LF-026 acceptance: "the script itself").
#
# Builds throwaway fixture trees and asserts the checker reports exactly the drift
# that is present in them - a missing route, a stale route, and an issue with no
# OpenSpec reference - and that a clean tree reports nothing and passes --strict.
# Fixtures are used so the test never depends on the real repo's current drift.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CHECK="$ROOT/scripts/check-docs.sh"
[ -x "$CHECK" ] || CHECK="bash $ROOT/scripts/check-docs.sh"

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

fixture() { # dir  -> builds a tree with known drift
  local d="$1"
  mkdir -p "$d/crates/lakeforge-api/src/api" "$d/docs" "$d/openspec/changes/demo"
  cat > "$d/crates/lakeforge-api/src/api/foo.rs" <<'RS'
pub fn router() -> Router {
    Router::new()
        .route("/api/2.0/thing", get(thing))
        .route("/api/2.0/thing/{id}", get(one))
        .route("/api/2.0/gone", get(gone))
}
RS
  cat > "$d/docs/api-surface.md" <<'MD'
# REST API surface

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/2.0/thing` | documented |
| GET | `/api/2.0/thing/{id}` | documented |
| GET | `/api/2.0/legacy` | no longer registered |
MD
  cat > "$d/docs/issues.md" <<'MD'
# Issue inventory

### LF-900 An issue with no OpenSpec reference

### LF-901 ~~An issue that is done~~

### LF-902 An issue referenced by a change
MD
  cat > "$d/openspec/changes/demo/proposal.md" <<'MD'
# Change: demo
Covers LF-902.
MD
}

echo "== fixture 1: drift present =="
D1="$(mktemp -d)"
fixture "$D1"
OUT1="$(bash $CHECK --root "$D1" 2>&1)"
RC1=$?

[ "$RC1" = "0" ] && ok "warn-only exits 0 even with findings" \
                 || bad "warn-only should exit 0, got $RC1"
check_contains "reports the registered-but-undocumented route" "$OUT1" "/api/2.0/gone"
check_contains "reports the documented-but-unregistered route" "$OUT1" "/api/2.0/legacy"
check_contains "reports the unreferenced issue" "$OUT1" "LF-900"
case "$OUT1" in
  *"/api/2.0/thing"*) bad "documented route /api/2.0/thing should be considered documented" ;;
  *) ok "does not treat the documented route as drift" ;;
esac
case "$OUT1" in
  *LF-901*) bad "a done issue (~~strikethrough~~) must not be reported" ;;
  *) ok "treats a struck-through issue as done" ;;
esac
case "$OUT1" in
  *LF-902*) bad "an issue referenced by an OpenSpec change must not be reported" ;;
  *) ok "treats an OpenSpec-referenced issue as covered" ;;
esac

OUT1S="$(bash $CHECK --root "$D1" --strict 2>&1)"
RC1S=$?
[ "$RC1S" != "0" ] && ok "--strict fails when there are findings" \
                   || bad "--strict should fail with findings, got $RC1S"

echo "== fixture 2: clean tree =="
D2="$(mktemp -d)"
fixture "$D2"
cat > "$D2/docs/api-surface.md" <<'MD'
| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/2.0/thing` | documented |
| GET | `/api/2.0/thing[/{id}]` | shorthand for both routes |
| GET | `/api/2.0/gone` | documented because the route exists |
MD
cat > "$D2/docs/issues.md" <<'MD'
### LF-902 An issue referenced by a change
MD
OUT2="$(bash $CHECK --root "$D2" --strict 2>&1)"
RC2=$?
[ "$RC2" = "0" ] && ok "--strict passes on a clean tree" || {
  bad "--strict should pass on a clean tree, got $RC2"; printf '%s\n' "$OUT2" | sed 's/^/    /'
}
check_contains "reports OK on a clean tree" "$OUT2" "OK (no findings)"
check_absent "clean tree reports no undocumented routes" "$OUT2" "undocumented routes:"
check_absent "clean tree reports no stale routes" "$OUT2" "stale documented routes:"
check_absent "clean tree reports no orphan issues" "$OUT2" "without an OpenSpec reference"

echo "== fixture 3: shorthand expansion =="
# `{a,b}` alternation must be expanded, so documenting the family covers the
# individual routes rather than being reported as 1 stale entry.
D3="$(mktemp -d)"
fixture "$D3"
cat > "$D3/docs/api-surface.md" <<'MD'
| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/2.0/{thing,gone}[/{id}]` | family notation |
MD
OUT3="$(bash $CHECK --root "$D3" 2>&1)"
check_contains "expands {a,b} alternation so nothing is undocumented" "$OUT3" "undocumented=0"
check_contains "still reports the genuinely-stale entry" "$OUT3" "stale documented routes:"

rm -rf "$D1" "$D2" "$D3"
echo
printf 'passed=%d failed=%d\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
