#!/usr/bin/env bash
# Docs drift checker (LF-026).
#
#   scripts/check-docs.sh [--strict] [--root DIR] [--quiet]
#
# Two checks:
#   1. routes   - every REST path registered in crates/lakeforge-api/src/api/*.rs
#                 (plus lib.rs / api/mod.rs) is present in docs/api-surface.md,
#                 and every path documented there still exists.
#   2. issues   - every LF-### defined in docs/issues.md is either referenced by an
#                 OpenSpec change under openspec/changes/ or marked done in the
#                 issue list.
#
# Default is WARN-ONLY (findings printed, exit 0) - that is what CI runs. With
# --strict the script exits 1 when it has findings, for use locally or once the
# docs are clean.
#
# Deliberate design choice: this checker reports what it could NOT parse instead
# of silently skipping it. A checker that cries wolf, or that hides a whole class
# of routes behind "unparseable", is worse than none.
#
# Portability: bash 3.2 (macOS /bin/bash) and bash 5 (Linux CI); no GNU-only
# flags, no `readarray`, no `${var,,}`, no `sed -i`.
set -u

STRICT=0
QUIET=0
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LIMIT="${DOCS_CHECK_LIMIT:-20}"

while [ $# -gt 0 ]; do
  case "$1" in
    --strict) STRICT=1; shift ;;
    --quiet) QUIET=1; shift ;;
    --root)
      if [ $# -lt 2 ]; then echo "check-docs: --root needs a directory" >&2; exit 2; fi
      ROOT="$2"; shift 2 ;;
    -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "check-docs: unknown argument: $1" >&2; exit 2 ;;
  esac
done

API_SRC="$ROOT/crates/lakeforge-api/src"
API_DOC="$ROOT/docs/api-surface.md"
ISSUES="$ROOT/docs/issues.md"
CHANGES="$ROOT/openspec/changes"

findings=0
say() { [ "$QUIET" = "1" ] || printf '%s\n' "$*"; }
finding() { findings=$((findings + 1)); say "  $*"; }

# ------------------------------------------------------------------ 1. routes

# Registered paths: `.route("/path", ...)` as written in the router modules.
registered_routes() {
  {
    for f in "$API_SRC"/api/*.rs "$API_SRC"/api/mod.rs "$API_SRC"/lib.rs; do
      [ -f "$f" ] || continue
      sed -n 's/.*\.route([[:space:]]*"\([^"]*\)".*/\1/p' "$f"
    done
  } | normalize | sort -u
}

# Documented paths: backticked paths in the API surface table, e.g.
#   | POST | `/api/2.0/thing/{id}` | notes |
# Documented shorthand `{a,b}` (alternation) and `[x]` (optional) is expanded, so
# `/api/2.0/token/{create,list,delete}` compares against the three real routes.
documented_routes() {
  grep -Eo '`/api/[^`]+`' "$API_DOC" 2>/dev/null \
    | sed 's/^`//; s/`$//' \
    | expand_shorthand \
    | normalize \
    | sort -u
}

# Paths the shorthand expander could not fully handle (nested groups, escapes).
unexpanded_documented() {
  grep -Eo '`/api/[^`]+`' "$API_DOC" 2>/dev/null \
    | sed 's/^`//; s/`$//' \
    | grep -E '\{[^}]*\{|\[[^]]*\[' || true
}

normalize() {
  sed -e 's/[?#].*$//' \
      -e 's/[[:space:]]*$//' \
      -e 's#/$##' \
      -e 's#/:\([A-Za-z_][A-Za-z0-9_]*\)#/{\1}#g' \
      -e 's#/(\*)#/{*}#g' \
    | grep -v '^$' || true
}

# Expand one documented token into concrete paths on stdout.
# `{a,b,c}` -> a|b|c ; `[x]` -> with x and without x. Single level only; anything
# deeper is left for unexpanded_documented() to report.
expand_shorthand() {
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    expand_braces "$line" | expand_optional
  done
}

expand_braces() {
  # Expand the first `{a,b,c}` group, recursively, on one documented path.
  printf '%s\n' "$1" | awk '
    function rec(s,   i, rest, cl, mid, post, parts, j, pre) {
      i = match(s, /\{[^{}]*,[^{}]*\}/)
      if (i == 0) { print s; return }
      rest = substr(s, i)
      cl = index(rest, "}")
      mid = substr(rest, 2, cl - 2)
      post = substr(rest, cl + 1)
      pre = substr(s, 1, i - 1)
      split(mid, parts, ",")
      for (j = 1; j <= length(parts); j++) rec(pre parts[j] post)
    }
    { rec($0) }
  '
}

expand_optional() {
  # `a[/{id}]` means both `a` and `a/{id}`. Emit each variant so the comparison
  # is against real paths rather than the shorthand text.
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    if printf '%s' "$line" | grep -q '\['; then
      printf '%s\n' "$line" | sed 's/\[\([^][]*\)\]/\1/g'
      printf '%s\n' "$line" | sed 's/\[[^][]*\]//g'
    else
      printf '%s\n' "$line"
    fi
  done | sort -u
}

say "check-docs: routes"
if [ ! -f "$API_DOC" ]; then
  say "  (skipped: $API_DOC not found)"
else
  reg="$(mktemp)"; doc="$(mktemp)"
  registered_routes > "$reg"
  documented_routes > "$doc"

  undoc="$(comm -23 "$reg" "$doc")"
  n_undoc="$(printf '%s\n' "$undoc" | grep -c . || true)"
  n_reg="$(grep -c . "$reg" || true)"

  # Split the documented-but-unregistered set: an entry that is a strict prefix of
  # a registered route is family/prose notation (a bare namespace heading, a
  # trailing-slash grouping), not stale documentation. Reporting those as drift
  # would bury the real findings.
  stale=""
  family=""
  for d in $(comm -13 "$reg" "$doc"); do
    if awk -v p="$d" 'index($0, p "/") == 1 { found = 1 } END { exit(found ? 0 : 1) }' "$reg"; then
      family="$family $d"
    else
      stale="$stale $d"
    fi
  done
  n_stale="$(printf '%s\n' "$stale" | grep -c . || true)"
  n_family="$(printf '%s\n' "$family" | grep -c . || true)"

  say "  registered=$n_reg documented=$(grep -c . "$doc" || true) undocumented=$n_undoc stale-in-docs=$n_stale family-notation=$n_family"
  if [ "$n_undoc" != "0" ]; then
    say "  routes registered but not in docs/api-surface.md:"
    printf '%s\n' "$undoc" | grep . | head -n "$LIMIT" | sed 's/^/    /'
    [ "$n_undoc" -gt "$LIMIT" ] && say "    ... and $((n_undoc - LIMIT)) more"
    finding "undocumented routes: $n_undoc"
  fi
  if [ "$n_stale" != "0" ]; then
    say "  routes documented but no longer registered:"
    printf '%s\n' "$stale" | grep . | head -n "$LIMIT" | sed 's/^/    /'
    [ "$n_stale" -gt "$LIMIT" ] && say "    ... and $((n_stale - LIMIT)) more"
    finding "stale documented routes: $n_stale"
  fi
  if [ "$n_family" != "0" ]; then
    say "  documented namespace/prefix entries (exact routes compared separately):"
    printf '%s\n' "$family" | grep . | head -n "$LIMIT" | sed 's/^/    /'
  fi
  unexp="$(unexpanded_documented)"
  n_unexp="$(printf '%s\n' "$unexp" | grep -c . || true)"
  if [ "$n_unexp" != "0" ]; then
    say "  NOT COMPARED - documented notation this checker cannot expand"
    say "  (nested groups); verify these by hand:"
    printf '%s\n' "$unexp" | grep . | head -n "$LIMIT" | sed 's/^/    /'
    finding "unexpanded documented notation: $n_unexp"
  fi
  rm -f "$reg" "$doc"
fi

# ------------------------------------------------------------------ 2. issues

say "check-docs: issue references"
if [ ! -f "$ISSUES" ]; then
  say "  (skipped: $ISSUES not found)"
else
  # Defined issues: "### LF-042 Title", and also the done convention
  # "### ~~LF-042 Title~~ (done — PR #n)" where the strikethrough wraps the id too.
  # Two spellings of the same shape: sed is BRE, grep -E is ERE.
  DEF_BRE='^### \(~~\)\{0,1\}\(LF-[0-9][0-9]*\)'
  DEF_ERE='^### (~~)?(LF-[0-9][0-9]*)'
  defined="$(sed -n "s/${DEF_BRE}.*/\2/p" "$ISSUES" | sort -u)"
  # Marked done = strikethrough anywhere on the heading, or an explicit marker.
  done_ids="$(grep -E "$DEF_ERE" "$ISSUES" | grep -E '~~|\(done\)|\[done\]' \
    | sed -n "s/${DEF_BRE}.*/\2/p" | sort -u)"
  # Referenced from any OpenSpec change artifact.
  referenced="$(grep -rhoE 'LF-[0-9][0-9]*' "$CHANGES" 2>/dev/null | sort -u)"

  n_def=0; n_orphan=0; orphans=""
  for id in $defined; do
    n_def=$((n_def + 1))
    if printf '%s\n' "$done_ids" | grep -qx "$id"; then continue; fi
    if printf '%s\n' "$referenced" | grep -qx "$id"; then continue; fi
    orphans="$orphans $id"
    n_orphan=$((n_orphan + 1))
  done

  n_ref="$(printf '%s\n' "$referenced" | grep -c '^LF-' || true)"
  n_done="$(printf '%s\n' "$done_ids" | grep -c '^LF-' || true)"
  say "  defined=$n_def referenced-by-openspec=$n_ref marked-done=$n_done uncovered=$n_orphan"
  if [ "$n_orphan" != "0" ]; then
    say "  issues neither referenced by an OpenSpec change nor marked done:"
    for id in $orphans; do say "    $id"; done
    finding "issues without an OpenSpec reference or done marker: $n_orphan"
  fi
fi

# ------------------------------------------------------------------ verdict

if [ "$findings" = "0" ]; then
  say "check-docs: OK (no findings)"
  exit 0
fi
say "check-docs: $findings finding group(s)"
if [ "$STRICT" = "1" ]; then
  say "check-docs: --strict, failing"
  exit 1
fi
say "check-docs: warn-only mode, not failing (use --strict to fail)"
exit 0
