#!/usr/bin/env bash
# Docs drift checker (LF-026).
#
#   scripts/check-docs.sh [--strict] [--root DIR] [--quiet]
#
# Two checks:
#   1. routes   - every REST path registered in crates/lakeforge-api/src/api/*.rs
#                 (plus api/mod.rs / lib.rs) is compared with the backticked paths
#                 in docs/api-surface.md, and vice versa.
#   2. issues   - every LF-### defined in docs/issues.md is either referenced by an
#                 OpenSpec change under openspec/changes/ or marked done in the
#                 issue list.
#
# Default is WARN-ONLY (findings printed, exit 0) - that is what CI runs. With
# --strict the script exits 1 when it has findings. Integrity failures
# (extraction, I/O, mktemp) exit nonzero in BOTH modes: a checker that fails
# open is worse than none.
#
# Honesty rules (see docs/development.md "Docs drift checker"):
#   - Never claim coverage it does not have. Anything the checker cannot model
#     is reported in an explicit NOT COMPARED section, never silently dropped
#     and never mislabelled as drift.
#   - A wrong finding is worse than a declared gap.
#
# Portability: bash 3.2 (macOS /bin/bash) and bash 5 (Linux CI); no GNU-only
# flags, no `readarray`, no `${var,,}`, no `sed -i`.
set -u
# Byte-order sort everywhere so `sort` and `comm` agree on macOS and Linux.
export LC_ALL=C
# Fail the pipeline if any stage fails, so an unreadable source file cannot
# silently produce an empty route set that looks "clean".
set -o pipefail

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
    -h|--help) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "check-docs: unknown argument: $1" >&2; exit 2 ;;
  esac
done

API_SRC="$ROOT/crates/lakeforge-api/src"
API_DOC="$ROOT/docs/api-surface.md"
ISSUES="$ROOT/docs/issues.md"
CHANGES="$ROOT/openspec/changes"

# Fail closed: a temp dir is mandatory for correct newline-delimited sets.
WORK="$(mktemp -d)" || { echo "check-docs: cannot create temporary directory" >&2; exit 2; }
trap 'rm -rf "$WORK"' EXIT
REG="$WORK/registered"
DOC="$WORK/documented"
BARE="$WORK/bare"
ELL="$WORK/ellipsis"
FAM="$WORK/family"
UNH="$WORK/unhandled"

findings=0
say() { [ "$QUIET" = "1" ] || printf '%s\n' "$*"; }
finding() { findings=$((findings + 1)); say "  $*"; }
die() { say "check-docs: error: $*"; exit 2; }

# A missing/undeclared root is an integrity failure, not "no findings".
[ -d "$ROOT" ] || die "root directory not found: $ROOT"

# ================================================================ 1. routes

# Extract every route the router actually serves.
#
#   - literal `.route("/path", ...)`
#   - dynamic `for VAR in ["a","b"] { ... .route(&format!("...{VAR}...{{id}}..."), ...) }`
#   - `.nest("/prefix", ...)` applied to the bare routes of the same file
#
# `format!` sites the extractor cannot model are written to "$UNH" and reported
# as NOT COMPARED, never silently dropped.
registered_routes() {
  local f
  for f in "$API_SRC"/api/*.rs "$API_SRC"/api/mod.rs "$API_SRC"/lib.rs; do
    [ -f "$f" ] || continue
    awk '
      function replace_all(s, find, repl,    out, idx) {
        out = ""
        while ((idx = index(s, find)) > 0) {
          out = out substr(s, 1, idx - 1) repl
          s = substr(s, idx + length(find))
        }
        return out s
      }
      function extract_quotes(s,    rest, n) {
        n = 0; rest = s
        while (match(rest, /"[^"]*"/)) {
          n++; qvals[n] = substr(rest, RSTART + 1, RLENGTH - 2)
          rest = substr(rest, RSTART + RLENGTH)
        }
        return n
      }
      function is_full(p) {
        return (substr(p,1,5) == "/api/" || substr(p,1,10) == "/ajax-api/" || p == "/health")
      }
      {
        line = $0
        has_for = match(line, /for[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]+in[[:space:]]+\[/)
        if (has_for) {
          vseg = substr(line, RSTART, RLENGTH)
          match(vseg, /[A-Za-z_][A-Za-z0-9_]*[[:space:]]+in/)
          vv = substr(vseg, RSTART, RLENGTH)
          match(vv, /[A-Za-z_][A-Za-z0-9_]*/)
          loop_var = substr(vv, RSTART, RLENGTH)
          nvals = extract_quotes(line)
          for (i = 1; i <= nvals; i++) loop_vals[i] = qvals[i]
          in_loop = 1; loop_brace = 0
        }
        # process every .route(/.nest( occurrence on the line
        rest = line
        while (match(rest, /\.route\(&format!\("|\.route\([[:space:]]*"|\.nest\([[:space:]]*"/)) {
          m = substr(rest, RSTART, RLENGTH)
          tail = substr(rest, RSTART + RLENGTH)
          qend = index(tail, "\"")
          if (m ~ /format!/) {
            if (qend > 0) {
              tpl = substr(tail, 1, qend - 1)
              if (in_loop && nvals > 0 && index(tpl, "{" loop_var "}") > 0) {
                for (i = 1; i <= nvals; i++) {
                  s = tpl
                  s = replace_all(s, "{" loop_var "}", loop_vals[i])
                  s = replace_all(s, "{{", "{")
                  s = replace_all(s, "}}", "}")
                  if (is_full(s)) full[n_full++] = s
                  else bare[n_bare++] = s
                }
              } else {
                print "UNHANDLED:" tpl > "/dev/stderr"
              }
            }
          } else if (m ~ /\.route\(/) {
            if (qend > 0) {
              p = substr(tail, 1, qend - 1)
              if (is_full(p)) full[n_full++] = p
              else bare[n_bare++] = p
            }
          } else {
            if (qend > 0) nest[n_nest++] = substr(tail, 1, qend - 1)
          }
          rest = tail
        }
        if (in_loop) {
          loop_brace += gsub(/{/, "{", line) - gsub(/}/, "}", line)
          if (loop_brace <= 0) in_loop = 0
        }
      }
      END {
        for (i = 0; i < n_full; i++) print full[i]
        if (n_nest > 0) {
          for (i = 0; i < n_bare; i++)
            for (j = 0; j < n_nest; j++)
              print nest[j] bare[i]
        } else {
          for (i = 0; i < n_bare; i++) print bare[i]
        }
      }
    ' "$f"
  done 2>"$UNH"
}

# Classify the documented backticked path tokens.  Emits, one per line:
#   A<TAB>path   absolute route path (may still carry shorthand)
#   B<TAB>path   bare/relative path (mount subpath or continuation)
#   E<TAB>path   ellipsis notation (NOT COMPARED)
#   F<TAB>path   namespace/mount prefix (family; not a concrete route)
documented_tokens() {
  awk '
    function norm_trailing(s) {
      sub(/[[:space:]]*$/, "", s)
      sub(/\/$/, "", s)
      return s
    }
    function in_fam(t,    i) {
      for (i = 1; i <= nfam; i++) if (fam[i] == t) return 1
      return 0
    }
    {
      line = $0
      if (line ~ /^## /) {
        in_mlflow = (line ~ /[Mm]lflow/)
        if (line ~ /mounted/) {
          tmp = line
          while (match(tmp, /`\/[^`]*`/)) {
            t = substr(tmp, RSTART+1, RLENGTH-2)
            fam[++nfam] = norm_trailing(t)
            print "F\t" norm_trailing(t)
            tmp = substr(tmp, RSTART+RLENGTH)
          }
        }
        next
      }
      if (line ~ /Not present/) { in_np = 1 }
      if (in_np) {
        if (line ~ /^[[:space:]]*$/) in_np = 0
        next
      }
      tmp = line
      while (match(tmp, /(prefixed with|under)[[:space:]]+`\/[^`]*\/`/)) {
        seg = substr(tmp, RSTART, RLENGTH)
        if (match(seg, /`\/[^`]*\/`/)) {
          t = substr(seg, RSTART+1, RLENGTH-2)
          fam[++nfam] = norm_trailing(t)
          print "F\t" norm_trailing(t)
        }
        tmp = substr(tmp, RSTART+RLENGTH)
      }
      mirror = 0
      if (match(line, /also under[[:space:]]+`\/[^`]*`/)) {
        seg = substr(line, RSTART, RLENGTH)
        if (match(seg, /`\/[^`]*`/)) {
          t = substr(seg, RSTART+1, RLENGTH-2)
          fam[++nfam] = norm_trailing(t)
          print "F\t" norm_trailing(t)
          mirror = 1
        }
      }
      while (match(line, /`[^`]*`/)) {
        content = substr(line, RSTART+1, RLENGTH-2)
        line = substr(line, RSTART+RLENGTH)
        c = content
        sub(/^[A-Za-z\/]+[[:space:]]+/, "", c)
        if (substr(c,1,1) == "/") {
          if (c ~ /…/) {
            print "E\t" c
          } else if (in_fam(norm_trailing(c))) {
            # already emitted as family
          } else if (c ~ /^\/api\// || c ~ /^\/ajax-api\// || c == "/health") {
            print "A\t" c
            if (mirror && c ~ /\/api\/2\.0\/preview\/scim\/v2\//) {
              m = c
              sub(/\/api\/2\.0\/preview\/scim\/v2\//, "/api/2.0/account/scim/v2/", m)
              print "A\t" m
            }
          } else {
            print "B\t" c
          }
        } else {
          if (in_mlflow && c ~ /\//) print "B\t/" c
        }
      }
    }
  ' "$API_DOC" 2>/dev/null
}

# Expand shorthand `{a,b}` alternation and `[x]` optional into concrete paths,
# recursively, one construct at a time.  Anything unparseable (ellipsis,
# escapes, unmatched delimiters) is emitted on stderr as UNEXP and dropped from
# stdout.
expand_paths() {
  awk '
    function has_tc(s,    i, d, ch) {
      d = 0
      for (i = 1; i <= length(s); i++) {
        ch = substr(s, i, 1)
        if (ch == "{" || ch == "[") d++
        else if (ch == "}" || ch == "]") d--
        else if (ch == "," && d == 0) return 1
      }
      return 0
    }
    function split_tc(inner, arr,    n, d, i, ch, cur) {
      n = 1; d = 0; cur = ""
      for (i = 1; i <= length(inner); i++) {
        ch = substr(inner, i, 1)
        if (ch == "{" || ch == "[") d++
        else if (ch == "}" || ch == "]") d--
        if (ch == "," && d == 0) { arr[n++] = cur; cur = "" }
        else cur = cur ch
      }
      arr[n] = cur
      return n
    }
    function find_optional(s,    i, j, d, ch) {
      for (i = 1; i <= length(s); i++) {
        if (substr(s, i, 1) == "[") {
          d = 0
          for (j = i; j <= length(s); j++) {
            ch = substr(s, j, 1)
            if (ch == "[" || ch == "{") d++
            else if (ch == "]" || ch == "}") d--
            if (d == 0) { opt_start = i; opt_end = j; return 1 }
          }
        }
      }
      return 0
    }
    function find_brace_alt(s,    i, j, d, ch, inner) {
      for (i = 1; i <= length(s); i++) {
        if (substr(s, i, 1) == "{") {
          d = 0
          for (j = i; j <= length(s); j++) {
            ch = substr(s, j, 1)
            if (ch == "{" || ch == "[") d++
            else if (ch == "}" || ch == "]") d--
            if (d == 0) {
              inner = substr(s, i + 1, j - i - 1)
              if (has_tc(inner)) { br_start = i; br_end = j; return 1 }
              break
            }
          }
        }
      }
      return 0
    }
    function balance_ok(s,    i, ob, cb, osq, csq, ch) {
      ob = 0; cb = 0; osq = 0; csq = 0
      for (i = 1; i <= length(s); i++) {
        ch = substr(s, i, 1)
        if (ch == "{") ob++
        else if (ch == "}") cb++
        else if (ch == "[") osq++
        else if (ch == "]") csq++
      }
      return (ob == cb && osq == csq)
    }
    {
      s = $0
      gsub(/\[\?[^]]*\]/, "", s)
      sub(/[?#].*$/, "", s)
      sub(/[[:space:]]*$/, "", s)
      sub(/\/$/, "", s)
      if (s == "") next
      if (s ~ /…/ || s ~ /\\/) { print "UNEXP:" s > "/dev/stderr"; next }
      if (!balance_ok(s)) { print "UNEXP:" s > "/dev/stderr"; next }
      nq = 1; q[1] = s; head = 1
      while (head <= nq) {
        cur = q[head]; head++
        if (find_optional(cur)) {
          inner = substr(cur, opt_start + 1, opt_end - opt_start - 1)
          q[++nq] = substr(cur, 1, opt_start - 1) inner substr(cur, opt_end + 1)
          q[++nq] = substr(cur, 1, opt_start - 1) substr(cur, opt_end + 1)
        } else if (find_brace_alt(cur)) {
          inner = substr(cur, br_start + 1, br_end - br_start - 1)
          nparts = split_tc(inner, parts)
          for (p = 1; p <= nparts; p++)
            q[++nq] = substr(cur, 1, br_start - 1) parts[p] substr(cur, br_end + 1)
        } else {
          sub(/\/$/, "", cur)
          if (cur != "") print cur
        }
      }
    }
  '
}

# Match bare (relative) documented paths against registered routes: a bare path
# is "documented" if some registered route ends with it (segment boundary, which
# the leading "/" guarantees).
suffix_match() { # <registered-file> <bare-file> -> registered routes covered
  awk 'NR == FNR { reg[++n] = $0; next }
       { for (i = 1; i <= n; i++)
           if (length(reg[i]) >= length($0) && substr(reg[i], length(reg[i]) - length($0) + 1) == $0)
             print reg[i] }' "$1" "$2" | sort -u
}

# Bare (relative) documented paths that match NO registered route -> stale.
suffix_unmatched() { # <registered-file> <bare-file> -> unmatched bare paths
  awk 'NR == FNR { reg[++n] = $0; next }
       { found = 0
         for (i = 1; i <= n; i++)
           if (length(reg[i]) >= length($0) && substr(reg[i], length(reg[i]) - length($0) + 1) == $0) { found = 1; break }
         if (!found) print }' "$1" "$2" | sort -u
}

say "check-docs: routes"
if [ ! -f "$API_DOC" ]; then
  say "  (skipped: $API_DOC not found)"
else
  registered_routes | sed 's#/$##' | sort -u > "$REG" || die "failed to extract registered routes"
  [ -s "$UNH" ] && sed 's/^/  NOT COMPARED (unmodeled format!): /' "$UNH" >&2

  documented_tokens > "$WORK/tokens" || die "failed to extract documented routes"
  grep '^F' "$WORK/tokens" | cut -f2- | sort -u > "$FAM"
  grep '^E' "$WORK/tokens" | cut -f2- | sort -u > "$ELL"

  # absolute documented paths (expand) + bare paths (expand then suffix-match)
  grep '^A' "$WORK/tokens" | cut -f2- | expand_paths 2>>"$UNH" | sort -u > "$DOC"
  grep '^B' "$WORK/tokens" | cut -f2- | expand_paths 2>>"$UNH" | sort -u > "$BARE"
  suffix_match "$REG" "$BARE" >> "$DOC"
  sort -u -o "$DOC" "$DOC"

  # a bare (relative) documented path that matches no registered route is
  # documented-but-not-registered: report it as stale, not silently drop it
  suffix_unmatched "$REG" "$BARE" > "$WORK/bare-unmatched"

  n_reg="$(grep -c . "$REG" || true)"
  n_doc="$(grep -c . "$DOC" || true)"
  n_fam="$(grep -c . "$FAM" || true)"

  # registered routes documented only via ellipsis are not compared
  n_shadow=0
  : > "$WORK/shadow"
  if [ -s "$ELL" ]; then
    while IFS= read -r e; do
      base="${e%…}"; base="${base%/}"
      [ -n "$base" ] && grep -F "$base" "$REG" >> "$WORK/shadow" || true
    done < "$ELL"
    sort -u -o "$WORK/shadow" "$WORK/shadow" 2>/dev/null || true
    n_shadow="$(grep -c . "$WORK/shadow" || true)"
  fi

  undoc="$(comm -23 "$REG" "$DOC" | grep -vFf "$WORK/shadow")"
  n_undoc="$(printf '%s\n' "$undoc" | grep -c . || true)"

  stale="$( { comm -13 "$REG" "$DOC"; cat "$WORK/bare-unmatched"; } | sort -u )"
  n_stale="$(printf '%s\n' "$stale" | grep -c . || true)"

  n_unexp="$(grep -c '^UNEXP:' "$UNH" || true)"
  n_unh="$(grep -c '^UNHANDLED:' "$UNH" || true)"
  n_ellipsis="$(grep -c . "$ELL" || true)"

  say "  registered=$n_reg documented=$n_doc undocumented=$n_undoc stale-in-docs=$n_stale family-notation=$n_fam not-compared=$((n_unexp + n_unh + n_ellipsis + n_shadow))"
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
  if [ "$n_fam" != "0" ]; then
    say "  namespace/mount prefixes (explicitly marked, not drift):"
    grep . "$FAM" | head -n "$LIMIT" | sed 's/^/    /'
  fi
  if [ "$n_unexp" != "0" ] || [ "$n_unh" != "0" ]; then
    say "  NOT COMPARED - documented notation this checker cannot expand:"
    grep '^UNEXP:' "$UNH" | sed 's/^UNEXP:/    /'
    grep '^UNHANDLED:' "$UNH" | sed 's/^UNHANDLED:/    (route template) /'
  fi
  if [ "$n_shadow" != "0" ]; then
    say "  NOT COMPARED - documented only via ellipsis (e.g. \`…\`):"
    grep . "$ELL" | head -n "$LIMIT" | sed 's/^/    /'
    say "    ($n_shadow registered routes under those prefixes are not compared)"
  fi
fi

# ================================================================ 2. issues

say "check-docs: issue references"
if [ ! -f "$ISSUES" ]; then
  say "  (skipped: $ISSUES not found)"
else
  # Defined issues: "### LF-042 Title", plus the two supported done conventions
  # "### ~~LF-042 Title~~" and "### LF-042 ~~Title~~".
  defined="$(sed -n 's/^### \(~~\)\{0,1\}\(LF-[0-9][0-9]*\).*/\2/p' "$ISSUES" | sort -u)"
  # Done: full-title strikethrough (id inside or outside the ~~) or an explicit
  # (done)/[done] marker. A live heading such as "### LF-900 Replace ~~old~~ wording"
  # is NOT done.
  done_ids="$(
    {
      sed -n 's/^### ~~\(LF-[0-9][0-9]*\) .*~~.*/\1/p' "$ISSUES"
      sed -n 's/^### \(LF-[0-9][0-9]*\) ~~.*~~[[:space:]]*$/\1/p' "$ISSUES"
      grep -E '^### (~~)?LF-[0-9][0-9]* ' "$ISSUES" | grep -E '\(done\)|\[done\]' \
        | sed -n 's/^### \(~~\)\{0,1\}\(LF-[0-9][0-9]*\).*/\2/p'
    } | sort -u
  )"
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

# ================================================================ verdict

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
