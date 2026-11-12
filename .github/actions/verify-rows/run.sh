#!/usr/bin/env bash
# Reads a source's stored rows back through the team's SQL API connection and runs a list of checks on them. An agent
# sends more than the test wrote (its own logs, the runner's system logs, host metrics), so a check does not count
# rows: it passes once at least one stored row is picked by its "where" and holds its "contains". "contains" is
# exact for strings, numbers and booleans, objects may hold more keys than expected, and every expected array item
# must be found in the stored array. Logs and spans land in hot storage first and are copied to cold storage later,
# so both are read. Checks that did not pass are read again every 10 s until the deadline.
# Writes <out>/summary.md on every exit path and adds it to the job summary.
# Env: INPUT_CONNECTION_USERNAME, INPUT_CONNECTION_PASSWORD, INPUT_CONNECT_HOST, INPUT_COLLECTION, INPUT_CHECKS,
#      INPUT_OUT (default out), INPUT_DEADLINE_SECONDS (default 300).
set -euo pipefail
# A native jq.exe, as on Windows runners, ends its output lines with CRLF unless given -b.
if [ "${OS:-}" = Windows_NT ]; then jq() { command jq -b "$@"; }; fi

OUT=${INPUT_OUT:-out}
FAILURE=""
mkdir -p "$OUT"
rm -f "$OUT"/check-*.json
printf '%s' "$INPUT_CHECKS" > "$OUT/checks.json"

fail() { FAILURE="$1"; echo "::error::$1"; exit 1; }

summarize() {
  local status=$?
  {
    echo "Read back through \`$INPUT_CONNECT_HOST\` from \`$INPUT_COLLECTION\`"
    echo
    if ls "$OUT"/check-*.json >/dev/null 2>&1; then
      jq -rs '.[] | "- \(if .ok then "✅" else "❌" end) \(.text)"' "$OUT"/check-*.json
      echo
      echo "<details><summary>One stored row per check</summary>"
      echo
      local result
      for result in "$OUT"/check-*.json; do
        echo "**$(jq -r .name "$result")**"
        echo
        echo '```json'
        jq -r '.sample // "nothing stored matches the condition"' "$result"
        echo '```'
        echo
      done
      echo "</details>"
    else
      echo "- ❌ ${FAILURE:-stopped on an error, see the log of this step}"
    fi
  } > "$OUT/summary.md"
  [ -z "${GITHUB_STEP_SUMMARY:-}" ] || cat "$OUT/summary.md" >> "$GITHUB_STEP_SUMMARY"
  exit "$status"
}
trap summarize EXIT

jq -e 'type == "array" and length > 0 and all(.[]; type == "object" and (.name | type) == "string"
    and (.in | IN("logs", "spans", "metrics")) and ((.where // "") | type) == "string" and ((.contains // {}) | type) == "object")' \
  "$OUT/checks.json" >/dev/null 2>&1 \
  || fail "checks is not a JSON array of {\"name\", \"in\": logs, spans or metrics, optional \"where\", optional \"contains\"} objects"
COUNT=$(jq length "$OUT/checks.json")

# Writes the rows as JSONEachRow into $2, or nothing while the collection does not exist: it is created with the
# source's first stored row. The connection user may run 4 logs queries at a time and every E2E job of ours reads
# back as that user, so ?wait=true queues the query for up to 60 s instead of answering HTTP 429.
sql() {
  local code
  code=$(curl -sS -m 90 -o "$2" -w '%{http_code}' -u "$INPUT_CONNECTION_USERNAME:$INPUT_CONNECTION_PASSWORD" \
    -H 'Content-Type: plain/text' --data-binary "$1 FORMAT JSONEachRow" "https://$INPUT_CONNECT_HOST/?wait=true") || code=000
  if [ "$code" = 500 ] && grep -qE 'CLUSTER_DOESNT_EXIST|NAMED_COLLECTION_DOESNT_EXIST|UNKNOWN_TABLE' "$2"; then : > "$2"; return; fi
  [ "$code" = 200 ] || fail "the SQL API on $INPUT_CONNECT_HOST answered HTTP $code: $(head -c 300 "$2" 2>/dev/null)"
}

# Writes the rows a check picks into <out>/rows.json, newest first: the raw JSON of logs and spans, {name, tags} of metrics.
read_rows() {
  local kind=$1 where=$2
  if [ "$kind" = metrics ]; then
    sql "SELECT name, tags FROM remote(${INPUT_COLLECTION}_metrics) WHERE name IS NOT NULL AND ($where) GROUP BY name, tags ORDER BY name LIMIT 1000" "$OUT/hot.ndjson"
    jq -s . "$OUT/hot.ndjson" > "$OUT/rows.json"
  else
    local row_type=1
    [ "$kind" = logs ] || row_type=3
    sql "SELECT dt, raw FROM remote(${INPUT_COLLECTION}_$kind) WHERE ($where) ORDER BY dt DESC LIMIT 200" "$OUT/hot.ndjson"
    sql "SELECT dt, raw FROM s3Cluster(primary, ${INPUT_COLLECTION}_s3) WHERE _row_type = $row_type AND ($where) ORDER BY dt DESC LIMIT 200" "$OUT/cold.ndjson"
    jq -n --slurpfile hot "$OUT/hot.ndjson" --slurpfile cold "$OUT/cold.ndjson" '[($hot + $cold)[] | .raw | fromjson?]' > "$OUT/rows.json"
  fi
}

DEADLINE=$((SECONDS + ${INPUT_DEADLINE_SECONDS:-300}))
while :; do
  PENDING=0
  for ((i = 0; i < COUNT; i++)); do
    RESULT="$OUT/check-$(printf '%02d' "$i").json"
    if [ -f "$RESULT" ] && jq -e .ok "$RESULT" >/dev/null; then continue; fi
    read_rows "$(jq -r ".[$i].in" "$OUT/checks.json")" "$(jq -r ".[$i].where // \"1\"" "$OUT/checks.json")"
    jq --argjson i "$i" --slurpfile rows "$OUT/rows.json" '
      def text: tojson | if length > 300 then .[0:300] + "..." else . end;
      def holds($e): . as $r
        | if ($e | type) == "object" then ($r | type) == "object" and all($e | to_entries[]; . as $kv | ($r | has($kv.key)) and ($r[$kv.key] | holds($kv.value)))
          elif ($e | type) == "array" then ($r | type) == "array" and all($e[]; . as $x | any($r[]; holds($x)))
          else $r == $e end;
      .[$i] as $check | $rows[0] as $rows | ($check.contains // {}) as $contains
      | [$rows[] | select(holds($contains))] as $matched
      | (if $check | has("where") then " where `\($check.where)`" else "" end) as $where
      | {name: $check.name, ok: ($matched | length > 0), sample: ($matched[0] // $rows[0]),
         text: (if $matched | length > 0 then "\($check.name): stored in \($check.in)"
                elif $rows | length == 0 then "\($check.name): nothing stored in \($check.in)\($where)"
                else "\($check.name): \($rows | length) rows stored in \($check.in)\($where), none of them holds `\($contains | text)`" end)}
    ' "$OUT/checks.json" > "$RESULT"
    jq -e .ok "$RESULT" >/dev/null || PENDING=$((PENDING + 1))
  done
  [ "$PENDING" -gt 0 ] || break
  [ "$SECONDS" -lt "$DEADLINE" ] || break
  echo "$PENDING of $COUNT checks found no row yet, reading again in 10 s"
  sleep 10
done

jq -rs '.[] | "\(if .ok then "✅" else "❌" end) \(.text)"' "$OUT"/check-*.json
# The job summary shows a stored row per check. A failed check's row goes to the log too, which is what the API serves.
jq -rs '.[] | select((.ok | not) and .sample != null) | "newest row picked for \"\(.name)\": \(.sample | tojson)"' "$OUT"/check-*.json
PROBLEMS=$(jq -rs 'map(select(.ok | not) | .text) | join("; ")' "$OUT"/check-*.json)
[ -z "$PROBLEMS" ] || fail "the stored rows are not what the test sent: $PROBLEMS"
echo "every check found its row"
