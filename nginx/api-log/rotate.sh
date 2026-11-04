#!/bin/sh
# Bounds the /api request log nginx writes (nginx/nginx.conf, `ks_api`).
#
# The `api-log` service in docker-compose.yml runs this every five minutes and
# reads it afresh on every pass, from a directory mount, so an edit here takes
# effect at the first pass after the deploy that brings it, with no recreate.
#
# THE BOUND. At most KEEP cuts, and the live file is cut once it passes
# MAX_BYTES, so the directory never holds much more than KEEP x MAX_BYTES of
# text before compression, whatever the traffic does: the cuts plus at most
# five minutes of what arrived since the last pass. On an ordinary day the
# file is cut once, at the first pass after midnight UTC, so KEEP is also the
# window in days: 62 is two months. That covers a lever somebody runs by hand
# once a month twice over, and the month a Python route is kept after its
# switch to Rust. A count of cuts, not a date to delete by: nothing reads an
# old line except the person counting.
#
# Copy, then truncate, never rename: nginx holds the file open and nothing
# here can signal it, so a renamed file would go on receiving every line.
# nginx opened it O_APPEND, so after the truncate its next line lands at the
# start. A line written between the copy and the truncate is lost. This log
# takes well under a request a second and is kept for counting, so that is a
# rounding error.
set -eu

DIR="${KS_API_LOG_DIR:-/var/log/ks-api}"
KEEP="${KS_API_LOG_KEEP:-62}"
MAX_BYTES="${KS_API_LOG_MAX_BYTES:-33554432}"
# Both clocks are UTC, as nginx's `t` is (the image sets no TZ). The two
# overrides are for the tests.
TODAY="${KS_API_LOG_TODAY:-$(date -u +%Y%m%d)}"
NOW="${KS_API_LOG_NOW:-$(date -u +%Y%m%dT%H%M%SZ)}"
LOG="$DIR/api.log"

[ -d "$DIR" ] || exit 0
# nginx creates the file world-readable. Its paths name what people open, so
# keep the directory to root.
chmod 0750 "$DIR"
if [ -e "$LOG" ]; then chmod 0640 "$LOG"; fi

# A pass stopped half-way leaves a partial copy, while api.log still holds
# every line of it, or a cut that was never compressed.
rm -f "$DIR"/api-*.log.tmp
for cut in "$DIR"/api-*.log; do
    [ -e "$cut" ] || continue
    gzip -f "$cut"
    chmod 0640 "$cut.gz"
done

if [ -s "$LOG" ]; then
    size=$(wc -c < "$LOG")
    # The day of the oldest line: `{"t":"2026-10-29T...` gives 20261029.
    # Empty when the format no longer starts with `t`, and then only the size
    # cuts: a day read as "not today" on every pass would cut every five
    # minutes and spend the whole window in five hours.
    first=$(head -c 16 "$LOG" |
        sed -n 's/^{"t":"\([0-9]\{4\}\)-\([0-9]\{2\}\)-\([0-9]\{2\}\)$/\1\2\3/p')
    if [ "$size" -gt "$MAX_BYTES" ] || { [ -n "$first" ] && [ "$first" -lt "$TODAY" ]; }; then
        cut="$DIR/api-$NOW.log"
        cp "$LOG" "$cut.tmp"
        mv "$cut.tmp" "$cut"
        : > "$LOG"
        gzip -f "$cut"
        chmod 0640 "$cut.gz"
    fi
fi

# The newest KEEP cuts stay. Their names sort in the order they were cut.
ls -1 "$DIR"/api-*.log.gz 2>/dev/null | sort -r | awk -v keep="$KEEP" 'NR > keep' |
    while IFS= read -r old; do rm -f "$old"; done
