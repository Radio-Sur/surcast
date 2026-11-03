#!/usr/bin/env bash
set -euo pipefail

# Generate markdown coverage comment for PR
# Works with lcov.info from cargo-llvm-cov and frontend vitest v8

FRONTEND_LCOV="frontend/coverage/lcov.info"
BACKEND_LCOV="coverage/lcov.backend.info"
[ -f "$BACKEND_LCOV" ] || BACKEND_LCOV="coverage/lcov.info"

backend_pct="N/A"
frontend_pct="N/A"
total_pct="N/A"
total_found=0
total_hit=0

if [ -f "$BACKEND_LCOV" ]; then
  # Parse lcov for backend lines coverage: LF (lines found) and LH (lines hit)
  lf=$(awk -F: '/^LF:/ {sum+=$2} END {print sum+0}' "$BACKEND_LCOV")
  lh=$(awk -F: '/^LH:/ {sum+=$2} END {print sum+0}' "$BACKEND_LCOV")
  if [ -n "$lf" ] && [ "$lf" -gt 0 ] 2>/dev/null; then
    backend_pct=$(awk "BEGIN {printf \"%.1f\", ($lh/$lf)*100}")
    total_found=$((total_found + lf))
    total_hit=$((total_hit + lh))
  fi
fi

if [ -f "$FRONTEND_LCOV" ]; then
  lf=$(awk -F: '/^LF:/ {sum+=$2} END {print sum+0}' "$FRONTEND_LCOV")
  lh=$(awk -F: '/^LH:/ {sum+=$2} END {print sum+0}' "$FRONTEND_LCOV")
  if [ -n "$lf" ] && [ "$lf" -gt 0 ] 2>/dev/null; then
    frontend_pct=$(awk "BEGIN {printf \"%.1f\", ($lh/$lf)*100}")
    total_found=$((total_found + lf))
    total_hit=$((total_hit + lh))
  fi
fi

if [ "$total_found" -gt 0 ]; then
  total_pct=$(awk "BEGIN {printf \"%.1f\", ($total_hit/$total_found)*100}")
fi

cat <<EOF
### Coverage report

| Flag | Coverage |
|------|----------|
| backend | ${backend_pct}% |
| frontend | ${frontend_pct}% |
| **total** | **${total_pct}%** |

<details>
<summary>Artifacts</summary>

- \`backend: coverage/lcov.backend.info\`
- \`frontend: frontend/coverage/lcov.info\`
- HTML: \`coverage/html/{backend,frontend}\` (published to gh-pages on main)

</details>

[View full HTML on gh-pages](https://radio-sur.github.io/surcast/coverage/) • [backend](https://radio-sur.github.io/surcast/coverage/backend/) • [frontend](https://radio-sur.github.io/surcast/coverage/frontend/) • Generated at $(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF
