#!/usr/bin/env bash
set -euo pipefail

# Generate markdown coverage comment for PR
# Works with lcov.info from cargo-llvm-cov and frontend vitest v8

FRONTEND_LCOV="frontend/coverage/lcov.info"
BACKEND_LCOV="coverage/lcov.info"
FRONTEND_JSON="frontend/coverage/coverage-final.json"

backend_pct="N/A"
frontend_pct="N/A"
total_pct="N/A"

if [ -f "$BACKEND_LCOV" ]; then
  # Parse lcov for backend lines coverage: LF (lines found) and LH (lines hit)
  lf=$(grep -E "^LF:" "$BACKEND_LCOV" | awk -F: '{sum+=$2} END {print sum}')
  lh=$(grep -E "^LH:" "$BACKEND_LCOV" | awk -F: '{sum+=$2} END {print sum}')
  if [ -n "$lf" ] && [ "$lf" -gt 0 ] 2>/dev/null; then
    backend_pct=$(awk "BEGIN {printf \"%.1f\", ($lh/$lf)*100}")
  fi
fi

if [ -f "$FRONTEND_JSON" ]; then
  frontend_pct=$(jq -r '[.[] | .lines.pct] | add / length' "$FRONTEND_JSON" 2>/dev/null | xargs printf "%.1f" 2>/dev/null || echo "N/A")
elif [ -f "$FRONTEND_LCOV" ]; then
  lf=$(grep -E "^LF:" "$FRONTEND_LCOV" | awk -F: '{sum+=$2} END {print sum}')
  lh=$(grep -E "^LH:" "$FRONTEND_LCOV" | awk -F: '{sum+=$2} END {print sum}')
  if [ -n "$lf" ] && [ "$lf" -gt 0 ] 2>/dev/null; then
    frontend_pct=$(awk "BEGIN {printf \"%.1f\", ($lh/$lf)*100}")
  fi
fi

# Total
if [ "$backend_pct" != "N/A" ] && [ "$frontend_pct" != "N/A" ]; then
  total_pct=$(awk "BEGIN {printf \"%.1f\", ($backend_pct+$frontend_pct)/2}")
elif [ "$backend_pct" != "N/A" ]; then
  total_pct="$backend_pct"
elif [ "$frontend_pct" != "N/A" ]; then
  total_pct="$frontend_pct"
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

- \`backend: coverage/lcov.info\`
- \`frontend: frontend/coverage/lcov.info\`
- HTML: \`coverage/html\` (published to gh-pages on main)

</details>

[View full HTML on gh-pages](https://radio-sur.github.io/surcast/coverage/html/) • Generated at $(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF
