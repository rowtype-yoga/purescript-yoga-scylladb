#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PASS=0
FAIL=0
ERRORS=()

for f in compile-fail/*.purs; do
  name=$(basename "$f" .purs)
  # Extract expected error pattern from first line comment: -- expect: <pattern>
  expected=$(head -1 "$f" | sed -n 's/^-- expect: //p')

  output=$(eval "purs compile 'src/**/*.purs' '.spago/p/*/src/**/*.purs' '$f' --output output-compile-fail" 2>&1 || true)

  if echo "$output" | grep -q "^Error found:"; then
    if [ -n "$expected" ]; then
      if echo "$output" | grep -q "$expected"; then
        printf "  \033[32m✓\033[0m %s  \033[2m(%s)\033[0m\n" "$name" "$expected"
        ((PASS++))
      else
        ERRORS+=("WRONG ERROR: $name — expected '$expected' but got different error")
        ((FAIL++))
      fi
    else
      printf "  \033[32m✓\033[0m %s correctly rejected\n" "$name"
      ((PASS++))
    fi
  else
    ERRORS+=("UNEXPECTED PASS: $name compiled but should have failed")
    ((FAIL++))
  fi
done

rm -rf output-compile-fail

echo ""
echo "Compile-fail: $PASS passed, $FAIL failed"

for err in "${ERRORS[@]+"${ERRORS[@]}"}"; do
  printf "  \033[31m✗ %s\033[0m\n" "$err"
done

[ "$FAIL" -eq 0 ]
