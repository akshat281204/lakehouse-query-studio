#!/bin/sh
# Fails if the built web bundle or its source could expose object-store or
# database credentials: credential variable names in the bundle, any real value
# from the local .env, or browser storage use in the source.
set -eu
dist="${1:-web/dist}"
src="${2:-web/src}"
[ -d "$dist" ] || { echo "check-bundle: $dist not found (build first)" >&2; exit 1; }

fail=0
if grep -rIlE 'MINIO_(SECRET|ACCESS|ROOT)|POSTGRES_PASSWORD|AWS_(SECRET|ACCESS)|API_TOKEN' "$dist" >/dev/null 2>&1; then
  echo "check-bundle: credential variable name found in $dist" >&2
  fail=1
fi

if [ -f .env ]; then
  for key in MINIO_ROOT_PASSWORD MINIO_SECRET_KEY POSTGRES_PASSWORD API_TOKEN; do
    val="$(sed -n "s/^$key=//p" .env | head -n1)"
    # Ignore empty and short values that would match by accident.
    if [ "${#val}" -ge 8 ] && grep -rIlF -- "$val" "$dist" "$src" >/dev/null 2>&1; then
      echo "check-bundle: value of $key from .env appears in the web bundle or source" >&2
      fail=1
    fi
  done
fi

if grep -rnE '\b(localStorage|sessionStorage|indexedDB)\b' "$src" --include='*.ts' --include='*.tsx' \
   --exclude='*.test.ts' --exclude='*.test.tsx' >/dev/null 2>&1; then
  echo "check-bundle: browser storage used in $src; the app must keep no data there" >&2
  fail=1
fi

[ "$fail" -eq 0 ] && echo "check-bundle: OK"
exit "$fail"
