#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
OUT="$ROOT/build/relay-dist"

JAR="$(ls "$ROOT"/build/libs/p2pmod-*.jar 2>/dev/null | head -1 || true)"
[ -n "$JAR" ] || { echo "error: run ./gradlew build -x test first" >&2; exit 1; }
JAR_NAME="$(basename "$JAR")"
VER="${JAR_NAME#p2pmod-}"
VER="${VER%.jar}"

[ -d "$HERE/lib" ] || { echo "error: missing p2pmod-relay/lib (slf4j-api and slf4j-simple)" >&2; exit 1; }

rm -rf "$OUT"
mkdir -p "$OUT"

stage() {
  local os="$1"
  local d="$OUT/p2pmod-relay-$VER-$os"
  mkdir -p "$d/lib"
  cp "$JAR" "$d/"
  cp "$HERE"/lib/*.jar "$d/lib/"
  cp "$HERE/relay.properties" "$d/"
  if [ "$os" = "linux" ]; then
    cp "$HERE/linux/relay.sh" "$d/"
    chmod +x "$d/relay.sh"
  else
    cp "$HERE/windows/relay.bat" "$d/"
    sed -i 's/\r$//' "$d/relay.bat"
    sed -i 's/$/\r/' "$d/relay.bat"
  fi
  echo "$d"
}

LINUX_DIR="$(stage linux)"
WIN_DIR="$(stage windows)"

cd "$OUT"
tar czf "p2pmod-relay-$VER-linux.tar.gz" "p2pmod-relay-$VER-linux"
jar cfM "p2pmod-relay-$VER-windows.zip" -C "$OUT" "p2pmod-relay-$VER-windows"

echo
echo "Done: $OUT"
ls -la "$OUT"/*.tar.gz "$OUT"/*.zip
echo
echo "linux package contents:"
tar tzf "p2pmod-relay-$VER-linux.tar.gz"
echo
echo "windows package contents:"
unzip -l "p2pmod-relay-$VER-windows.zip" 2>/dev/null || jar tf "p2pmod-relay-$VER-windows.zip"
