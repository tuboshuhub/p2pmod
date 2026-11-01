#!/usr/bin/env bash
# Runs tools/E2ETest.java: real loopback UDP sockets, real relay, injectable loss.
#   ./tools/e2e.sh          (from the repo root, or anywhere)
# Exit code 0 = all checks passed.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

JAR=build/libs/p2pmod-1.0.0.jar
if [ ! -f "$JAR" ]; then
  echo "missing $JAR -- run ./gradlew build first" >&2
  exit 2
fi

SLF4J=$(find "$HOME/.gradle/caches/modules-2/files-2.1/org.slf4j" -name 'slf4j-api-*.jar' 2>/dev/null | head -1)

case "$(uname -s)" in
  CYGWIN*|MINGW*|MSYS*) SEP=';' ;;
  *)                    SEP=':' ;;
esac

to_win() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else printf '%s' "$1"; fi
}

CP="$(to_win "$JAR")"
if [ -n "${SLF4J:-}" ]; then
  CP="$CP${SEP}$(to_win "$SLF4J")"
fi

OUT=build/e2etest
rm -rf "$OUT"
mkdir -p "$OUT"

echo "compiling tools/E2ETest.java"
javac -cp "$CP" -d "$OUT" tools/E2ETest.java || exit 2

echo "running"
java -cp "$(to_win "$OUT")${SEP}$CP" E2ETest
exit $?
