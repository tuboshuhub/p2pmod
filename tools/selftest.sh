#!/usr/bin/env bash
# Runs tools/SelfTest.java against the freshly built jar.
#   ./tools/selftest.sh          (from the repo root, or anywhere)
# Exit code 0 = all checks passed.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

JAR=build/libs/p2pmod-1.0.0.jar
if [ ! -f "$JAR" ]; then
  echo "missing $JAR -- run ./gradlew build first" >&2
  exit 2
fi

# slf4j is only needed because Trace initialises a Logger; any version will do.
SLF4J=$(find "$HOME/.gradle/caches/modules-2/files-2.1/org.slf4j" -name 'slf4j-api-*.jar' 2>/dev/null | head -1)

case "$(uname -s)" in
  CYGWIN*|MINGW*|MSYS*) SEP=';' ;;
  *)                    SEP=':' ;;
esac

# java.exe on Windows does not resolve MSYS paths like /c/Users/..., so convert them.
to_win() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else printf '%s' "$1"; fi
}

CP="$(to_win "$JAR")"
if [ -n "${SLF4J:-}" ]; then
  CP="$CP${SEP}$(to_win "$SLF4J")"
  echo "slf4j: $SLF4J"
fi

OUT=build/selftest
rm -rf "$OUT"
mkdir -p "$OUT"

echo "compiling tools/SelfTest.java"
javac -cp "$CP" -d "$OUT" tools/SelfTest.java || exit 2

echo "running"
java -cp "$(to_win "$OUT")${SEP}$CP" SelfTest
exit $?
