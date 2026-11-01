#!/usr/bin/env bash
# Runs every test suite. Exit code 0 only if all of them pass.
#   ./tools/test.sh
#
#   selftest.sh : logic-level checks inside one JVM (Mux, Receiver, Sender, Path, STUN)
#   e2e.sh      : real loopback UDP sockets through a real relay, with injected loss
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

./tools/selftest.sh
S1=$?
echo
./tools/e2e.sh
S2=$?

echo
if [ $S1 -eq 0 ] && [ $S2 -eq 0 ]; then
  echo "ALL TEST SUITES PASSED"
  exit 0
fi
echo "SOME TEST SUITES FAILED (selftest=$S1 e2e=$S2)"
exit 1
