#!/usr/bin/env bash
set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE="p2pmod-relay"
INSTALL_DIR="${P2P_RELAY_DIR:-/opt/p2pmod-relay}"
PORT="${P2P_RELAY_PORT:-34777}"
RUN_USER="p2prelay"
DO_FIREWALL=1
DO_START=1

say() { printf '\033[36m%s\033[0m\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
Usage: bash relay.sh [command] [options]

Commands:
  run        Run in the foreground (default)
  install    Install as a systemd service and start it (needs root)
  uninstall  Stop and remove the service (needs root)
  start | stop | restart | status
  log        Follow the log
  probe      Check whether a relay port is reachable, e.g.: probe 1.2.3.4:34777
  selftest   Local self-test

Options:
  --port N         UDP port, default 34777
  --dir PATH       Install directory, default /opt/p2pmod-relay
  --no-firewall    Skip opening the port in ufw/firewalld
  --no-start       Do not start automatically after installing
EOF
}

check_port() {
  case "$PORT" in ''|*[!0-9]*) die "port must be a number: $PORT" ;; esac
  [ "$PORT" -ge 1 ] && [ "$PORT" -le 65535 ] || die "port out of range: $PORT"
}

find_jar() {
  local dir="$1" jar
  jar="$(ls "$dir"/p2pmod-*.jar 2>/dev/null | head -1 || true)"
  [ -n "$jar" ] && [ -f "$jar" ] || die "no p2pmod-*.jar found in $dir"
  printf '%s' "$jar"
}

need_lib() {
  [ -d "$1/lib" ] || die "missing $1/lib directory (the slf4j-api and slf4j-simple jars)"
}

need_root() {
  [ "$(id -u)" -eq 0 ] || die "root required: sudo bash $0 $*"
}

need_java() {
  command -v java >/dev/null 2>&1 || die "java not found
Recommended: Microsoft Build of OpenJDK 25:
  curl -LO https://aka.ms/download-jdk/microsoft-jdk-25-linux-x64.tar.gz
  tar xzf microsoft-jdk-25-linux-x64.tar.gz -C /opt
  ln -sf /opt/microsoft-jdk-25*/bin/java /usr/local/bin/java
The distribution's own 21+ works too:
  Debian/Ubuntu: apt install -y openjdk-21-jre-headless
  CentOS/RHEL:   dnf install -y java-21-openjdk-headless"
  local v major
  v="$(java -version 2>&1 | awk -F'"' '/version/ {print $2; exit}')"
  case "$v" in
    1.*) major="$(printf '%s' "$v" | cut -d. -f2)" ;;
    *)   major="$(printf '%s' "$v" | cut -d. -f1)" ;;
  esac
  [ -n "${major:-}" ] || die "could not detect the Java version: $v"
  [ "$major" -ge 21 ] || die "Java 21 or newer is required, currently $v
Recommended: Microsoft Build of OpenJDK 25:
  curl -LO https://aka.ms/download-jdk/microsoft-jdk-25-linux-x64.tar.gz"
  say "Java $v  ->  $(command -v java)"
}

run_foreground() {
  local jar
  jar="$(find_jar "$SELF_DIR")"
  need_lib "$SELF_DIR"
  need_java
  say "Running in the foreground on UDP $PORT (Ctrl+C to stop)"
  exec java -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 \
    -Xms32m -Xmx128m \
    -cp "$jar:$SELF_DIR/lib/*" dev.p2pmod.relay.RelayMain --port "$PORT"
}

do_install() {
  need_root "$@"
  need_java
  local jar jar_name conf pubip
  jar="$(find_jar "$SELF_DIR")"
  need_lib "$SELF_DIR"
  jar_name="$(basename "$jar")"

  say "Installing to $INSTALL_DIR"
  mkdir -p "$INSTALL_DIR/lib" "$INSTALL_DIR/config"
  cp -f "$jar" "$INSTALL_DIR/$jar_name"
  cp -f "$SELF_DIR"/lib/*.jar "$INSTALL_DIR/lib/"
  cp -f "$SELF_DIR/relay.sh" "$INSTALL_DIR/relay.sh" 2>/dev/null || true
  chmod +x "$INSTALL_DIR/relay.sh" 2>/dev/null || true

  conf="$INSTALL_DIR/config/p2pmod.properties"
  if [ -f "$conf" ]; then
    say "Keeping the existing config $conf"
  else
    if [ -f "$SELF_DIR/relay.properties" ]; then
      cp -f "$SELF_DIR/relay.properties" "$conf"
    else
      printf 'relay.port=%s\n' "$PORT" > "$conf"
    fi
    sed -i "s/^relay\.port=.*/relay.port=$PORT/" "$conf"
    say "Config written to $conf"
  fi

  if ! id "$RUN_USER" >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /usr/sbin/nologin "$RUN_USER" 2>/dev/null \
      || useradd --system --no-create-home --shell /sbin/nologin "$RUN_USER" 2>/dev/null || true
  fi
  chown -R "$RUN_USER":"$RUN_USER" "$INSTALL_DIR" 2>/dev/null || true

  say "Writing /etc/systemd/system/$SERVICE.service"
  cat > "/etc/systemd/system/$SERVICE.service" <<EOF
[Unit]
Description=p2pmod UDP relay
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$INSTALL_DIR
ExecStart=$(command -v java) -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 -Xms32m -Xmx128m -cp "$INSTALL_DIR/$jar_name:$INSTALL_DIR/lib/*" dev.p2pmod.relay.RelayMain --port $PORT
Restart=always
RestartSec=3
TimeoutStopSec=8
KillMode=mixed
LimitNOFILE=65535
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable "$SERVICE" >/dev/null 2>&1 || true

  if [ "$DO_FIREWALL" -eq 1 ]; then
    if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -qi active; then
      ufw allow "$PORT"/udp >/dev/null && say "ufw: allowed UDP $PORT"
    elif command -v firewall-cmd >/dev/null 2>&1 && firewall-cmd --state >/dev/null 2>&1; then
      firewall-cmd --permanent --add-port="$PORT"/udp >/dev/null && firewall-cmd --reload >/dev/null \
        && say "firewalld: allowed UDP $PORT"
    else
      say "No active ufw/firewalld detected; if you use iptables/nftables, allow UDP $PORT yourself"
    fi
  else
    say "Skipped the local firewall (remember to open UDP $PORT in the security group or router)"
  fi

  if [ "$DO_START" -eq 1 ]; then
    systemctl restart "$SERVICE"
    sleep 2
    if systemctl is-active --quiet "$SERVICE"; then
      say "Service started"
    else
      printf 'Service did not start, recent log://n' >&2
      journalctl -u "$SERVICE" -n 30 --no-pager >&2 || true
      exit 1
    fi
  else
    say "Installed but not started: systemctl start $SERVICE"
  fi

  pubip="$(curl -fsS --max-time 4 https://ifconfig.me 2>/dev/null || curl -fsS --max-time 4 https://api.ipify.org 2>/dev/null || echo '<your public IP>')"
  cat <<EOF

========================================================
 Installation complete
--------------------------------------------------------
 Directory    $INSTALL_DIR
 Config       $conf
 Port         UDP $PORT
 Public IP    $pubip
--------------------------------------------------------
 Live log     bash $INSTALL_DIR/relay.sh log
 Self-test    bash $INSTALL_DIR/relay.sh selftest
 Remote probe bash $INSTALL_DIR/relay.sh probe $pubip:$PORT
--------------------------------------------------------
 Server registers  /p2p host start $pubip:$PORT <4-char room id>
 Clients connect   p2p:<room id>@$pubip
========================================================

If this machine sits behind a router, also forward UDP $PORT to it on the router;
the cloud security group must allow the same port too -- the local firewall is only one layer.
EOF
}

do_uninstall() {
  need_root "$@"
  systemctl stop "$SERVICE" 2>/dev/null || true
  systemctl disable "$SERVICE" 2>/dev/null || true
  rm -f "/etc/systemd/system/$SERVICE.service"
  systemctl daemon-reload
  say "Service definition removed; data is still in $INSTALL_DIR (rm -rf it manually to delete everything)"
}

svc() {
  local action="$1"
  need_root "$@"
  systemctl "$action" "$SERVICE"
  [ "$action" = "status" ] && return 0
  sleep 1
  systemctl is-active --quiet "$SERVICE" && say "Status: running" || say "Status: not running"
}

do_status() {
  systemctl status "$SERVICE" --no-pager -n 5 || true
}

do_log() {
  journalctl -u "$SERVICE" -f -n 40 --no-pager || true
}

do_probe() {
  local target="$1" jar
  [ -n "$target" ] || die "usage: relay.sh probe <host[:port]>"
  jar="$(find_jar "$SELF_DIR")"
  need_lib "$SELF_DIR"
  need_java
  exec java -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 \
    -cp "$jar:$SELF_DIR/lib/*" dev.p2pmod.relay.RelayMain --probe "$target"
}

do_selftest() {
  local jar
  jar="$(find_jar "$SELF_DIR")"
  need_lib "$SELF_DIR"
  need_java
  exec java -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 \
    -cp "$jar:$SELF_DIR/lib/*" dev.p2pmod.relay.RelayMain --port "$PORT" --selftest
}

CMD="run"
if [ $# -gt 0 ]; then
  CMD="$1"
  shift
fi

while [ $# -gt 0 ]; do
  case "$1" in
    --port) PORT="${2:?--port needs an argument}"; shift 2 ;;
    --dir) INSTALL_DIR="${2:?--dir needs an argument}"; shift 2 ;;
    --no-firewall) DO_FIREWALL=0; shift ;;
    --no-start) DO_START=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) break ;;
  esac
done

check_port

case "$CMD" in
  run)       run_foreground ;;
  install)   do_install "$@" ;;
  uninstall) do_uninstall "$@" ;;
  start|stop|restart) svc "$CMD" "$@" ;;
  status)    do_status ;;
  log)       do_log ;;
  probe)     do_probe "${1:-}" ;;
  selftest)  do_selftest ;;
  -h|--help|help) usage ;;
  *) usage; die "unknown command: $CMD" ;;
esac
