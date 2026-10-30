#!/bin/sh
# Variables: CENTRAL_HOST, CENTRAL_PORT, WS_ID, WS_UBICACION, KAFKA_BROKER, MONITOR_HOST, MONITOR_PORT
case "$1" in
  monitor) exec java -cp app.jar WM_WS_M "${MONITOR_PORT:-7101}" "$CENTRAL_HOST" "$CENTRAL_PORT" "$WS_ID" "$WS_UBICACION" ;;
  engine)  exec java -cp app.jar WM_WS_E "$KAFKA_BROKER" "$MONITOR_HOST" "${MONITOR_PORT:-7101}" ;;
  *) echo "Uso: monitor | engine"; exit 1 ;;
esac