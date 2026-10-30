# Configuracion comun a todos los scripts. Si Railway cambia host o puerto, se cambia AQUI y en ningun otro sitio.
$KAFKA_BROKER = "tramway.proxy.rlwy.net:45381"
$CENTRAL_HOST = "switchyard.proxy.rlwy.net"
$CENTRAL_PORT = 20512

$RED    = "redws"     # red Docker donde viven Monitor y Engine de las estaciones
$IMG_WS = "wm-ws"
$IMG_FO = "wm-fo"

# Ubicaciones que se asignan a WS-01, WS-02, ... (sin tildes a proposito)
$UBICACIONES = @("Parque Central", "Jardines del Rio", "Plaza Norte", "Parque Sur",
                 "Rotonda Este", "Jardin Botanico", "Parque de la Paz", "Plaza Mayor")
