# Lanza el Monitor y/o el Engine de una o varias estaciones, cada uno en su propia ventana.
#
# MODO NORMAL (Monitor y Engine en el MISMO PC, el que pide el enunciado):
#   .\estaciones.ps1                          -> WS-01 (Monitor + Engine)
#   .\estaciones.ps1 -Cantidad 3              -> WS-01, WS-02 y WS-03
#   .\estaciones.ps1 -Desde 4                 -> solo WS-04 (estacion nueva en caliente)
#   .\estaciones.ps1 -Desde 1 -Solo Monitor   -> relanza solo el Monitor de WS-01 (tras matarlo)
#   .\estaciones.ps1 -Desde 1 -Solo Engine    -> relanza solo el Engine de WS-01 (tras matarlo)
#
# MODO REPARTIDO (Monitor en un PC y Engine en otro):
#   PC A (Monitor):  .\estaciones.ps1 -Cantidad 3 -Solo Monitor        (publica los puertos 7001, 7002...)
#   PC B (Engine):   .\estaciones.ps1 -Cantidad 3 -Solo Engine -HostMonitor 10.20.30.40   (IP del PC A)
param(
    [int]$Desde = 1,
    [int]$Cantidad = 1,
    [ValidateSet("Ambos", "Monitor", "Engine")][string]$Solo = "Ambos",
    [string]$HostMonitor = ""          # vacio = mismo PC (se usa el nombre monN de la red Docker)
)
. "$PSScriptRoot\config.ps1"

if ($HostMonitor -ne "" -and $Solo -eq "Ambos") {
    Write-Host "Con -HostMonitor usa -Solo Engine (el Monitor esta en el otro PC)." -ForegroundColor Red
    exit 1
}

docker network inspect $RED *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Creando la red Docker '$RED' ..." -ForegroundColor Cyan
    docker network create $RED | Out-Null
}

for ($i = $Desde; $i -lt ($Desde + $Cantidad); $i++) {
    $id     = "WS-{0:D2}" -f $i
    $puerto = 7000 + $i
    $mon    = "mon$i"
    $eng    = "eng$i"
    $ubi    = $UBICACIONES[($i - 1) % $UBICACIONES.Count]

    if ($Solo -ne "Engine") {
        # -p publica el puerto del Monitor hacia fuera del PC (necesario solo en modo repartido, no molesta en modo normal)
        $cmd = "`$host.UI.RawUI.WindowTitle = 'MONITOR $id'; docker run --rm -it --name $mon --network $RED -p ${puerto}:${puerto} $IMG_WS WM_WS_M $puerto $CENTRAL_HOST $CENTRAL_PORT $id '$ubi'"
        Start-Process powershell -ArgumentList '-NoExit', '-Command', $cmd
        Write-Host "Monitor de $id lanzado ($mon, puerto $puerto, '$ubi')" -ForegroundColor Green
        Start-Sleep -Seconds 3
    }
    if ($Solo -ne "Monitor") {
        if ($HostMonitor -eq "") {
            $destino = $mon
            $red = "--network $RED"
        } else {
            $destino = $HostMonitor
            $red = ""
        }
        # -v wm-engN:/data = volumen donde el Engine guarda el riego en curso; sobrevive a un crash (docker kill / cerrar ventana)
        $cmd = "`$host.UI.RawUI.WindowTitle = 'ENGINE $id'; docker run --rm -it --name $eng $red -v wm-${eng}:/data $IMG_WS WM_WS_E $KAFKA_BROKER $destino $puerto"
        Start-Process powershell -ArgumentList '-NoExit', '-Command', $cmd
        Write-Host "Engine de $id lanzado ($eng -> ${destino}:${puerto})" -ForegroundColor Green
        Start-Sleep -Seconds 2
    }
}
Write-Host "`nComandos en la ventana ENGINE:  R [operario] [segundos] = regar | P = parar | KO = fuga | OK = recuperar | ESTADO" -ForegroundColor Yellow
