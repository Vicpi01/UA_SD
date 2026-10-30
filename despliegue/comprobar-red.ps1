# EJECUTAR EN CADA PC DE LA UNIVERSIDAD ANTES DE NADA.
# Comprueba que Docker funciona y que el PC alcanza CENTRAL y Kafka en Railway.
. "$PSScriptRoot\config.ps1"
$fallos = 0

Write-Host "== Docker ==" -ForegroundColor Cyan
docker version --format "Cliente {{.Client.Version}}  Servidor {{.Server.Version}}"
if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker no responde. Abre Docker Desktop y espera a que este en verde." -ForegroundColor Red
    $fallos++
}

Write-Host "`n== Red hacia Railway ==" -ForegroundColor Cyan
$kHost = $KAFKA_BROKER.Split(":")[0]
$kPort = [int]$KAFKA_BROKER.Split(":")[1]
$destinos = @(
    @{ n = "CENTRAL (sockets)"; h = $CENTRAL_HOST; p = $CENTRAL_PORT },
    @{ n = "KAFKA";             h = $kHost;        p = $kPort }
)
foreach ($d in $destinos) {
    $r = Test-NetConnection $d.h -Port $d.p -WarningAction SilentlyContinue
    if ($r.TcpTestSucceeded) {
        Write-Host ("OK     {0,-18} {1}:{2}" -f $d.n, $d.h, $d.p) -ForegroundColor Green
    } else {
        Write-Host ("FALLO  {0,-18} {1}:{2}" -f $d.n, $d.h, $d.p) -ForegroundColor Red
        $fallos++
    }
}

Write-Host "`n== Imagenes ==" -ForegroundColor Cyan
docker images | Select-String "REPOSITORY|wm-"

if ($fallos -eq 0) { Write-Host "`nTODO CORRECTO" -ForegroundColor Green }
else { Write-Host "`n$fallos problema(s). Si es la red, avisa al profesor ANTES de seguir." -ForegroundColor Red }
