# PC LAB 1 (operarios). Lanza un WM_FO.
#
# Ejemplos:
#   .\operario.ps1                                         -> FO-01 interactivo (escribes WS-01#20)
#   .\operario.ps1 -Id FO-02 -NuevaVentana                 -> FO-02 interactivo en otra ventana
#   .\operario.ps1 -Id FO-01 -Fichero servicios-FO-01.txt  -> FO-01 leyendo servicios\servicios-FO-01.txt
#   .\operario.ps1 -Id FO-99                               -> operario que NO existe (debe ser rechazado)
param(
    [string]$Id = "FO-01",
    [string]$Fichero = "",
    [switch]$NuevaVentana
)
. "$PSScriptRoot\config.ps1"

$nombre = $Id.ToLower().Replace("-", "")      # FO-01 -> fo01 (nombre del contenedor)
$cmd = "docker run --rm -it --name $nombre"
$args2 = "$IMG_FO $KAFKA_BROKER $Id"

if ($Fichero -ne "") {
    $ruta = Join-Path $PSScriptRoot "servicios\$Fichero"
    if (-not (Test-Path $ruta)) {
        Write-Host "No existe $ruta" -ForegroundColor Red
        exit 1
    }
    $cmd = "$cmd -v '${ruta}:/app/servicios.txt' $args2 /app/servicios.txt"
} else {
    $cmd = "$cmd $args2"
}

if ($NuevaVentana) {
    $cmd = "`$host.UI.RawUI.WindowTitle = 'OPERARIO $Id'; $cmd"
    Start-Process powershell -ArgumentList '-NoExit', '-Command', $cmd
} else {
    Invoke-Expression $cmd
}
