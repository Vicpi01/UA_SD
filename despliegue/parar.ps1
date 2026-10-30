# Simula un "crash" repentino (docker kill) de un modulo.
#
# Ejemplos:
#   .\parar.ps1 mon1     -> mata el Monitor de WS-01
#   .\parar.ps1 eng2     -> mata el Engine de WS-02
#   .\parar.ps1 fo01     -> mata el operario FO-01
#   .\parar.ps1 todo     -> mata todos los contenedores del proyecto (mon*, eng*, fo*)
#   .\parar.ps1          -> lista los contenedores en marcha
param([string]$Nombre = "")

if ($Nombre -eq "") {
    docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"
    exit 0
}
if ($Nombre -eq "todo") {
    $nombres = docker ps --format "{{.Names}}" | Where-Object { $_ -match '^(mon|eng|fo)' }
    foreach ($n in $nombres) { docker kill $n | Out-Null; Write-Host "Matado: $n" -ForegroundColor Yellow }
    exit 0
}
docker kill $Nombre
