# EJECUTAR EN CADA PC DE LA UNIVERSIDAD (una sola vez). Carga las imagenes .tar que haya en despliegue\imagenes.
# PC LAB 2 solo necesita wm-ws.tar; PC LAB 1 solo necesita wm-fo.tar (pero no pasa nada por cargar las dos).
$tars = Get-ChildItem "$PSScriptRoot\imagenes\*.tar" -ErrorAction SilentlyContinue
if (-not $tars) {
    Write-Host "No hay ningun .tar en $PSScriptRoot\imagenes" -ForegroundColor Red
    Write-Host "Alternativa con internet: clona el repositorio y ejecuta  docker build -t wm-ws ./WM_WS   y   docker build -t wm-fo ./WM_FO"
    exit 1
}
foreach ($f in $tars) {
    Write-Host "Cargando $($f.Name) ..." -ForegroundColor Cyan
    docker load -i $f.FullName
}
Write-Host "`nImagenes disponibles:" -ForegroundColor Green
docker images | Select-String "REPOSITORY|wm-"
