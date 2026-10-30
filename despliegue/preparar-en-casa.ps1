# EJECUTAR EN CASA (con Docker Desktop abierto) antes de ir a la universidad.
# Construye las imagenes wm-ws y wm-fo y las guarda en despliegue\imagenes\*.tar para llevarlas en un pendrive.
. "$PSScriptRoot\config.ps1"

$raiz = Split-Path $PSScriptRoot -Parent
Set-Location $raiz
Write-Host "Repositorio: $raiz" -ForegroundColor Cyan

Write-Host "`n[1/4] Construyendo $IMG_WS ..." -ForegroundColor Cyan
docker build -t $IMG_WS ./WM_WS
if ($LASTEXITCODE -ne 0) { Write-Host "FALLO al construir $IMG_WS" -ForegroundColor Red; exit 1 }

Write-Host "`n[2/4] Construyendo $IMG_FO ..." -ForegroundColor Cyan
docker build -t $IMG_FO ./WM_FO
if ($LASTEXITCODE -ne 0) { Write-Host "FALLO al construir $IMG_FO" -ForegroundColor Red; exit 1 }

New-Item -ItemType Directory -Force "$PSScriptRoot\imagenes" | Out-Null

Write-Host "`n[3/4] Guardando $IMG_WS en imagenes\wm-ws.tar ..." -ForegroundColor Cyan
docker save -o "$PSScriptRoot\imagenes\wm-ws.tar" $IMG_WS
if ($LASTEXITCODE -ne 0) { Write-Host "FALLO al guardar $IMG_WS" -ForegroundColor Red; exit 1 }

Write-Host "`n[4/4] Guardando $IMG_FO en imagenes\wm-fo.tar ..." -ForegroundColor Cyan
docker save -o "$PSScriptRoot\imagenes\wm-fo.tar" $IMG_FO
if ($LASTEXITCODE -ne 0) { Write-Host "FALLO al guardar $IMG_FO" -ForegroundColor Red; exit 1 }

Write-Host "`nLISTO. Copia la carpeta 'despliegue' ENTERA (con imagenes\) a un pendrive." -ForegroundColor Green
Get-ChildItem "$PSScriptRoot\imagenes" | Format-Table Name, @{n="MB";e={[math]::Round($_.Length/1MB,1)}}
