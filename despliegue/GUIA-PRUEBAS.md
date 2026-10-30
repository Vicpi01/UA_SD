# Guía de pruebas en la universidad (ensayo + día de la demostración)

## 0. Qué se monta y dónde

```
   PC LAB 1 (operarios)         RAILWAY (ya desplegado)          PC LAB 2 (estaciones)
   ─────────────────────        ───────────────────────          ─────────────────────
   WM_FO  FO-01  ──┐            WM_Central + SQLite              Monitor WS-01 + Engine WS-01
   WM_FO  FO-02  ──┼─ Kafka ──► Kafka                  ◄─ Kafka ─ Monitor WS-02 + Engine WS-02
                   │                 ▲                           Monitor WS-03 + Engine WS-03
                   └── (solo Kafka)  └──── sockets (solo Monitor) ┘
```

- Los **FO** solo hablan con Kafka. Los **Monitor** hablan por sockets con CENTRAL; los **Engine** hablan con Kafka y con su Monitor.
- En el PC LAB 2, Monitor y Engine de una estación se ven por la red Docker `redws` (el Engine usa el nombre `monN` como host).
- Un solo PC puede hacer de LAB 1 y LAB 2 a la vez (así ensayas en casa), pero **en la corrección tienen que ser dos equipos distintos**.

## 1. Archivos nuevos (carpeta `UA_SD\despliegue`)

| Archivo | Para qué | Dónde se ejecuta |
|---|---|---|
| `config.ps1` | Broker Kafka, host/puerto de CENTRAL, nombres de imagen. **Único sitio donde se cambia un puerto de Railway.** | (lo leen los demás) |
| `preparar-en-casa.ps1` | Construye `wm-ws` y `wm-fo` y los guarda como `.tar` | Tu PC, en casa |
| `comprobar-red.ps1` | Docker + conexión a CENTRAL y Kafka + imágenes | Cada PC de la uni |
| `cargar-imagenes.ps1` | `docker load` de los `.tar` | Cada PC de la uni |
| `estaciones.ps1` | Lanza Monitor + Engine de una o varias estaciones (una ventana cada uno) | PC LAB 2 |
| `operario.ps1` | Lanza un FO (interactivo o con fichero) | PC LAB 1 |
| `parar.ps1` | `docker kill` de un módulo (simula un crash) | Donde corra el módulo |
| `servicios\*.txt` | Ficheros de servicios para los FO | PC LAB 1 |
| `imagenes\` | Aquí caen los `.tar` (no se suben a GitHub) | — |

Copia la carpeta dentro de `UA_SD`, haz `commit` y `push`. Es útil para la memoria (guía de despliegue).

> Si PowerShell dice que la ejecución de scripts está deshabilitada, abre PowerShell y ejecuta **una vez**: `Set-ExecutionPolicy -Scope Process Bypass`. Solo vale para esa ventana. Si el equipo no te deja, lanza cada script con `powershell -ExecutionPolicy Bypass -File .\estaciones.ps1`.

## 2. En casa, antes de ir

1. Railway: CENTRAL en verde, volumen montado en `/data`, panel web abre. Anota la URL del panel.
2. Abre Docker Desktop. En PowerShell:
   ```powershell
   cd $HOME\OneDrive\Desktop\WM_SD\UA_SD\despliegue
   .\preparar-en-casa.ps1
   ```
3. **Ensayo general en tu PC** (sirve de LAB 1 y LAB 2 a la vez):
   ```powershell
   .\comprobar-red.ps1
   .\estaciones.ps1 -Cantidad 3      # WS-01..03: salen 6 ventanas
   .\operario.ps1 -Id FO-01          # en esta ventana
   .\operario.ps1 -Id FO-02 -NuevaVentana
   ```
   Pasa las pruebas de la sección 4. Cuando acabes: `.\parar.ps1 todo`.
4. Copia **toda** la carpeta `despliegue` (con `imagenes\*.tar`) a un pendrive. Lleva también el cargador del portátil por si acaso.

## 3. En la universidad: puesta en marcha

**En los dos PC** (abre PowerShell dentro de `despliegue`):

```powershell
.\comprobar-red.ps1     # Docker OK + CENTRAL OK + KAFKA OK. Si falla la red: avisa al profesor ya.
.\cargar-imagenes.ps1   # una sola vez
```

**PC LAB 2 – estaciones:**
```powershell
.\estaciones.ps1 -Cantidad 3
```
Cada Monitor debe decir `Monitor autenticado en CENTRAL: WS-0N` y el Engine `Conectado al Monitor`. En el panel de Railway las tres pasan a verde.

**PC LAB 1 – operarios** (una ventana por operario):
```powershell
.\operario.ps1 -Id FO-01
.\operario.ps1 -Id FO-02 -NuevaVentana
```
Cada FO debe mostrar `Operario FO-0N conectado a Kafka` y la lista de estaciones con `*` en las disponibles.

## 4. Plan de pruebas (en este orden)

Marca cada una. La última columna es el apartado de la guía de corrección que cubre.

| # | Qué haces | Qué debes ver | Apartado |
|---|---|---|---|
| 1 | Arranque limpio (sección 3) | Panel: WS-01..03 verdes. Antes de arrancar las estaciones, las de la BD aparecían **grises** | Funcionamiento base |
| 2 | FO-01: `WS-01#20` | Solicitud → autorizado → litros en directo (FO, Engine y panel con verde parpadeante, caudal, volumen, operario) → resumen a los 20 s → WS vuelve a verde | Base |
| 3 | FO-01: `WS-99#10` | Denegado: estación inexistente | Base |
| 4 | `.\operario.ps1 -Id FO-99` y pide `WS-01#5` | Denegado: operario desconocido | Base |
| 5 | FO-01: `WS-02#30` y, mientras riega, FO-02: `WS-02#10` | FO-02 denegado: estación no disponible (REGANDO) | Base |
| 6 | FO-01 `WS-01#20` y FO-02 `WS-03#20` a la vez | Cada FO solo ve los mensajes de **su** estación; ambos ven la lista completa con `LISTA` | Operarios |
| 7 | `.\operario.ps1 -Id FO-01 -Fichero servicios-FO-01.txt` | Hace el 1.º, espera 4 s, hace el 2.º... "Fichero de servicios completado" | Fichero |
| 8 | `.\operario.ps1 -Id FO-02 -Fichero servicios-errores.txt` | Denegaciones y formato inválido, pero **sigue** con el siguiente | Fichero |
| 9 | En la ventana ENGINE WS-01: `R FO-01 15` | Riego manual desde la propia estación, autorizado por CENTRAL | Riego desde WS |
| 10 | Panel web: iniciar, parar, bloquear (naranja "Fuera de servicio") y activar WS-01; luego bloquear/activar `ALL` | Cada acción se refleja en panel, Engine y FO | Órdenes de CENTRAL |
| 11 | FO-01 `WS-01#40`; a los 8 s escribe `KO` en el Engine | Riego cortado al instante, FO avisa de fuga, WS **roja** en el panel y en el Monitor. `OK` → vuelve a verde | Fuga |
| 12 | `KO` con la WS sin regar, luego FO pide riego en ella | Denegado (estado FUGA) | Fuga |
| 13 | FO-01 `WS-01#40`, a los 8 s `.\parar.ps1 fo01`; espera a que acabe y relanza `.\operario.ps1 -Id FO-01` | El riego **sigue**. Al volver: "Mientras estabas desconectado: Riego finalizado en WS-01…" | Resiliencia FO |
| 14 | FO-01 `WS-01#40`, a los 8 s `.\parar.ps1 mon1` | WS-01 **gris** en ≤5 s; el FO recibe "la estación perdió la conexión". Relanza `.\estaciones.ps1 -Desde 1 -Solo Monitor` → vuelve a verde sola | Resiliencia Monitor |
| 15 | FO-01 `WS-01#40`, a los 8 s `.\parar.ps1 eng1` | WS **roja**, FO avisado. Relanza `.\estaciones.ps1 -Desde 1 -Solo Engine` → verde | Resiliencia Engine |
| 16 | FO-01 `WS-02#40`; en Railway **reinicia WM_Central** | El riego continúa en la estación. Estaciones grises mientras CENTRAL está caída y vuelven solas. Un FO que pida riego ve "Imposible conectar con WM_Central: no hay respuesta en 15 s" | Resiliencia CENTRAL |
| 17 | Tras el reinicio de CENTRAL | Las estaciones registradas aparecen en el panel (grises) hasta que reconectan | Persistencia BD |
| 18 | En caliente: `.\estaciones.ps1 -Desde 4` y `.\operario.ps1 -Id FO-03 -NuevaVentana` | WS-04 se registra sola y FO-03 funciona sin tocar nada más | Escalabilidad |

Haz capturas de las pruebas 2, 7, 10, 11, 13, 14 y 16: son el material de la memoria.

## 5. Si algo falla

| Síntoma | Causa probable y arreglo |
|---|---|
| `comprobar-red` da FALLO en CENTRAL o Kafka | Red de la uni bloquea el puerto. Prueba con el móvil como punto de acceso; avisa al profesor |
| `Test-NetConnection` OK pero el Monitor dice "No se pudo conectar con CENTRAL" | Railway cambió el puerto. Copia el nuevo en `config.ps1` |
| Engine: `No se pudo conectar con el Monitor` | El Monitor aún no está listo, o `monN` no existe. Se reintenta cada 2 s; si persiste, comprueba `.\parar.ps1` (lista) y que ambos usan la red `redws` |
| FO: "Imposible conectar con WM_Central (Kafka en …)" | Kafka inaccesible o el puerto cambió |
| FO sin lista de estaciones | CENTRAL tarda ~5 s en publicarla; espera y escribe `LISTA` |
| `docker run` dice que el nombre ya existe | Quedó un contenedor: `docker rm -f mon1` (o `.\parar.ps1 todo`) |
| Las ventanas no se abren con Start-Process | Lanza a mano los `docker run` (ver `estaciones.ps1`, son una línea cada uno) |
| No hay Docker en el PC del lab | Avisa al profesor: el despliegue con Docker es obligatorio |

## 6. Día de la demostración (chuleta)

**Llevar:** pendrive con `despliegue\` + `.tar`, la guía de corrección impresa a doble cara con la columna **E.** rellena, el acceso a GitHub (el profesor ya añadido) y la memoria.

**Antes de que llegue el profesor (10 min):** CENTRAL en verde en Railway; panel abierto y proyectado; `comprobar-red` + `cargar-imagenes` en los dos PC; todo en `despliegue` sin compilar nada (la guía dice que no se puede recompilar).

**Orden sugerido:**
1. Enseña el panel con las estaciones **grises** (BD) → lanza `estaciones.ps1 -Cantidad 3` → pasan a verde.
2. Lanza los FO → riego a mano (prueba 2) y con fichero (7).
3. Órdenes desde el panel (10) y fuga (11).
4. Crashes que pida el profesor: FO (13), Monitor (14), Engine (15), CENTRAL (16). Cada una tiene un solo comando.
5. Si pide una instancia nueva: prueba 18.

**Tienes que saber explicar:** qué hace cada topic (`fo-peticiones`, `fo-respuestas`, `ws-ordenes`, `ws-estado`, `ws-lista`), por qué el FO usa un grupo de consumidor fijo (recupera lo perdido), cómo detecta CENTRAL que un Monitor cae (timeout de 5 s sin latidos) y cómo se calcula el LRC de la trama. El profesor puede preguntar por cualquier línea y la guía avisa de que no entender el código suspende.

**Prompts de IA:** deben estar en la carpeta `prompts` del repositorio.
