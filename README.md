# P2_SD_WM
Sistema de riego automático con una centralita y estaciones mediante uso de Sockets y Kafka en Docker.

## Central: Sistema de monitorización de la red
Muestra en detalle el funcionamiento en tiempo real del sistema incluyendo las **estaciones de riego**, las **peticiones de activación** y el **estado del sistema central**.

### Datos que se muestran:
* ID de la estación de riego.
* Ubicación.
* Cantidad de agua suministrada.
* Estado:
    * **Disponible (activada):** Funciona correctamente y está a la espera. Se muestra mediante el color **VERDE**.
    * **Regando:** Se encuentra suministrando agua. Se muestra mediante el color **VERDE PARPADEANTE** y muestra la siguiente informacion:
        * Caudal (L/min).
        * Volumen (L).
        * ID del operario que lo ha activado.
    * **Fuga:** Existe una fuga, por lo que no puede ser activada. Se muestra mediante el color **ROJO**.
    * **Fuera de servicio:** La central bloquea la estación. Se muestra mediante el color **NARANJA**.
    * **Desconectada:** No se puede monitorizar la estación. Se muestra mediante el color **GRIS**.

## Operarios de campo:

## Planificación de la práctica:

### Plan de seguimiento:
Para la realización de la práctica vamos a empezar por hacer la Water Station. Para ello, comenzaremos con el monitor, creando un motor "fake" para probar la conexión. A continuación, haremos la conexión con Kafka y, mediante un productor-consumidor, verificaremos que funciona correctamente.

Una vez hechos eso, implementaremos la Central, pero no completamente. Empezaremos con la conectividad con Kafka y los sockets hacia la WS para poder implementar los operarios y, así, finalmente terminaremos la central.

### Reparto de tareas:








## Protocolo de mensajes (versión inicial)

Reglas generales:
- Todos los mensajes son **texto en una sola línea**, con los campos separados por `#`.
- Los mensajes por **sockets** terminan con salto de línea (`println` / `readLine`).
- Los mensajes por **Kafka** llevan una clave (la que se indica) y el texto como valor.
- Esta es la versión mínima para empezar. Se ampliará con el resto de mensajes (telemetría, fugas, bloqueos).

### 1. Registro de una estación (socket: Monitor → CENTRAL)

El Monitor (`WM_WS_M`) se conecta al puerto de CENTRAL y envía:

    REGISTER#idEstacion#ubicacion

Ejemplo: `REGISTER#WS-01#Parque Central`

CENTRAL responde con una línea:

| Respuesta | Significado |
|---|---|
| `OK` | Estación registrada (o reconocida) y marcada como disponible |
| `KO#motivo` | Rechazada (por ejemplo `KO#formato invalido`) |

### 2. Petición de riego (Kafka: FO → CENTRAL)

| Campo | Valor |
|---|---|
| Topic | `fo-requests` |
| Clave | `idOperario` |
| Valor | `idEstacion` |

Ejemplo: clave `FO-01`, valor `WS-01` (el operario FO-01 pide regar en WS-01).

### 3. Respuesta a la petición (Kafka: CENTRAL → FO)

| Campo | Valor |
|---|---|
| Topic | `fo-notifications` |
| Clave | `idOperario` |
| Valor | `AUTORIZADO#idEstacion` o `DENEGADO#idEstacion#motivo` |

Motivos de denegación: `NO_EXISTE`, `FUGA`, `FUERA_DE_SERVICIO`, `DESCONECTADA`, `YA_REGANDO`.

Ejemplos: `AUTORIZADO#WS-01`, `DENEGADO#WS-02#FUGA`

### Pendiente de definir
- Orden de CENTRAL a la estación para abrir/cerrar la válvula.
- Telemetría (caudal y volumen cada segundo).
- Aviso de fuga y de fuga resuelta.
- Bloqueo/activación de estaciones desde CENTRAL.
- Resumen final del riego al operario.