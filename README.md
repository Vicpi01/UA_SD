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