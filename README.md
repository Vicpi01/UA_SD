# WaterManagement

Práctica de Sistemas Distribuidos, curso 2026-2027, Universidad de Alicante.

WaterManagement es un sistema distribuido que controla el riego de varias estaciones desde un servidor central. Los operarios piden riegos desde sus terminales y siguen el riego en directo.

## Autores

- Naalamene Farouk
- Víctor Ramos Latorre

Grupo de prácticas: [grupo]. Los datos completos están en Nombres.txt y en la memoria.

## Estructura del repositorio

 WM_Central (Servidor central): sockets con los Monitores, Kafka, base de datos SQLite y panel web 

 WM_WS (Estación): Monitor (WM_WS_M) y Engine (WM_WS_E) en un mismo proyecto 
 WM_FO:  Aplicación del operario (Field Operator) 
 despliegue: Scripts de PowerShell y guías para lanzar todo con Docker 
 prompts : Registro del uso de IA y conversaciones 
 Nombres.txt: Nombres de los integrantes del grupo 

## Cómo funciona

- Los Monitores hablan con CENTRAL por sockets TCP con tramas del tipo STX, texto, ETX y LRC (con ENQ, ACK, NACK y EOT).
- Los Engines y los operarios hablan con CENTRAL a través de Kafka, con los topics fo-peticiones, fo-respuestas, ws-ordenes, ws-estado y ws-lista.
- CENTRAL guarda estaciones, operarios y riegos en SQLite.
- El panel web de CENTRAL muestra el estado de las estaciones y permite regar, parar, bloquear y activar.

## Despliegue

Todo se despliega con Docker, sin entorno de desarrollo.

**Nube (Railway):** WM_Central (con su panel web y SQLite) y Kafka. CENTRAL usa un volumen montado en /data y estas variables de entorno: KAFKA_BROKER, OPERARIOS, DURACION_RIEGO, DB_PATH, CENTRAL_PORT y PORT.

**Laboratorio:** un ordenador con las estaciones (Monitor y Engine) y otro con los operarios (WM_FO).

Pasos resumidos, desde la carpeta despliegue de cada ordenador y con Docker Desktop abierto:

1. Comprobar la red y cargar las imágenes:
   ```
   .\comprobar-red.ps1
   .\cargar-imagenes.ps1
   ```
2. En el ordenador de las estaciones:
   ```
   .\estaciones.ps1 -Cantidad 3
   ```
   También se puede lanzar una estación nueva con `.\estaciones.ps1 -Desde 4 -Ubicacion "Plaza Mayor"`.
3. En el ordenador de los operarios:
   ```
   .\operario.ps1 -Id FO-01 -NuevaVentana
   ```
4. Para parar o reconectar un módulo: `.\parar.ps1 <nombre>` y volver a lanzarlo con el mismo script.

La guía detallada está en la memoria y en la carpeta despliegue.



## Reparto del trabajo
 
WM_Central: Farouk  
WM_WS: Víctor  
WM_FO: Víctor  
despliegue: Frouk  

## Notas

- Las estaciones se dan de alta solas la primera vez que su Monitor se conecta. Una estación que se para pasa a desconectada, pero sigue registrada.
- La lista de operarios válidos se configura con la variable OPERARIOS de Railway.