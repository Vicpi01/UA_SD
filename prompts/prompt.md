Uso de IA en la práctica:

Naalamene Farouk:

Durante la práctica he usado Claude, de Anthropic, a través del chat web y del móvil. Lo he tenido como apoyo para planificar, depurar errores y conseguir borradores de código que después he ejecutado, probado y adaptado a mi proyecto. Mi parte ha sido WM_Central, el panel web y el despliegue con Docker de los tres módulos, además de revisar WM_WS y WM_FO, que desarrolla mi compañero. Las conversaciones completas están en la carpeta prompts/conversaciones.

Cómo lo he usado, por etapas

Al empezar le pedí ayuda para organizar la práctica en etapas, empezando por lo más pequeño. Salió un plan que iba de los sockets a SQLite, el protocolo, Kafka y el despliegue, y lo ajusté al reparto de trabajo con mi compañero (CENTRAL, WS y FO).

Con CENTRAL fui paso a paso. Primero un servidor de sockets que atendiera a varios clientes, con un hilo por conexión, y un cliente de prueba. Me falló el proyecto Maven en VS Code y tuve que recargarlo. Después guardé las estaciones registradas en SQLite; el pom.xml estaba incompleto y había una excepción sin declarar, que arreglé, y comprobé las filas con SQLite Viewer. Más tarde pedí cargar las estaciones al arrancar como desconectadas, distinguir el alta (REGISTER) de la autenticación (AUTH), la trama STX, ETX y LRC con ENQ, ACK, NACK y EOT (la clase Trama la compartimos con mi compañero), el latido PING con los avisos FAULT y RECOVER y la desconexión a los cinco segundos sin latidos. También el alta de operarios y el rechazo de los desconocidos, y guardar los riegos en base de datos junto con la lista de estaciones que se publica en ws-lista.

Para Kafka pedí cómo montar un broker local con Docker, los topics y el productor y el consumidor en CENTRAL, y después definir el contrato de mensajes del riego (START, RIEGO, FIN y STOP) con los topics fo-peticiones, fo-respuestas, ws-ordenes y ws-estado. En las pruebas vi que mi cliente de prueba enviaba mensajes antes de tener la partición asignada, lo corregí, y probé también el Kafka de Railway a través del proxy TCP. Comprobé una fuga durante un riego y la caída del Monitor.

El panel web también lo pedí con ayuda: una interfaz con colores por estado, botones y un registro de mensajes, con un servidor HTTP integrado. Le añadí yo el botón de parar y la duración configurable, y lo probé en local y en Railway.

Con Docker pedí parametrizar los módulos con argumentos y variables de entorno y empaquetarlos con el plugin shade y un Dockerfile de dos fases. Se me descargó mal una imagen de Maven y tuve que borrarla y bajarla de nuevo, y ajusté el nombre del jar. En Railway tuve problemas con los permisos de GitHub, porque el repositorio es de mi compañero, y con un despliegue antiguo que seguía activo.

Más cerca de la demostración le pasé a la IA el enunciado y la guía de corrección. Con eso me ayudó a adaptar el Engine y el FO al contrato de Kafka, y a empaquetar WM_FO y WM_WS: un pom.xml con shade, un Dockerfile de dos fases, el .dockerignore y el simplelogger.properties. Como no hay pom.xml en la raíz del repositorio, WM_WS lleva el suyo, sin clase principal porque tiene dos programas, el Monitor y el Engine, y se elige cuál arrancar al ejecutar la imagen. El FO de mi compañero usaba otros topics y formatos que mi CENTRAL, así que lo adapté al contrato (fo-peticiones, fo-respuestas y ws-lista) y añadí que muestre las estaciones y que recupere los mensajes que se pierden mientras el operario está desconectado.

Al revisar el código antes de la demostración salieron dos fallos en WM_Central.java. En atenderPeticion habían quedado dos líneas del método procesarEstado que lanzaban una excepción porque usaban un campo que no existe en la petición, así que el operario nunca recibía la autorización. Y en crearTopics el topic ws-lista había quedado fuera de la lista. Los entendí y los corregí a mano.

Para los ordenadores de la universidad pedí ayuda con el despliegue. Me dieron una carpeta con scripts de PowerShell que lanzan y paran las estaciones y los operarios con un solo comando, ficheros de servicios de ejemplo y un plan con unas veinte pruebas. Probando entre ordenadores vi que el Engine no encontraba al Monitor cuando estaban en equipos distintos, porque el nombre mon1 solo se resuelve dentro de la red de Docker de un mismo ordenador. Lo resolví dejando el Monitor y el Engine de cada estación en el mismo equipo, y para repartirlos usamos la dirección IP del equipo del Monitor con un parámetro que se añadió al script. También le pregunté qué hace exactamente cada módulo y qué debía pasar cuando cae uno, y me explicó qué cambios hacían falta en WM_WS_E para cumplir la guía: que la estación aparezca como averiada y rechace riegos cuando pierde al Monitor, y que guarde en disco el riego en curso para enviar el resumen final si el Engine se cae y se recupera. Por último me preparó una guía impresa para el día de la corrección, con el arranque y la forma de reconectar un Monitor o un Engine que se cierre.

Frases que usé en algunos de los prompts

Te paso ahora el enunciado de la práctica y la guía de corrección de esta misma, por tenerlo en cuenta y por algún detalle que hemos hecho últimamente en el último código y para tener en cuenta en el futuro para seguir.

¿Puedes ayudarme a hacerlo para probarlos en los ordenadores de la universidad? ¿Qué archivos nuevos debo crear y cómo lo hago para el día de la demostración?

¿Cómo puedo volver a conectar esos monitores caídos? ¿Cómo puedo tener varios operarios, intercambiarlos entre estaciones o probar con operarios inexistentes?

¿Qué hace exactamente el Monitor y el Engine y para qué sirven? ¿Qué pasa si cae alguno de ellos? ¿Qué debería salir?


Lo que puedo explicar de lo que se hizo con apoyo de la IA

Los pom.xml añaden las dependencias de Kafka y el plugin shade, que mete todo en un único jar ejecutable. Los Dockerfile usan dos etapas, una que compila con Maven y otra que solo ejecuta con el JRE. El FO usa un productor y un consumidor de Kafka con un grupo fijo por operario, y por eso al volver tras una caída recibe lo que se perdió; además lee ws-lista para enseñar las estaciones. En el Engine, una variable pasa a verdadero cuando falla la conexión con el Monitor y el estado cambia a averiada; el riego se guarda cada segundo en un volumen de Docker y, al reconectar, el Engine lee ese fichero, publica el resumen final en ws-estado y lo borra. Los scripts de PowerShell solo automatizan los comandos docker run de cada módulo con sus argumentos y no contienen lógica de la práctica.

Víctor Ramos:

En mi caso, he utilizado ChatGPT como herramienta de apoyo en todo el desarrollo de la práctica. Como mi parte del proyecto ha sido la Water Station, con su Monitor y Engine, y el Field Operator, voy a detallar el uso de la IA separado por las partes que he realizado.

Pero antes de ello, al principio pedí ayuda para entender por dónde empezar con una práctica de este tamaño. Como no estoy acostumbrado a realizar proyectos tan grandes y desde cero, y, aunque conocía de forma general los sockets y Kafka, no tenía claro como convertir el enunciado en programas independientes y como repartir las responsabiliaddes ni cómo organizar el desarrollo.

La IA me ayudó a separar el sistema de en sus principales componentes y a enteder el papel que desempeña cada uno. Es por eso, que pedí una hoja de ruta del proyecto para saber cómo abordarlo.

También me gustaría destacar que los comentarios que tenía me los modificó Codex porque quiso y no los he cambiado, se han quedado tal y como me los corrigió.

Water Station Monitor:

Una parte fundamental del uso de la IA, fue la de entender el uso de hilos. Partimos del código de la primera práctica, la que era para entender el código y sobre ella empecé a trabajar. El problema principal que encontré es que no sabía como poder realizar ambas comprobaciones de manera simultánea, la del Monitor con el Engine y la del Monitor con la Central, para ello se usan hilos, como bien se explicó en clase, pero no tenía ni idea de definirlos e implementarlos, aunque la idea la tenía. Por eso le pedí que los implementase y los explicara para saber cómo se trabaja con ellos. Con esto, conseguí ejecutar dos hilos a la vez para comprobar de manera simultánea las conexiones entre los módulos antes mencionados.

Otro uso muy importante de la IA fueron la corrección de errores, momentos en los que no sabía qué fallaba exactamente porque, en cuanto a lógica estaba bien pero se me olvidaba añadir alguna comprobación y ese tipo de cosas.

Water Station Engine:

El caso del Engine es más o menos el mismo que del Monitor, corrección de errores; sin embargo, como el uso de Kafka es algo con lo que no estaba muy familiarizado, por no decir nada, le pedí mucha ayuda. En primer lugar, le pedí los argumentos necesarios en relación al Broker. Después, necesité ayuda para saber cómo publicar o suscribirse en distintos topics los mensajes que debían de recibirse o enviarse.

En este caso, hubo las mismas dudas que con el Monitor en cuanto a los Threads y, con ayuda de la IA y siguiendo la estructura que se usa con ellos en el Monitor, se consiguió implementar para leer los comandos por teclado, consultar continuamente los topics de Kafka e ir haciendo el Health_Check con el Monitor de manera simultánea.

Además, se le pidió a la IA que pusiera una serie de comandos para probar la comunicación entre el Monitor y el Engine, a parte del KO y del OK, la función de regar con x cantidad de litros y parar el riego. Con ello se podía observar que la conexión estaba bien hecha y se transmitía entre dos powershells en el mismo ordenador. La implementación no tiene ningún misterio porque se trata de un switch case, pero por ahorro de tiempo más que nada.

Field Operator:

La IA me ayudó a entender el papel de un productor y un consumidor de Kafka y cómo encajar el envío de solicitudes y la recepción de notificaciones dentro de la aplicación del operario. También la utilicé para revisar la organización del código y la relación entre la entrada del usuario, el envío de la petición y la respuesta que recibe desde Central.

Y, como se puede intuir, también ha sido necesario la IA para verificar el código y corregirlo en caso de que sea necesario.

Revisión e integración de mi parte del proyecto:

A medida que avanzaba, fui pasando fragmentos de mi código y describiendo qué quería conseguir para que ChatGPT señalara problemas o propusiera correcciones. Una dificultad recurrente fue que algunas de la solucionan que proponía usaba estructuras o conceptos que van más allá del alcance de la asignatura y tuve que ir guiándola para obtener el resultado esperado.

También recurrí a la IA para entender mejor cómo se configuran los proyectos Java con Maven y cómo se añaden las dependencias necesarias para trabajar con Kafka. En los proyectos WM_WS y WM_Fo tenemos archivos pom.xml donde se incluyen Kafka Clients y SLF4J, además del plugin Maven Shade, que permite generar un JAR con las dependencias necesarias. En el caso de WM_FO, también se configura la clase WM_FO como punto de entrada de la aplicación.

Varios de los prompts utilizados (pedido a la IA para que me recopile una serie de mensajes del chat que teníamos):

- «Te paso el enunciado de la práctica. Quiero que me diseñes una hoja de ruta para saber como abarcarla, po dónde empezar y por qué hacerlo de esa manera».
- «¿Qué atributos debería tener el Engine y cuáles el Monitor?».
- «¿El socket que utiliza el Monitor para conectarse al Engine necesita el host o solamente el puerto?».
- «¿Cómo puede el Monitor mantener la conexión con el Engine y con CENTRAL al mismo tiempo?».
- «Quiero que el Monitor compruebe el Engine cada segundo mientras sigue recibiendo mensajes de CENTRAL. ¿Cómo lo puedo implementar?».
- «Analiza el código que he hecho y dime qué está mal».

Básicamente, la IA ha sido una ayuda importante y se ha utilizado de forma bastante amplia, especialmente para obtener ejemplos y borradores de código y para resolver dudas sobre sockets, Kafka y la organización de los módulos. Soy consciente de que el uso de ella puede ser perjudicial si se usa sin control. Por eso he intentado revisar las respuestas, adaptarlas a la estructura del proyecto y pedir explicaciones de los cambios, en lugar de limitarme a copiar el código sin entenderlo.

Aun así, el uso de IA no significa que domine automáticamente todo lo que aparece en el proyecto. Hay partes que he tenido que estudiar más y otras que debo seguir repasando. La responsabilidad de entender el funcionamiento de los módulos, comprobar que se integran correctamente y poder explicar o modificar el código sigue siendo mía.