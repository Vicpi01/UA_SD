<<<<<<< HEAD
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
=======
import java.io.*;
import java.util.*;
import java.nio.*;
>>>>>>> f9ed35dce40f5f5387bf3dd8df076c2109050c75
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
<<<<<<< HEAD
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
=======
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
>>>>>>> f9ed35dce40f5f5387bf3dd8df076c2109050c75

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/*
Aplicación del operario (Field Operator). Pide riegos a CENTRAL por Kafka y muestra cada paso:
autorización o denegación, caudal y volumen en directo, y el resumen final.
Uso: WM_FO <brokerHost:puerto> <idOperario> [ficheroServicios]
Cada línea del fichero (o lo que se escribe por consola) es: idWS#duracionSegundos
*/
public class WM_FO {
    private static final String PETICIONES = "fo-peticiones";
    private static final String RESPUESTAS = "fo-respuestas";
    private static final String LISTA = "ws-lista";
    private static final String SER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String DES = "org.apache.kafka.common.serialization.StringDeserializer";

    private static volatile String ultimaLista = "";     // última lista de estaciones publicada por CENTRAL

    private static Properties propsProductor(String broker) {
        Properties p = new Properties();
        p.put("bootstrap.servers", broker);
<<<<<<< HEAD
        p.put("key.serializer", SER);
        p.put("value.serializer", SER);
        p.put("max.block.ms", "5000");
        return p;
    }

    private static Properties propsConsumidor(String broker, String grupo) {
        Properties p = new Properties();
        p.put("bootstrap.servers", broker);
        p.put("group.id", grupo);
        p.put("key.deserializer", DES);
        p.put("value.deserializer", DES);
        p.put("auto.offset.reset", "latest");
        return p;
    }

    // ---------- Estaciones disponibles ----------

    private static void escucharLista(String broker, String operario) {
        Thread hilo = new Thread(() -> {
            String grupo = "fo-lista-" + operario + "-" + System.currentTimeMillis();
            try (KafkaConsumer<String, String> c = new KafkaConsumer<>(propsConsumidor(broker, grupo))) {
                c.subscribe(List.of(LISTA));
                while (true) {
                    for (ConsumerRecord<String, String> r : c.poll(Duration.ofSeconds(1))) ultimaLista = r.value();
                }
            } catch (Exception e) {
                System.out.println("No se puede leer la lista de estaciones: " + e.getMessage());
            }
        });
        hilo.setDaemon(true);
        hilo.start();
    }

    private static void mostrarLista() {
        String lista = ultimaLista;
        if (lista.isBlank()) {
            System.out.println("Todavía no se ha recibido la lista de estaciones de CENTRAL.");
            return;
        }
        System.out.println("Estaciones (las disponibles llevan *):");
        for (String s : lista.split(";")) {
            String[] f = s.split(",", -1);
            if (f.length < 3) continue;
            System.out.println((f[2].equals("DISPONIBLE") ? "  * " : "    ") + f[0] + "  " + f[1] + "  " + f[2]);
        }
    }

    // ---------- Mensajes recibidos mientras el operario estaba desconectado ----------

    private static void mostrarPendientes(String operario, KafkaConsumer<String, String> consumidor) {
        boolean alguno = false;
        int recibidos;
        do {
            recibidos = 0;
            for (ConsumerRecord<String, String> r : consumidor.poll(Duration.ofMillis(700))) {
                recibidos++;
                if (!operario.equals(r.key())) continue;
                String[] f = r.value().split("#", -1);
                String texto = null;
                if (f.length >= 5 && f[1].equals("FIN")) texto = "Riego finalizado en " + f[2] + ": " + f[3] + " L en " + f[4] + " s";
                else if (f.length >= 3 && (f[1].equals("FUGA") || f[1].equals("CAIDA") || f[1].equals("BLOQUEADA")))
                    texto = "Riego interrumpido en " + f[2] + ": " + motivo(f[1]);
                if (texto != null) {
                    if (!alguno) System.out.println("Mientras estabas desconectado:");
                    alguno = true;
                    System.out.println("  " + texto);
                }
            }
        } while (recibidos > 0);
        consumidor.seekToEnd(consumidor.assignment());          // descarta los RIEGO antiguos
        for (var tp : consumidor.assignment()) consumidor.position(tp);
    }

    private static String motivo(String tipo) {
        return switch (tipo) {
            case "FUGA" -> "fuga detectada en la estación";
            case "CAIDA" -> "la estación perdió la conexión con CENTRAL";
            default -> "estación puesta fuera de servicio por CENTRAL";
        };
    }

    // ---------- Un servicio completo: petición, autorización, riego y resumen ----------

    private static void solicitar(String operario, String ws, long segundos,
                                  KafkaProducer<String, String> productor, KafkaConsumer<String, String> consumidor) {
        String valor = operario + "#" + ws + (segundos > 0 ? "#" + segundos : "");
        try {
            productor.send(new ProducerRecord<>(PETICIONES, operario, valor)).get(8, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.out.println("Imposible conectar con WM_Central: " + e.getMessage());
            return;
        }
        System.out.println("Solicitud enviada a CENTRAL: riego en " + ws + (segundos > 0 ? " durante " + segundos + " s" : ""));

        long inicio = System.nanoTime();
        long ultimoMensaje = inicio;
        boolean autorizado = false;
        boolean interrumpido = false;
        long limiteFin = 0;

        while (true) {
            long ahora = System.nanoTime();
            if (!autorizado && ahora - inicio > Duration.ofSeconds(15).toNanos()) {
                System.out.println("Imposible conectar con WM_Central: no hay respuesta en 15 s");
                return;
            }
            if (autorizado && !interrumpido && ahora - ultimoMensaje > Duration.ofSeconds(20).toNanos()) {
                System.out.println("Sin noticias de la estación desde hace 20 s. El riego puede seguir en curso.");
                return;
            }
            if (interrumpido && ahora > limiteFin) return;

            for (ConsumerRecord<String, String> r : consumidor.poll(Duration.ofSeconds(1))) {
                if (!operario.equals(r.key())) continue;
                String[] f = r.value().split("#", -1);
                if (f.length < 2) continue;
                ultimoMensaje = System.nanoTime();
                switch (f[1]) {
                    case "OK" -> {
                        autorizado = true;
                        System.out.println("CENTRAL ha autorizado el riego en " + (f.length > 2 ? f[2] : ws));
                    }
                    case "KO" -> {
                        System.out.println("CENTRAL ha denegado el riego: " + (f.length > 2 ? f[2] : "sin motivo"));
                        return;
                    }
                    case "RIEGO" -> {
                        if (f.length >= 5) System.out.println("  Regando " + f[2] + ": " + f[3] + " L/min, " + f[4] + " L acumulados");
                    }
                    case "FIN" -> {
                        if (f.length >= 5) System.out.println("Riego finalizado en " + f[2] + ": " + f[3] + " L en " + f[4] + " s");
                        return;
                    }
                    case "FUGA", "CAIDA", "BLOQUEADA" -> {
                        System.out.println("Riego interrumpido en " + ws + ": " + motivo(f[1]));
                        if (!interrumpido) {                      // espera unos segundos el resumen que envía la estación
                            interrumpido = true;
                            limiteFin = System.nanoTime() + Duration.ofSeconds(6).toNanos();
                        }
                    }
                    default -> { }
=======
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("group.id", "fo-" + operatorId + "-" + UUID.randomUUID());
        p.put("auto.offset.reset", "latest");
        KafkaConfig.addSecurity(p);
        return p;
    }

    private static void listarEstaciones(String operatorId, KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer) {
        String requestId = UUID.randomUUID().toString();
        producer.send(new ProducerRecord<>(REQUESTS, operatorId, "LIST#" + requestId));
        producer.flush();

        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : records) {
                if (!operatorId.equals(r.key())) continue;
                String[] f = r.value().split("#", -1);
                if (f.length < 2 || !requestId.equals(f[1])) continue;

                System.out.println("=== ESTADO DE LAS ESTACIONES (WS) ===");
                if (f.length == 2 || (f.length == 3 && f[2].isBlank())) {
                    System.out.println("No hay estaciones registradas en CENTRAL.");
                } else {
                    for (int i = 2; i < f.length; i++) {
                        for (String wsInfo : f[i].split(";")) {
                            if (!wsInfo.isBlank()) {
                                System.out.println(" - " + wsInfo);
                            }
                        }
                    }
                }
                System.out.println("=====================================");
                return;
            }
        }
        System.out.println("No se recibió respuesta de CENTRAL al consultar las estaciones.");
    }

    private static String solicitar(String operatorId, String wsId, long seconds, KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer) {
        String requestId = UUID.randomUUID().toString();
        producer.send(new ProducerRecord<>(REQUESTS, operatorId, "START#" + requestId + "#" + wsId + "#" + seconds));
        producer.flush();
        System.out.println("Solicitud enviada: " + requestId + " para " + wsId + " (" + seconds + "s)");
        
        long startDeadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        long lastMessageTime = System.nanoTime();
        boolean started = false;

        while (true) {
            if (!started && System.nanoTime() > startDeadline) {
                System.out.println("No se recibió inicio ni denegación en 30 segundos.");
                return "TIMEOUT#" + requestId;
            }
            if (started && System.nanoTime() - lastMessageTime > Duration.ofSeconds(15).toNanos() && seconds > 0) {
                System.out.println("Pérdida de comunicación durante el riego.");
                return "TIMEOUT_STREAM#" + requestId;
            }

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> r : records) {
                if (!operatorId.equals(r.key())) continue;
                String[] f = r.value().split("#", -1);
                if (f.length < 2 || !requestId.equals(f[1])) continue;

                lastMessageTime = System.nanoTime();
                System.out.println("CENTRAL: " + r.value());

                String tipo = f[0];
                if (tipo.equals("STARTED")) {
                    started = true;
                }
                // Salimos tanto si termina normal como si se deniega o se corta por FUGA (FAULT/LEAK) o BLOQUEO (BLOCKED/ABORTED)
                if (tipo.equals("DENIED") || tipo.equals("FINISHED") || tipo.equals("FAULT") || tipo.equals("LEAK") || tipo.equals("ABORTED") || tipo.equals("BLOCKED")) {
                    return r.value();
>>>>>>> f9ed35dce40f5f5387bf3dd8df076c2109050c75
                }
            }
        }
    }

<<<<<<< HEAD
    private static void procesarLinea(String linea, String operario,
                                      KafkaProducer<String, String> productor, KafkaConsumer<String, String> consumidor) {
        String[] f = linea.trim().split("#", -1);
        if (f.length > 2 || f[0].isBlank()) {
            System.out.println("Formato: idWS#duracionSegundos (0 = duración por defecto de CENTRAL), LISTA o SALIR");
            return;
        }
        try {
            long segundos = f.length == 2 ? Long.parseLong(f[1].trim()) : 0;
            if (segundos < 0) throw new NumberFormatException();
            solicitar(operario, f[0].trim().toUpperCase(), segundos, productor, consumidor);
=======
    private static void procesarLinea(String linea, String operatorId, KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer) {
        String texto = linea.trim();
        if (texto.equalsIgnoreCase("LIST") || texto.equalsIgnoreCase("DISPONIBLES")) {
            listarDisponibles(operatorId, producer, consumer);
            return;
        }
        String[] f = texto.split("#", -1);
        if (f.length < 1 || f.length > 2 || f[0].isBlank()) {
            System.out.println("Formato: idWS#duracionSegundos (0 = sin límite) o LIST para ver disponibles");
            return;
        }
        try {
            // Si en el fichero o consola solo ponen el ID de la WS (ej. "WS-01"), asume 10s por defecto
            long seconds = (f.length == 2) ? Long.parseLong(f[1]) : 10L;
            if (seconds < 0) throw new NumberFormatException();
            solicitar(operatorId, f[0].trim(), seconds, producer, consumer);
>>>>>>> f9ed35dce40f5f5387bf3dd8df076c2109050c75
        } catch (NumberFormatException e) {
            System.out.println("La duración debe ser un número de segundos.");
        }
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Uso: WM_FO <brokerHost:puerto> <idOperario> [ficheroServicios]");
            return;
        }
        String broker = args[0];
        String operario = args[1];
        if (broker.isBlank() || operario.isBlank() || operario.contains("#")) {
            System.out.println("El broker y el ID del operario deben ser válidos.");
            return;
        }

<<<<<<< HEAD
        // Grupo fijo por operario: al volver tras una caída, Kafka entrega lo que se perdió.
        try (KafkaProducer<String, String> productor = new KafkaProducer<>(propsProductor(broker));
             KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(propsConsumidor(broker, "fo-" + operario));
             BufferedReader consola = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {

            consumidor.subscribe(List.of(RESPUESTAS));
            long limite = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (consumidor.assignment().isEmpty()) {
                if (System.nanoTime() > limite) {
                    System.out.println("Imposible conectar con WM_Central (Kafka en " + broker + ")");
                    return;
                }
                consumidor.poll(Duration.ofMillis(200));
            }
            for (var tp : consumidor.assignment()) consumidor.position(tp);
            System.out.println("Operario " + operario + " conectado a Kafka (" + broker + ")");

            mostrarPendientes(operario, consumidor);
            escucharLista(broker, operario);
            for (int i = 0; i < 12 && ultimaLista.isBlank(); i++) Thread.sleep(500);
            mostrarLista();

            if (args.length == 3) {
                for (String linea : Files.readAllLines(Path.of(args[2]), StandardCharsets.UTF_8)) {
                    if (linea.isBlank() || linea.stripLeading().startsWith("#")) continue;
                    procesarLinea(linea, operario, productor, consumidor);
                    System.out.println("Esperando 4 segundos antes del siguiente servicio...");
                    Thread.sleep(4000);
=======
        Properties p = properties(broker, operatorId);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
             BufferedReader console = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {

            consumer.subscribe(List.of(RESULTS));
            while (consumer.assignment().isEmpty()) consumer.poll(Duration.ofMillis(200));

            if (args.length == 3) {
                Path file = Path.of(args[2]);
                List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i);
                    if (line.isBlank() || line.stripLeading().startsWith("#")) continue;
                    procesarLinea(line, operatorId, producer, consumer);
                    if (i < lines.size() - 1) {
                        Thread.sleep(4000);
                    }
>>>>>>> f9ed35dce40f5f5387bf3dd8df076c2109050c75
                }
                System.out.println("Fichero de servicios completado.");
                return;
            }

<<<<<<< HEAD
            System.out.println("Escribe idWS#duracionSegundos (0 = duración por defecto), LISTA para ver las estaciones o SALIR.");
            String linea;
            while ((linea = consola.readLine()) != null && !linea.equalsIgnoreCase("SALIR")) {
                if (linea.isBlank()) continue;
                if (linea.equalsIgnoreCase("LISTA")) mostrarLista();
                else procesarLinea(linea, operario, productor, consumidor);
=======
            System.out.println("Introduce idWS#duracionSegundos (0 = ilimitada), LIST para ver disponibles o SALIR para terminar.");
            String line;
            while ((line = console.readLine()) != null && !line.equalsIgnoreCase("SALIR")) {
                if (!line.isBlank()) {
                    procesarLinea(line, operatorId, producer, consumer);
                }
>>>>>>> f9ed35dce40f5f5387bf3dd8df076c2109050c75
            }
        } catch (IOException e) {
            System.out.println("No se pudo leer el fichero o la entrada: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            System.out.println("Error Kafka: " + e.getMessage());
        }
    }
}