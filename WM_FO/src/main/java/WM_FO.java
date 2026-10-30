import java.io.*;
import java.util.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

public class WM_FO {
    private static final String REQUESTS = "fo-requests";
    private static final String RESULTS = "fo-results";

    private static Properties properties(String broker, String operatorId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", broker);
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
                }
            }
        }
    }

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
        } catch (NumberFormatException e) {
            System.out.println("ID o duración no válidos.");
        }
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Uso: WM_FO <brokerHost:puerto> <idOperario> [ficheroSolicitudes]");
            return;
        }
        String broker = args[0];
        String operatorId = args[1];
        if (broker.isBlank() || operatorId.isBlank() || operatorId.contains("#")) {
            System.out.println("Broker e ID de operario deben ser válidos.");
            return;
        }

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
                }
                return;
            }

            System.out.println("Introduce idWS#duracionSegundos (0 = ilimitada), LIST para ver disponibles o SALIR para terminar.");
            String line;
            while ((line = console.readLine()) != null && !line.equalsIgnoreCase("SALIR")) {
                if (!line.isBlank()) {
                    procesarLinea(line, operatorId, producer, consumer);
                }
            }
        } catch (IOException e) {
            System.out.println("No se pudo leer la entrada: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            System.out.println("Error Kafka: " + e.getMessage());
        }
    }
}