import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.time.Duration;
import org.apache.kafka.clients.*;

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
        p.put("group.id", "fo-" + operatorId);
        p.put("auto.offset.reset", "latest");
        KafkaConfig.addSecurity(p);
        return p;
    }

    private static String solicitar(String operatorId, String wsId, long seconds, KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer) {
        String requestId = UUID.randomUUID().toString();
        producer.send(new ProducerRecord<>(REQUESTS, operatorId, "START#" + requestId + "#" + wsId + "#" + seconds));
        producer.flush();
        System.out.println("Solicitud enviada: " + requestId + " para " + wsId);
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        boolean started = false;
        while (true) {
            if (!started && System.nanoTime() > deadline) {
                System.out.println("No se recibió inicio ni denegación en 30 segundos.");
                return "TIMEOUT#" + requestId;
            }
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> r : records) {
                if (!operatorId.equals(r.key())) continue;
                String[] f = r.value().split("#", -1);
                if (f.length < 2 || !requestId.equals(f[1])) continue;
                System.out.println("CENTRAL: " + r.value());
                if (f[0].equals("STARTED")) started = true;
                if (f[0].equals("DENIED") || f[0].equals("FINISHED")) return r.value();
            }
        }
    }

    private static void procesarLinea(String linea, String operatorId, KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer) {
        String[] f = linea.trim().split("#", -1);
        if (f.length != 2) {
            System.out.println("Formato: idWS#duracionSegundos (0 = sin límite)");
            return;
        }
        try {
            long seconds = Long.parseLong(f[1]);
            if (seconds < 0 || f[0].isBlank()) throw new NumberFormatException();
            solicitar(operatorId, f[0], seconds, producer, consumer);
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
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p); KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p); BufferedReader console = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            consumer.subscribe(List.of(RESULTS));
            while (consumer.assignment().isEmpty()) consumer.poll(Duration.ofMillis(200));
            if (args.length == 3) {
                Path file = Path.of(args[2]);
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (line.isBlank() || line.stripLeading().startsWith("#")) continue;
                    procesarLinea(line, operatorId, producer, consumer);
                    Thread.sleep(4000);
                }
                return;
            }
            System.out.println("Introduce idWS#duracionSegundos; usa 0 para duración ilimitada y SALIR para terminar.");
            String line;
            while ((line = console.readLine()) != null && !line.equalsIgnoreCase("SALIR")) {
                procesarLinea(line, operatorId, producer, consumer);
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