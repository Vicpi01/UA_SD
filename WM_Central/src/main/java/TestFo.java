import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestFo {
    public static void main(String[] args) throws Exception {
        String broker = args.length > 0 ? args[0] : "localhost:9092";
        String idWS = args.length > 1 ? args[1] : "WS-01";
        String idFO = "FO-01";
        String ser = "org.apache.kafka.common.serialization.StringSerializer";
        String des = "org.apache.kafka.common.serialization.StringDeserializer";

        Properties pc = new Properties();
        pc.put("bootstrap.servers", broker);
        pc.put("group.id", "test-" + System.currentTimeMillis());
        pc.put("key.deserializer", des);
        pc.put("value.deserializer", des);
        pc.put("auto.offset.reset", "latest");
        Properties pp = new Properties();
        pp.put("bootstrap.servers", broker);
        pp.put("key.serializer", ser);
        pp.put("value.serializer", ser);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pc);
             KafkaProducer<String, String> producer = new KafkaProducer<>(pp)) {
            consumer.subscribe(List.of("fo-respuestas"));
            while (consumer.assignment().isEmpty()) {          // espera a estar realmente enganchado
                consumer.poll(Duration.ofMillis(200));
            }
            for (var tp : consumer.assignment()) consumer.position(tp);   // fija el punto de lectura
            
            producer.send(new ProducerRecord<>("fo-peticiones", idFO, idFO + "#" + idWS));
            producer.flush();
            producer.flush();
            System.out.println("Petición enviada: " + idFO + " -> " + idWS);

            long fin = System.currentTimeMillis() + 10000;
            while (System.currentTimeMillis() < fin) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (idFO.equals(r.key())) System.out.println("Respuesta: " + r.value());
                }
            }
        }
    }
}