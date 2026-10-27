import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;



public class WM_Central {
    public static void main(String[] args) throws Exception {
        int puerto = Integer.parseInt(args.length > 0 ? args[0]
                : System.getenv().getOrDefault("CENTRAL_PORT", "7000"));
        String broker = args.length > 1 ? args[1]
                : System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");
        System.out.println("Broker Kafka configurado: " + broker);

        initBD();
        cargarEstaciones();
        mostrarPanel();
        crearTopics(broker);
        iniciarKafka(broker);

        try (ServerSocket servidor = new ServerSocket(puerto)) {
            System.out.println("CENTRAL escuchando en el puerto " + puerto);
            while (true) {
                Socket cliente = servidor.accept();
                new Thread(() -> atender(cliente)).start();
            }
        }
    }

    // Atiende a UN cliente (se ejecuta en su propio hilo)
    static void atender(Socket cliente) {
        String idEstacion = null;
        try (cliente;
             InputStream in = new BufferedInputStream(cliente.getInputStream());
             OutputStream out = cliente.getOutputStream()) {
            
            cliente.setSoTimeout(5000);   // si pasan 5 s sin recibir nada, se corta
            
            String linea;
            while ((linea = Trama.recibir(in, out)) != null) {
                if (!linea.startsWith("PING#")) System.out.println("Recibido: " + linea);
            
                String respuesta = procesar(linea);
            
                if (respuesta.equals("OK")
                        && (linea.startsWith("REGISTER#") || linea.startsWith("AUTH#"))) {
                    idEstacion = linea.split("#")[1];
                }
                Trama.enviar(in, out, respuesta);
            }
        } catch (SocketTimeoutException e) {
            System.out.println("Sin latidos de " + idEstacion + ": se marca como desconectada");
        } catch (IOException e) {
            System.out.println("Conexión cerrada: " + e.getMessage());
        } finally {
            if (idEstacion != null) {
                Estacion est = estaciones.get(idEstacion);
                if (est != null) est.estado = "DESCONECTADA";
                mostrarPanel();
            }
        }
    }

    static final String DB_URL = "jdbc:sqlite:"
        + System.getenv().getOrDefault("DB_PATH", "central.db");
    
    // Crea el fichero central.db y la tabla si no existen
    static void initBD() throws SQLException {
        try (Connection c = DriverManager.getConnection(DB_URL);
             Statement st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS estaciones ("
                     + "id TEXT PRIMARY KEY, "
                     + "ubicacion TEXT NOT NULL)");
        }
    }


    // Guarda la estación; si el id ya existía, actualiza la ubicación
    static boolean registrar(String id, String ubicacion) {
        String sql = "INSERT OR REPLACE INTO estaciones (id, ubicacion) VALUES (?, ?)";
        try (Connection c = DriverManager.getConnection(DB_URL);
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, ubicacion);
            ps.executeUpdate();
            Estacion e = new Estacion(id, ubicacion);
            e.estado = "DISPONIBLE";
            estaciones.put(id, e);
            mostrarPanel();
            return true;
        } catch (SQLException e) {
            System.out.println("Error BD: " + e.getMessage());
            return false;
        }
    }



    // Aquí vivirá la lógica del protocolo
    static String procesar(String mensaje) {
        String[] campos = mensaje.split("#");
        if (campos[0].equals("REGISTER") && campos.length == 3) {
            return registrar(campos[1], campos[2]) ? "OK" : "KO#error de base de datos";
        }
        if (campos[0].equals("AUTH") && campos.length == 2) {
            Estacion e = estaciones.get(campos[1]);
            if (e == null) return "KO#estacion no registrada";
            e.estado = "DISPONIBLE";
            mostrarPanel();
            return "OK";
        }

        if (campos[0].equals("PING") && campos.length == 2) return "OK";
        if (campos[0].equals("FAULT") && campos.length == 2) return fuga(campos[1]);
        if (campos[0].equals("RECOVER") && campos.length == 2) return cambiarEstado(campos[1], "DISPONIBLE");


        return "KO#formato invalido";
    }



    static class Estacion {
        final String id, ubicacion;
        volatile String estado = "DESCONECTADA";
        volatile double caudal = 0, volumen = 0;
        volatile String operario = "-";
        Estacion(String id, String ubicacion) { this.id = id; this.ubicacion = ubicacion; }
    }

    static final Map<String, Estacion> estaciones = new ConcurrentHashMap<>();

    // Lee la BD y deja todas las estaciones como DESCONECTADA
    static void cargarEstaciones() throws SQLException {
        try (Connection c = DriverManager.getConnection(DB_URL);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, ubicacion FROM estaciones")) {
            while (rs.next()) {
                estaciones.put(rs.getString("id"),
                               new Estacion(rs.getString("id"), rs.getString("ubicacion")));
            }
        }
    }

    static void mostrarPanel() {
        System.out.println("=== ESTACIONES ===");
        for (Estacion e : estaciones.values()) {
            String extra = e.estado.equals("REGANDO")
                    ? String.format("%.1f L/min  %.1f L  %s", e.caudal, e.volumen, e.operario) : "";
            System.out.printf("%-8s %-20s %-12s %s%n", e.id, e.ubicacion, e.estado, extra);
        }
        System.out.println("==================");
    }


    static String cambiarEstado(String id, String estado) {
       Estacion e = estaciones.get(id);
       if (e == null) return "KO#estacion no registrada";
       e.estado = estado;
       mostrarPanel();
       return "OK";
    }


    static KafkaProducer<String, String> productor;

    static void enviarKafka(String topic, String clave, String valor) {
        try {
            if (productor != null) productor.send(new ProducerRecord<>(topic, clave, valor));
        } catch (Exception e) {
            System.out.println("Kafka no disponible: " + e.getMessage());
        }
    }

    static Properties propsConsumidor(String broker, String grupo) {
        Properties p = new Properties();
        p.put("bootstrap.servers", broker);
        p.put("group.id", grupo);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("auto.offset.reset", "earliest");
        return p;
    }

    static void iniciarKafka(String broker) {
        Properties pp = new Properties();
        pp.put("bootstrap.servers", broker);
        pp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pp.put("max.block.ms", "3000");
        productor = new KafkaProducer<>(pp);

        new Thread(() -> escuchar(broker, "central", "fo-peticiones", WM_Central::atenderPeticion)).start();
        new Thread(() -> escuchar(broker, "central-estado", "ws-estado", WM_Central::procesarEstado)).start();
    }

    static void escuchar(String broker, String grupo, String topic, java.util.function.Consumer<String> accion) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(propsConsumidor(broker, grupo))) {
            c.subscribe(List.of(topic));
            System.out.println("Kafka: escuchando " + topic);
            while (true) {
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) {
                    try { accion.accept(r.value()); }
                    catch (Exception ex) { System.out.println("Mensaje inválido en " + topic + ": " + r.value()); }
                }
            }
        } catch (Exception ex) {
            System.out.println("Error Kafka (" + topic + "): " + ex.getMessage());
        }
    }


    static final int DURACION_RIEGO = 30;   // segundos, provisional

    static void atenderPeticion(String valor) {
        System.out.println("Petición de riego: " + valor);
        String[] c = valor.split("#");                       // idFO#idWS
        if (c.length != 2) return;
        String idFO = c[0];
        Estacion e = estaciones.get(c[1]);
        String respuesta;
        if (e == null) {
            respuesta = idFO + "#KO#estacion inexistente";
        } else synchronized (e) {
            if (!e.estado.equals("DISPONIBLE")) {
                respuesta = idFO + "#KO#estacion no disponible (" + e.estado + ")";
            } else {
                e.estado = "REGANDO"; e.operario = idFO; e.caudal = 0; e.volumen = 0;
                respuesta = idFO + "#OK#" + e.id;
                enviarKafka("ws-ordenes", e.id, e.id + "#START#" + idFO + "#" + DURACION_RIEGO);
            }
        }
        System.out.println("Respuesta al FO: " + respuesta);
        enviarKafka("fo-respuestas", idFO, respuesta);
        mostrarPanel();
    }

    static void procesarEstado(String valor) {
        System.out.println("Estado recibido: " + valor);
        String[] c = valor.split("#");
        if (c.length < 2) return;
        Estacion e = estaciones.get(c[0]);
        if (e == null) { System.out.println("Ignorado: estación desconocida " + c[0]); return; }
    
        if (c[1].equals("RIEGO") && c.length == 5) {            // idWS#RIEGO#caudal#volumen#idFO
            if (!e.estado.equals("REGANDO")) {
                System.out.println("Ignorado: " + e.id + " no está regando (" + e.estado + ")");
                return;
            }
            e.caudal = Double.parseDouble(c[2]);
            e.volumen = Double.parseDouble(c[3]);
            e.operario = c[4];
            enviarKafka("fo-respuestas", c[4], c[4] + "#RIEGO#" + e.id + "#" + c[2] + "#" + c[3]);
        } else if (c[1].equals("FIN") && c.length == 5) {       // idWS#FIN#volumen#duracion#idFO
            if (e.estado.equals("REGANDO")) e.estado = "DISPONIBLE";
            e.caudal = 0; e.volumen = 0; e.operario = "-";
            enviarKafka("fo-respuestas", c[4], c[4] + "#FIN#" + e.id + "#" + c[2] + "#" + c[3]);
        } else {
            System.out.println("Ignorado: mensaje no reconocido");
            return;
        }
        mostrarPanel();
    }


    static String fuga(String id) {
        Estacion e = estaciones.get(id);
        if (e == null) return "KO#estacion no registrada";
        if (e.estado.equals("REGANDO")) {
            enviarKafka("ws-ordenes", id, id + "#STOP");
            enviarKafka("fo-respuestas", e.operario, e.operario + "#FUGA#" + id);
            System.out.println("Riego cortado por fuga en " + id);
        }
        e.estado = "FUGA";
        e.caudal = 0; e.volumen = 0;
        mostrarPanel();
        return "OK";
    }


    static void crearTopics(String broker) {
        Properties p = new Properties();
        p.put("bootstrap.servers", broker);
        try (AdminClient admin = AdminClient.create(p)) {
            admin.createTopics(List.of(
                    new NewTopic("fo-peticiones", 1, (short) 1),
                    new NewTopic("fo-respuestas", 1, (short) 1),
                    new NewTopic("ws-ordenes", 1, (short) 1),
                    new NewTopic("ws-estado", 1, (short) 1))).all().get();
        } catch (Exception e) {
            System.out.println("Topics: " + e.getMessage());   // si ya existen, no pasa nada
        }
    }



    }
