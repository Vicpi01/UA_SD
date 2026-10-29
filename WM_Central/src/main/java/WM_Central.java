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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;




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
        iniciarWeb();
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
        }finally {
            if (idEstacion != null) {
                Estacion est = estaciones.get(idEstacion);
                if (est != null) {
                    if (est.estado.equals("REGANDO")) {
                        String fo = est.operario;
                        evento("Monitor caído durante un riego en " + idEstacion + ": se corta el riego");
                        enviarKafka("ws-ordenes", idEstacion, idEstacion + "#STOP");
                        enviarKafka("fo-respuestas", fo, fo + "#CAIDA#" + idEstacion);
                    }
                    est.estado = "DESCONECTADA";
                    est.caudal = 0; est.volumen = 0; est.operario = "-";
                }
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

            st.execute("CREATE TABLE IF NOT EXISTS operarios (id TEXT PRIMARY KEY)");
            st.execute("DELETE FROM operarios");
            st.execute("CREATE TABLE IF NOT EXISTS riegos (id INTEGER PRIMARY KEY AUTOINCREMENT, estacion TEXT, operario TEXT, "
                + "inicio TEXT, fin TEXT, duracion_prevista INTEGER, duracion INTEGER, volumen REAL, estado TEXT)");
                st.execute("UPDATE riegos SET estado = 'INTERRUMPIDO', fin = datetime('now') WHERE estado = 'EN_CURSO'");

            try (PreparedStatement ps = c.prepareStatement("INSERT OR IGNORE INTO operarios (id) VALUES (?)")) {
                for (String op : System.getenv().getOrDefault("OPERARIOS",
                        "FO-01,FO-02,FO-03,FO-04,FO-05,FO-06,FO-07,FO-08,FO-09,FO-10").split(",")) {
                    ps.setString(1, op.trim());
                    ps.executeUpdate();
                }
            }
            
            
        
            st.execute("CREATE TABLE IF NOT EXISTS operarios (id TEXT PRIMARY KEY)");
            try (PreparedStatement ps = c.prepareStatement("INSERT OR IGNORE INTO operarios (id) VALUES (?)")) {
                for (String op : System.getenv().getOrDefault("OPERARIOS", "FO-01,FO-02,FO-03").split(",")) {
                    ps.setString(1, op.trim());
                    ps.executeUpdate();
                }
            }
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
        ScheduledExecutorService reloj = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r); t.setDaemon(true); return t; });
        reloj.scheduleAtFixedRate(() -> enviarKafka("ws-lista", "estado", listaTexto()), 2, 5, TimeUnit.SECONDS);
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


    static final int DURACION_RIEGO = Integer.parseInt(System.getenv().getOrDefault("DURACION_RIEGO", "30"));

    static void atenderPeticion(String valor) {
        evento("Petición de riego: " + valor);
        String[] c = valor.split("#");                        // idFO#idWS   o   idFO#idWS#segundos
        if (c.length < 2 || c.length > 3) return;
        String idFO = c[0];

        if (!idFO.equals("CENTRAL") && !operarioExiste(idFO)) {
            evento("Operario desconocido: " + idFO);
            enviarKafka("fo-respuestas", idFO, idFO + "#KO#operario desconocido");
            return;
        }

        int duracion = DURACION_RIEGO;
        if (c.length == 3) {
            try { duracion = Integer.parseInt(c[2].trim()); } catch (NumberFormatException ex) { /* se queda la duración por defecto */ }
        }

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
                enviarKafka("ws-ordenes", e.id, e.id + "#START#" + idFO + "#" + duracion);
                registrarFinRiego(e.id, Double.parseDouble(c[2]), Integer.parseInt(c[3]));
                evento("Riego finalizado en " + e.id + ": " + c[2] + " L en " + c[3] + " s (" + c[4] + ")");
            }
        }
        evento("Respuesta al FO: " + respuesta);
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
            registrarFinRiego(e.id, Double.parseDouble(c[2]), Integer.parseInt(c[3]));
            evento("Riego finalizado en " + e.id + ": " + c[2] + " L en " + c[3] + " s (" + c[4] + ")");
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
            evento("Riego cortado por fuga en " + id);
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
                    new NewTopic("ws-lista", 1, (short) 1);
        } catch (Exception e) {
            System.out.println("Topics: " + e.getMessage());   // si ya existen, no pasa nada
        }
    }


    static boolean operarioExiste(String id) {
        try (Connection c = DriverManager.getConnection(DB_URL);
             PreparedStatement ps = c.prepareStatement("SELECT 1 FROM operarios WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        } catch (SQLException e) {
            System.out.println("Error BD: " + e.getMessage());
            return false;
        }
    }




    static final ConcurrentLinkedDeque<String> eventos = new ConcurrentLinkedDeque<>();

    static void evento(String texto) {
        String linea = java.time.LocalTime.now().withNano(0) + "  " + texto;
        System.out.println(linea);
        eventos.addFirst(linea);
        while (eventos.size() > 30) eventos.removeLast();
    }


    static boolean aplicable(String accion, Estacion e) {
        return switch (accion) {
            case "START" -> e.estado.equals("DISPONIBLE");
            case "STOP" -> e.estado.equals("REGANDO");
            case "BLOCK" -> !e.estado.equals("DESCONECTADA") && !e.estado.equals("FUERA_DE_SERVICIO");
            case "ACTIVATE" -> e.estado.equals("FUERA_DE_SERVICIO");
            default -> false;
        };
    }

    
    static String ordenar(String accion, String ws, String segundos) {
        List<Estacion> objetivo = new ArrayList<>();
        if (ws.equals("ALL")) {
            objetivo.addAll(estaciones.values());
        } else {
            Estacion e = estaciones.get(ws);
            if (e == null) return "KO#estacion inexistente";
            objetivo.add(e);
        }
        for (Estacion e : objetivo) {
            if (ws.equals("ALL") && !aplicable(accion, e)) continue;
            switch (accion) {
                case "START" -> atenderPeticion("CENTRAL#" + e.id + (segundos.matches("\\d+") ? "#" + segundos : ""));
                case "STOP" -> parar(e);
                case "BLOCK" -> bloquear(e);
                case "ACTIVATE" -> activar(e);
                default -> { return "KO#accion desconocida"; }
            }
        }
        return "OK";
    }

    static void bloquear(Estacion e) {
        if (e.estado.equals("DESCONECTADA")) { evento("No se puede bloquear " + e.id + ": está desconectada"); return; }
        if (e.estado.equals("REGANDO")) {
            String fo = e.operario;
            enviarKafka("ws-ordenes", e.id, e.id + "#STOP");
            enviarKafka("fo-respuestas", fo, fo + "#BLOQUEADA#" + e.id);
        }
        e.estado = "FUERA_DE_SERVICIO";
        e.caudal = 0; e.volumen = 0; e.operario = "-";
        enviarKafka("ws-ordenes", e.id, e.id + "#BLOCK");
        evento(e.id + " puesta Fuera de servicio por CENTRAL");
        mostrarPanel();
    }

    static void activar(Estacion e) {
        if (e.estado.equals("FUERA_DE_SERVICIO")) {
            e.estado = "DISPONIBLE";
            enviarKafka("ws-ordenes", e.id, e.id + "#ACTIVATE");
            evento(e.id + " activada por CENTRAL");
            mostrarPanel();
        } else {
            evento(e.id + ": no se puede activar (estado " + e.estado + ")");
        }
    }



    static void iniciarWeb() throws IOException {
        int puerto = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        HttpServer http = HttpServer.create(new InetSocketAddress(puerto), 0);
        http.createContext("/", ex -> responder(ex, 200, "text/html; charset=utf-8", leerRecurso("/panel.html")));
        http.createContext("/api/estado", ex ->
                responder(ex, 200, "application/json; charset=utf-8", json().getBytes(StandardCharsets.UTF_8)));
        http.createContext("/api/orden", ex -> {
            if (!ex.getRequestMethod().equals("POST")) { responder(ex, 405, "text/plain", "POST".getBytes()); return; }
            Map<String, String> q = consulta(ex.getRequestURI().getRawQuery());
            String r = ordenar(q.getOrDefault("accion", ""), q.getOrDefault("ws", ""), q.getOrDefault("segundos", ""));
            responder(ex, r.equals("OK") ? 200 : 400, "text/plain; charset=utf-8", r.getBytes(StandardCharsets.UTF_8));
        });
        http.setExecutor(Executors.newCachedThreadPool());
        http.start();
        System.out.println("Panel web en el puerto " + puerto);
    }

    static void responder(HttpExchange ex, int codigo, String tipo, byte[] cuerpo) throws IOException {
        ex.getResponseHeaders().set("Content-Type", tipo);
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(codigo, cuerpo.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(cuerpo); }
    }
    
    static byte[] leerRecurso(String ruta) throws IOException {
        try (InputStream in = WM_Central.class.getResourceAsStream(ruta)) {
            return in == null ? "Falta panel.html".getBytes(StandardCharsets.UTF_8) : in.readAllBytes();
        }
    }
    
    static Map<String, String> consulta(String q) {
        Map<String, String> m = new HashMap<>();
        if (q == null) return m;
        for (String par : q.split("&")) {
            String[] kv = par.split("=", 2);
            if (kv.length == 2) m.put(kv[0], java.net.URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
        }
        return m;
    }

    static void parar(Estacion e) {
        if (!e.estado.equals("REGANDO")) { evento("No hay ningún riego que parar en " + e.id); return; }
        enviarKafka("ws-ordenes", e.id, e.id + "#STOP");
        evento("Riego de " + e.id + " detenido por CENTRAL");
        e.estado = "DISPONIBLE"; e.caudal = 0; e.volumen = 0; e.operario = "-";
        mostrarPanel();
    }
    
    static String esc(String s) { return s.replace("\\", "\\\\").replace("\"", "\\\""); }
    
    static String json() {
        StringBuilder sb = new StringBuilder("{\"estaciones\":[");
        boolean primero = true;
        for (Estacion e : new TreeMap<>(estaciones).values()) {
            if (!primero) sb.append(',');
            primero = false;
            sb.append("{\"id\":\"").append(esc(e.id)).append("\",\"ubicacion\":\"").append(esc(e.ubicacion))
              .append("\",\"estado\":\"").append(e.estado)
              .append("\",\"caudal\":").append(String.format(Locale.US, "%.1f", e.caudal))
              .append(",\"volumen\":").append(String.format(Locale.US, "%.1f", e.volumen))
              .append(",\"operario\":\"").append(esc(e.operario)).append("\"}");
        }
        sb.append("],\"eventos\":[");
        primero = true;
        for (String ev : eventos) {
            if (!primero) sb.append(',');
            primero = false;
            sb.append('"').append(esc(ev)).append('"');
        }
        return sb.append("]}").toString();
    }

    static void registrarInicioRiego(String ws, String operario, int prevista) {
        String sql = "INSERT INTO riegos (estacion, operario, inicio, duracion_prevista, estado) "
                   + "VALUES (?, ?, datetime('now'), ?, 'EN_CURSO')";
        try (Connection c = DriverManager.getConnection(DB_URL); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, ws); ps.setString(2, operario); ps.setInt(3, prevista);
            ps.executeUpdate();
        } catch (SQLException ex) { System.out.println("Error BD: " + ex.getMessage()); }
    }

    static void registrarFinRiego(String ws, double volumen, int duracion) {
        String sql = "UPDATE riegos SET fin = datetime('now'), volumen = ?, duracion = ?, estado = 'FINALIZADO' "
                   + "WHERE id = (SELECT id FROM riegos WHERE estacion = ? AND estado = 'EN_CURSO' ORDER BY id DESC LIMIT 1)";
        try (Connection c = DriverManager.getConnection(DB_URL); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setDouble(1, volumen); ps.setInt(2, duracion); ps.setString(3, ws);
            ps.executeUpdate();
        } catch (SQLException ex) { System.out.println("Error BD: " + ex.getMessage()); }
    }

    static String listaTexto() {
        StringBuilder sb = new StringBuilder();
        for (Estacion e : new TreeMap<>(estaciones).values()) {
            if (sb.length() > 0) sb.append(';');
            sb.append(e.id).append(',').append(e.ubicacion).append(',').append(e.estado);
        }
        return sb.toString();
    }


    }
