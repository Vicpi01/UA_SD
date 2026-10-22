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
public class WM_Central {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: WM_Central <puerto> <broker host:puerto>");
            return;
        }
        int puerto = Integer.parseInt(args[0]);
        String broker = args[1];   // todavía no lo usamos, será para Kafka
        System.out.println("Broker Kafka configurado: " + broker);

        initBD();
        cargarEstaciones();
        mostrarPanel();

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
        if (campos[0].equals("FAULT") && campos.length == 2) return cambiarEstado(campos[1], "FUGA");
        if (campos[0].equals("RECOVER") && campos.length == 2) return cambiarEstado(campos[1], "DISPONIBLE");


        return "KO#formato invalido";
    }



    static class Estacion {
    final String id, ubicacion;
    volatile String estado = "DESCONECTADA";
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
            System.out.printf("%-8s %-20s %s%n", e.id, e.ubicacion, e.estado);
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

    
}
