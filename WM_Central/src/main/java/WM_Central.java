import java.io.*;
import java.net.*;
import java.sql.*;

public class WM_Central {
    public static void main(String[] args) throws Exception {
        // Si no se pasa puerto, usamos 7000 para poder probar fácilmente
        int puerto = args.length > 0 ? Integer.parseInt(args[0]) : 7000;
        initBD();

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
        try (cliente;
             BufferedReader in = new BufferedReader(new InputStreamReader(cliente.getInputStream()));
             PrintWriter out = new PrintWriter(cliente.getOutputStream(), true)) {

            String linea;
            while ((linea = in.readLine()) != null) {
                System.out.println("Recibido: " + linea);
                out.println(procesar(linea));
            }
        } catch (IOException e) {
            System.out.println("Conexión cerrada: " + e.getMessage());
        }
    }

    static final String DB_URL = "jdbc:sqlite:central.db";
    
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
        return "KO#formato invalido";
    }

    
}
