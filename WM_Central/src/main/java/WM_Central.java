import java.io.*;
import java.net.*;

public class WM_Central {
    public static void main(String[] args) throws IOException {
        // Si no se pasa puerto, usamos 7000 para poder probar fácilmente
        int puerto = args.length > 0 ? Integer.parseInt(args[0]) : 7000;

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

    // Aquí vivirá la lógica del protocolo
    static String procesar(String mensaje) {
        String[] campos = mensaje.split("#");
        if (campos[0].equals("REGISTER") && campos.length == 3) {
            return "OK";
        }
        return "KO#formato invalido";
    }
}
