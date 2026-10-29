import java.io.*;
import java.net.*;

public class TestRegisterClient {
    // Vuelta del bucle (1 vuelta = 1 s) en la que simula una fuga y su recuperación. -1 = no simular.
    static final int FUGA_EN = 60;
    static final int RECUPERA_EN = 70;

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int puerto = args.length > 1 ? Integer.parseInt(args[1]) : 7000;

        try (Socket s = new Socket(host, puerto);
             InputStream in = new BufferedInputStream(s.getInputStream());
             OutputStream out = s.getOutputStream()) {

            System.out.println("Conectado a CENTRAL en " + host + ":" + puerto);
            Trama.iniciar(in, out);

            String[] mensajes = {
                "REGISTER#WS-01#Parque Central",
                "AUTH#WS-01",
                "AUTH#WS-99",      // debe dar KO: no registrada
                "HOLA",            // debe dar KO: formato inválido
                "PING#WS-01",
                "FAULT#WS-01",
                "RECOVER#WS-01"
            };
            for (String m : mensajes) {
                Trama.enviar(in, out, m);
                System.out.println(m + " -> " + Trama.recibir(in, out));
            }

            System.out.println("Enviando PING cada segundo (Ctrl+C para terminar)...");
            for (int i = 0; i < 300; i++) {
                Trama.enviar(in, out, "PING#WS-01");
                if (Trama.recibir(in, out) == null) throw new IOException("CENTRAL cerró la conexión");

                if (i == FUGA_EN) {
                    Trama.enviar(in, out, "FAULT#WS-01");
                    System.out.println("FAULT -> " + Trama.recibir(in, out));
                }
                if (i == RECUPERA_EN) {
                    Trama.enviar(in, out, "RECOVER#WS-01");
                    System.out.println("RECOVER -> " + Trama.recibir(in, out));
                }
                Thread.sleep(1000);
            }
        } catch (IOException e) {
            System.out.println("Imposible conectar con WM_Central: " + e.getMessage());
        }
    }
}