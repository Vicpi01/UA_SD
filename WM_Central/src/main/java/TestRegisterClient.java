import java.io.*;
import java.net.*;

public class TestRegisterClient {
    public static void main(String[] args) throws Exception {
        try (Socket s = new Socket("localhost", 7000);
             InputStream in = new BufferedInputStream(s.getInputStream());
             OutputStream out = s.getOutputStream()) {

            Trama.iniciar(in, out);

            String[] mensajes = {
                "REGISTER#WS-01#Parque Central",
                "AUTH#WS-01",
                "AUTH#WS-99",      // debe dar KO: no registrada
                "HOLA",            // debe dar KO: formato inválido
                "PING#WS-01",      // latido
                "FAULT#WS-01",     // pasa a FUGA
                "RECOVER#WS-01"    // vuelve a DISPONIBLE
            };

            for (String m : mensajes) {
                Trama.enviar(in, out, m);
                System.out.println(m + " -> " + Trama.recibir(in, out));
            }

            for (int i = 0; i < 300; i++) {
                Trama.enviar(in, out, "PING#WS-01");
                Trama.recibir(in, out);
                if (i == 90) { Trama.enviar(in, out, "FAULT#WS-01");   System.out.println("FAULT -> " + Trama.recibir(in, out)); }
                if (i == 100) { Trama.enviar(in, out, "RECOVER#WS-01"); System.out.println("RECOVER -> " + Trama.recibir(in, out)); }
                Thread.sleep(1000);
            }
        }
    }
}