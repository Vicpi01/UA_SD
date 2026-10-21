import java.io.*;
import java.net.*;

public class TestRegisterClient {
    public static void main(String[] args) throws IOException {
        try (Socket s = new Socket("localhost", 7000);
             InputStream in = new BufferedInputStream(s.getInputStream());
             OutputStream out = s.getOutputStream()) {

            Trama.iniciar(in, out);
            String[] mensajes = {"REGISTER#WS-01#Parque Central", "AUTH#WS-01", "AUTH#WS-99", "HOLA"};
            for (String m : mensajes) {
                Trama.enviar(in, out, m);
                System.out.println(m + " -> " + Trama.recibir(in, out));
            }
            Trama.cerrar(out);
        }
    }
}