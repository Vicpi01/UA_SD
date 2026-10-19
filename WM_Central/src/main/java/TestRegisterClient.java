import java.io.*;
import java.net.*;

public class TestRegisterClient {
    public static void main(String[] args) throws IOException {
        try (Socket s = new Socket("localhost", 7000);
             PrintWriter out = new PrintWriter(s.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))) {

            out.println("REGISTER#WS-01#Parque Central");
            System.out.println("CENTRAL respondió: " + in.readLine());

            out.println("HOLA");   // mensaje inválido a propósito
            System.out.println("CENTRAL respondió: " + in.readLine());
        }
    }
}