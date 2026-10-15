import java.net.*;
import java.io.*;

public class EngineFake {
    public static void main(String[] args) throws IOException {
        try {
            int puerto = Integer.parseInt(args[0]);
            ServerSocket servidor = new ServerSocket(puerto);
            System.out.println("Engine fake escuchando en puerto " + puerto);

            while (true) {
                Socket cliente = servidor.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(cliente.getInputStream()));
                PrintWriter out = new PrintWriter(cliente.getOutputStream(), true);

                String mensaje = in.readLine();
                System.out.println("Recibido: " + mensaje);
                out.println("OK"); // de momento, siempre contesta bien

                cliente.close();
            }
        } catch (Exception e) {
                           System.out.println("error ");
        }
    }
}