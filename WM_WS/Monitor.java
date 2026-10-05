import java.net.*;
import java.io.*;

public class Monitor {
    public static void main(String[] args) throws IOException, InterruptedException {
        try{
            String host = args[0];
            int puerto = Integer.parseInt(args[1]);

            while (true) {
                Socket socket = new Socket(host, puerto);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                out.println("estado?");
                String respuesta = in.readLine();
                System.out.println("Engine respondió: " + respuesta);

                socket.close();
                Thread.sleep(1000); // esperar 1 segundo antes de volver a preguntar
            }

        }
        catch(Exception e){
               System.out.println("error ");
   
        }
    }
}