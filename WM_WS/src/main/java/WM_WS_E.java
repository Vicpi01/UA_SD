import java.net.*;
import java.io.*;

public class WM_WS_E {
    private final int id;
    private String estado;
    private boolean riego;
    private int caudal;
    private boolean fuga;

    WM_WS_E(int id) {
        this.id = id;
        this.estado = "Disponible";
        this.riego = false;
        this.caudal = 0;
        this.fuga = false;
    }

    public int getId() {
        return id;
    }

    public void regar() {
        this.estado = "Regando";
        this.riego = true;
    }

    public void pararRiego() {
        this.estado = "Disponible";
        this.riego = false;
    }

    public String getEstado() {
        return estado;
    }

    public int getCaudal() {
        return caudal;
    }

    public void setCaudal(int caudal) {
        this.caudal = caudal;
    }

    public boolean hayFuga() {
        return fuga;
    }

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