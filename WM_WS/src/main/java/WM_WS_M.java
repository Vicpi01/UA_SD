import java.net.*;
import java.io.*;

public class WM_WS_M {
    private final int id;
    private String ubicacion;
    private int puertoCentral;
    private int puertoMonitor;
    private String host;
    private String estado;
    private Boolean fuga;

    WM_WS_M(int id, String ubicacion, int puertoCentral, int puertoMonitor, String host) {
        this.id = id;
        this.ubicacion = ubicacion;
        this.puertoCentral = puertoCentral;
        this.puertoMonitor = puertoMonitor;
        this.host = host;
        this.estado = null;
        this.fuga = null;
    }

    private void conexionEngine() {
        try {
            ServerSocket socketEngine = new ServerSocket(puertoMonitor);
            Socket respuestaEngine = socketEngine.accept();
            PrintWriter out = new PrintWriter(respuestaEngine.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(respuestaEngine.getInputStream()));

            out.println("Cuál es tu estado?");
            String respuesta = in.readLine();
            System.out.println("Respuesta del Engine: " + respuesta);

            socketEngine.close();
        } catch (IOException e) {
            System.out.println("Error al conectar con el Engine: " + e.getMessage());
        }
    }

    private void registrarEnCentral() {
        try {
            Socket socketCentral = new Socket(host, puertoCentral);
            PrintWriter out = new PrintWriter(socketCentral.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socketCentral.getInputStream()));

            out.println("ID: " + id + ", Ubicación: " + ubicacion);
            String respuesta = in.readLine();
            System.out.println("Conexión con la Central: " + respuesta);

            socketCentral.close();
        } catch (IOException e) {
            System.out.println("Error al registrar en la Central: " + e.getMessage());
        }
    }

    private void conexionCentral() {
        try {
            Socket socketCentral = new Socket(host, puertoCentral);
            PrintWriter out = new PrintWriter(socketCentral.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socketCentral.getInputStream()));

            out.println("Mi estado es: " + estado);
            String respuesta = in.readLine();
            System.out.println("Respuesta de la Central: " + respuesta);

            socketCentral.close();
        } catch (IOException e) {
            System.out.println("Error al conectar con la Central: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int puertoMonitor = Integer.parseInt(args[0]);
        String host = args[1];
        int puertoCentral = Integer.parseInt(args[2]);
        int id = Integer.parseInt(args[3]);
        String ubicacion = args[4];

        WM_WS_M Monitor = new WM_WS_M(id, ubicacion, puertoCentral, puertoMonitor, host);

        // Una vez creado el Monitor lo conecto con el Engine y cada
        // segundo va preguntando por el estado y recibe la respuesta del Engine
        // A su vez, va a ir recibiendo mensajes de la Central y va a ir mandando
        // ordenes al Engine conforme a lo que le vaya recibiendo de la Central

        try {
            while (true) {
                Monitor.conexionEngine();
                Monitor.registrarEnCentral();
                Monitor.conexionCentral();
                Thread.sleep(1000); // Espera 1 segundo antes de la siguiente iteración
            }
        } catch (InterruptedException e) {
            System.out.println("Ha surgido un error: " + e.getMessage());
        }
    }
}