import java.net.*;
import java.io.*;

public class WM_WS_M {

    private final int id;
    private String ubicacion;
    private int puertoCentral;
    private int puertoMonitor;
    private String host;
    private String estado;
    private boolean fuga;

    // Guardamos la conexion con CENTRAL para reutilizarla.
    private Socket socketCentral;
    private PrintWriter outCentral;
    private BufferedReader inCentral;

    // Guardamos la salida hacia ENGINE para poder enviarle ordenes.
    private PrintWriter outEngine;

    private volatile boolean ejecutando = true;

    WM_WS_M(int id, String ubicacion,
            int puertoCentral, int puertoMonitor, String host) {

        this.id = id;
        this.ubicacion = ubicacion;
        this.puertoCentral = puertoCentral;
        this.puertoMonitor = puertoMonitor;
        this.host = host;

        this.estado = "DESCONECTADA";
        this.fuga = false;
    }

    private void conexionEngine() {
        try {
            ServerSocket servidorEngine =
                    new ServerSocket(puertoMonitor);

            System.out.println("Esperando al Engine...");

            Socket respuestaEngine = servidorEngine.accept();

            // Esperamos como maximo 2 segundos por respuesta.
            respuestaEngine.setSoTimeout(2000);

            PrintWriter out = new PrintWriter(
                    respuestaEngine.getOutputStream(), true);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                            respuestaEngine.getInputStream()));

            outEngine = out;
            estado = "CONECTADA";

            System.out.println("Engine conectado");

            while (ejecutando) {

                out.println("ESTADO?");

                try {
                    String respuesta = in.readLine();

                    if (respuesta == null) {
                        System.out.println(
                                "El Engine se ha desconectado");
                        break;
                    }

                    System.out.println(
                            "Respuesta del Engine: " + respuesta);

                    if (respuesta.equals("KO")) {
                        estado = "FUGA";

                        if (!fuga) {
                            fuga = true;
                            outCentral.println("FAULT#" + id);
                        }

                    } else if (respuesta.equals("OK")) {
                        estado = "CONECTADA";

                        if (fuga) {
                            fuga = false;
                            outCentral.println("RECOVERY#" + id);
                        }
                    }

                } catch (SocketTimeoutException e) {
                    System.out.println(
                            "El Engine no ha respondido");

                    estado = "DESCONECTADA";
                    outCentral.println("FAULT#" + id + "#TIMEOUT");
                    break;
                }

                // Esperamos un segundo antes de volver a preguntar.
                Thread.sleep(1000);
            }

            outEngine = null;
            respuestaEngine.close();
            servidorEngine.close();

        } catch (IOException e) {
            System.out.println(
                    "Error al conectar con el Engine: "
                            + e.getMessage());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean registrarEnCentral() {
        try {
            socketCentral = new Socket(host, puertoCentral);

            outCentral = new PrintWriter(
                    socketCentral.getOutputStream(), true);

            inCentral = new BufferedReader(
                    new InputStreamReader(
                            socketCentral.getInputStream()));

            outCentral.println(
                    "ID: " + id + ", Ubicacion: " + ubicacion);

            String respuesta = inCentral.readLine();

            System.out.println(
                    "Respuesta del registro: " + respuesta);

            if (respuesta == null ||
                    !respuesta.trim().equalsIgnoreCase("OK")) {

                socketCentral.close();
                return false;
            }

            // Importante: no cerramos el socket.
            // Lo seguiremos utilizando para recibir mensajes.
            return true;

        } catch (IOException e) {
            System.out.println(
                    "Error al registrar en la Central: "
                            + e.getMessage());
            return false;
        }
    }

    private void conexionCentral() {
        try {
            String mensaje;

            while (ejecutando &&
                    (mensaje = inCentral.readLine()) != null) {

                System.out.println(
                        "Mensaje recibido de la Central: " + mensaje);

                // Reenviamos a ENGINE el mensaje recibido.
                if (outEngine != null) {
                    outEngine.println(mensaje);
                } else {
                    System.out.println(
                            "El Engine todavia no esta conectado");
                }
            }

        } catch (IOException e) {
            if (ejecutando) {
                System.out.println(
                        "Error al recibir mensajes de Central: "
                                + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {

        int puertoMonitor = Integer.parseInt(args[0]);
        String host = args[1];
        int puertoCentral = Integer.parseInt(args[2]);
        int id = Integer.parseInt(args[3]);
        String ubicacion = args[4];

        WM_WS_M Monitor = new WM_WS_M(
                id, ubicacion, puertoCentral, puertoMonitor, host);

        try {
            // Primero registramos el Monitor en CENTRAL.
            boolean conexion = Monitor.registrarEnCentral();

            if (!conexion) {
                System.out.println(
                        "No se ha podido registrar en Central");
                return;
            }

            // Un hilo recibe mensajes de CENTRAL continuamente.
            Thread hiloCentral =
                    new Thread(Monitor::conexionCentral);

            hiloCentral.start();

            // El hilo principal espera al Engine y lo vigila.
            Monitor.conexionEngine();

        } finally {
            Monitor.ejecutando = false;

            try {
                if (Monitor.socketCentral != null) {
                    Monitor.socketCentral.close();
                }
            } catch (IOException e) {
                System.out.println(
                        "Error al cerrar la conexion con Central");
            }
        }
    }
}