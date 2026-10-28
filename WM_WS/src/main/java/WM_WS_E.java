import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/*
El Engine simula el funcionamiento de la estación controlando el riego,
acumulando el volumen y detectando o resolviendo fugas. Responde al Monitor y permite
cambiar el estado desde la consola mientras mantiene la conexión abierta.
*/
public class WM_WS_E {
    private String id;
    private String estado = "DISPONIBLE";
    private boolean riego;
    private int caudal;
    private double volumen;
    private boolean fuga;
    private final ConcurrentLinkedQueue<String> comandos = new ConcurrentLinkedQueue<>();

    public String getId() {
        return id;
    }

    public String getEstado() {
        return estado;
    }

    public boolean estaRegando() {
        return riego;
    }

    public int getCaudal() {
        return caudal;
    }

    public double getVolumen() {
        return volumen;
    }

    public boolean hayFuga() {
        return fuga;
    }

    public void setCaudal(int caudal) {
        if (caudal >= 0) {
            this.caudal = caudal;
        }
    }

    // Inicia un riego si no hay una fuga activa y reinicia el volumen acumulado.
    public void regar() {
        if (!fuga) {
            riego = true;
            estado = "REGANDO";
            volumen = 0;
        }
    }

    // Detiene el riego y deja la estación disponible, salvo que haya una fuga.
    public void pararRiego() {
        riego = false;
        if (fuga) {
            estado = "FUGA";
        } else {
            estado = "DISPONIBLE";
        }
    }

    // Simula una fuga: la estación queda averiada y el riego se detiene.
    public void detectarFuga() {
        fuga = true;
        riego = false;
        estado = "FUGA";
    }

    // Marca la fuga como resuelta y devuelve la estación al estado disponible.
    public void arreglarFuga() {
        fuga = false;
        estado = "DISPONIBLE";
    }

    /*
    Se ejecuta cuando el Monitor solicita el estado de salud. Si hay riego,
    añade al volumen el caudal correspondiente a un segundo y responde KO
    mientras exista una fuga.
    */
    private String comprobarSalud() {
        if (riego) volumen += caudal / 60.0;
        return fuga ? "KO" : "OK";
    }

    // Muestra por consola el estado de la estación y el volumen acumulado.
    private void mostrarEstado() {
        System.out.printf("WS %s | %s | caudal: %d L/min | volumen: %.2f L%n", id == null ? "sin ID" : id, estado, caudal, volumen);
    }

    /*
    Lee las órdenes del teclado en un hilo separado. La cola permite que el
    hilo de red aplique los cambios al estado de la estación.
    */
    private void leerComandos() {
        Thread teclado = new Thread(() -> {
            try {
                BufferedReader consola = new BufferedReader(new InputStreamReader(System.in));
                String comando;
                while ((comando = consola.readLine()) != null) {
                    comandos.add(comando.trim().toUpperCase());
                }
            } catch (IOException e) {
                System.out.println("No se pudo leer la consola: " + e.getMessage());
            }
        });
        teclado.setDaemon(true);
        teclado.start();
        System.out.println("Comandos del Engine: R regar, P parar, KO fuga, OK recuperar, ESTADO");
    }

    /*
    Vacía las órdenes pendientes y las aplica. R y P controlan el riego;
    KO y OK simulan una fuga y su recuperación.
    */
    private void procesarComandos() {
        String comando;
        while ((comando = comandos.poll()) != null) {
            switch (comando) {
                case "R" -> regar();
                case "P" -> pararRiego();
                case "KO", "K" -> detectarFuga();
                case "OK", "O" -> arreglarFuga();
                case "ESTADO" -> { }
                default -> System.out.println("Comandos: R regar, P parar, KO fuga, OK recuperar, ESTADO");
            }
            mostrarEstado();
        }
    }

    /*
    Obtiene el host y el puerto del Monitor, inicia la lectura de órdenes y
    mantiene la conexión. Si se pierde, el Engine vuelve a intentarlo.
    */
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Uso: WM_WS_E [brokerHost:puerto] <hostMonitor> <puertoMonitor>");
            return;
        }

        String hostMonitor = args.length == 1 ? "localhost" : args[args.length - 2];
        int puertoMonitor;
        try {
            puertoMonitor = Integer.parseInt(args[args.length - 1]);
        } catch (NumberFormatException e) {
            System.out.println("El puerto del Monitor debe ser un número");
            return;
        }

        WM_WS_E engine = new WM_WS_E();
        engine.leerComandos();

        while (true) {
            try (Socket socket = new Socket(hostMonitor, puertoMonitor);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                out.println("CONECTAR");
                String identificacion = in.readLine();
                if (identificacion == null || !identificacion.startsWith("ID#")) {
                    System.out.println("Monitor rechazó la conexión: " + identificacion);
                    return;
                }
                String[] campos = identificacion.split("#", 3);
                engine.id = campos[1];
                System.out.println("Conectado al Monitor: " + identificacion);
                engine.mostrarEstado();

                String mensaje;
                while ((mensaje = in.readLine()) != null) {
                    if (mensaje.startsWith("SALUD#")) {
                        engine.procesarComandos();
                        out.println(engine.comprobarSalud());
                    }
                }
            } catch (IOException e) {
                System.out.println("No se pudo conectar con el Monitor: " + e.getMessage());
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
