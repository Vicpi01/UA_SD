import java.io.*;
import java.net.*;
import java.util.Arrays;

/*
El Monitor mantiene el servidor al que se conecta el Engine y una conexión
TCP con CENTRAL. Recibe del Engine el estado de salud de la estación y envía
a CENTRAL el registro, los latidos y los avisos de avería o recuperación.
*/
public class WM_WS_M {
    private final String id;
    private final String ubicacion;
    private final int puertoMonitor;
    private final String hostCentral;
    private final int puertoCentral;

    private Socket socketCentral;
    private InputStream inCentral;
    private OutputStream outCentral;
    private boolean enAveria;
    private boolean averiaAvisada;

    WM_WS_M(String id, String ubicacion, int puertoMonitor, String hostCentral, int puertoCentral) {
        this.id = id;
        this.ubicacion = ubicacion;
        this.puertoMonitor = puertoMonitor;
        this.hostCentral = hostCentral;
        this.puertoCentral = puertoCentral;
    }

    /*
    CENTRAL responde a cada mensaje antes de que el Monitor envíe el
    siguiente. Trama se encarga del formato y de confirmar el intercambio.
    */
    private String intercambiar(String mensaje) throws IOException {
        Trama.enviar(inCentral, outCentral, mensaje);
        String respuesta = Trama.recibir(inCentral, outCentral);
        if (respuesta == null) {
            throw new EOFException("Central ha cerrado la conexión");
        }
        return respuesta;
    }

    /*
    Abre la conexión persistente con CENTRAL e inicia el protocolo de trama.
    Comprueba si la estación existe y, si no está registrada, la da de alta
    con su ubicación antes de volver a autenticarse.
    */
    private boolean conectarCentral() {
        try {
            socketCentral = new Socket();
            socketCentral.connect(new InetSocketAddress(hostCentral, puertoCentral), 3000);
            socketCentral.setSoTimeout(5000);
            inCentral = new BufferedInputStream(socketCentral.getInputStream());
            outCentral = socketCentral.getOutputStream();
            Trama.iniciar(inCentral, outCentral);

            String respuesta = intercambiar("AUTH#" + id);
            if ("KO#estacion no registrada".equals(respuesta)) {
                respuesta = intercambiar("REGISTER#" + id + "#" + ubicacion);
                if (!"OK".equals(respuesta)) {
                    System.out.println("Registro rechazado por CENTRAL: " + respuesta);
                    cerrarCentral();
                    return false;
                }
                respuesta = intercambiar("AUTH#" + id);
            }

            if (!"OK".equals(respuesta)) {
                System.out.println("Autenticación rechazada por CENTRAL: " + respuesta);
                cerrarCentral();
                return false;
            }

            averiaAvisada = false;
            System.out.println("Monitor autenticado en CENTRAL: " + id);
            return true;
        } catch (IOException e) {
            System.out.println("No se pudo conectar con CENTRAL: " + e.getMessage());
            cerrarCentral();
            return false;
        }
    }

    /*
    Envía un ping para que CENTRAL sepa que el Monitor sigue activo.
    Cuando cambia el estado de la estación, comunica la avería o recuperación.
    */
    private void mantenerCentral() throws IOException {
        String respuesta = intercambiar("PING#" + id);
        if (!"OK".equals(respuesta)) {
            throw new IOException("Respuesta inesperada al PING: " + respuesta);
        }

        if (enAveria != averiaAvisada) {
            String mensaje = enAveria ? "FAULT#" + id : "RECOVER#" + id;
            respuesta = intercambiar(mensaje);
            if (!"OK".equals(respuesta)) {
                throw new IOException("CENTRAL rechazó " + mensaje + ": " + respuesta);
            }
            averiaAvisada = enAveria;
            System.out.println("CENTRAL actualizado: " + mensaje);
        }
    }

    /*
    Envuelve mantenerCentral() gestionando la reconexión automática si CENTRAL
    se cae o se reinicia, permitiendo llamarlo tanto esperando al Engine como dentro del bucle de salud.
    */
    private void mantenerCentralSeguro() throws InterruptedException {
        try {
            mantenerCentral();
        } catch (IOException e) {
            System.out.println("Conexión con CENTRAL perdida: " + e.getMessage());
            cerrarCentral();
            while (!conectarCentral()) {
                Thread.sleep(5000);
            }
        }
    }

    /*
    Cierra el protocolo y el socket de CENTRAL y limpia las referencias
    para que se pueda volver a conectar más adelante.
    */
    private void cerrarCentral() {
        try {
            if (outCentral != null) {
                Trama.cerrar(outCentral);
            }
            if (socketCentral != null) {
                socketCentral.close();
            }
        } catch (IOException ignored) { }
        socketCentral = null;
        inCentral = null;
        outCentral = null;
    }

    /*
    Atiende una conexión del Engine. La primera línea identifica el protocolo;
    después pide el estado de salud aproximadamente cada segundo y comunica
    los cambios a CENTRAL.
    */
    private void atenderEngine(Socket engine) throws InterruptedException {
        boolean identificado = false;
        try (engine) {
            engine.setSoTimeout(3000);
            OutputStream out = engine.getOutputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(engine.getInputStream()));

            if (!"CONECTAR".equals(in.readLine())) {
                out.write("KO#NO_ES_ENGINE\n".getBytes());
                out.flush();
                return;
            }
            identificado = true;
            out.write(("ID#" + id + "#" + ubicacion + "\n").getBytes());
            out.flush();
            System.out.println("Engine conectado a Monitor (" + id + ")");

            while (true) {
                out.write(("SALUD#" + id + "\n").getBytes());
                out.flush();
                String respuesta = in.readLine();
                if (respuesta == null) throw new IOException("El Engine cerró la conexión");

                boolean averiaAnterior = enAveria;
                enAveria = !respuesta.startsWith("OK");
                if (averiaAnterior != enAveria) {
                    System.out.println(enAveria ? "Avería/Fuga detectada en WS (" + respuesta + ")" : "Avería/Fuga resuelta en WS");
                }
                mantenerCentralSeguro();
                Thread.sleep(1000);
            }
        } catch (SocketTimeoutException e) {
            if (identificado) {
                enAveria = true;
                System.out.println("El Engine no responde (Timeout): reportando avería a CENTRAL");
                mantenerCentralSeguro();
            }
        } catch (IOException e) {
            if (identificado) {
                enAveria = true;
                System.out.println("Conexión con Engine perdida (" + e.getMessage() + "): reportando avería a CENTRAL");
                mantenerCentralSeguro();
            }
        }
    }

    /*
    Mantiene el servidor disponible para el Engine. El tiempo de espera de
    accept permite seguir enviando PING a CENTRAL mientras espera conexiones.
    Si CENTRAL se desconecta, intenta restablecer la conexión.
    */
    private void ejecutar() {
        try (ServerSocket servidor = new ServerSocket(puertoMonitor)) {
            servidor.setSoTimeout(1000);
            System.out.println("Esperando al Engine en el puerto " + puertoMonitor);
            while (true) {
                mantenerCentralSeguro();
                try {
                    atenderEngine(servidor.accept());
                } catch (SocketTimeoutException ignored) {
                    // Revisa CENTRAL y espera otra conexión del Engine.
                }
            }
        } catch (IOException e) {
            System.out.println("Error en el servidor del Engine: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            cerrarCentral();
        }
    }

    /*
    Los argumentos son: puerto local del Monitor, host y puerto de CENTRAL,
    ID de la estación y ubicación. La ubicación puede ocupar varios argumentos.
    */
    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("Uso: WM_WS_M <puertoMonitor> <hostCentral> <puertoCentral> <id> <ubicacion>");
            return;
        }
        try {
            String ubicacion = String.join(" ", Arrays.copyOfRange(args, 4, args.length));
            WM_WS_M monitor = new WM_WS_M(args[3], ubicacion, Integer.parseInt(args[0]), args[1], Integer.parseInt(args[2]));
            while (!monitor.conectarCentral()) {
                Thread.sleep(5000);
            }
            monitor.ejecutar();
        } catch (NumberFormatException e) {
            System.out.println("Los puertos deben ser números");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
