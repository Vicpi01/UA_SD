import java.io.*;
import java.net.*;
import java.util.*;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class WM_WS_E {
    private static final String SER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String DES = "org.apache.kafka.common.serialization.StringDeserializer";

    private volatile String id;
    private String estado = "DISPONIBLE";
    private boolean riego;
    private int caudal;
    private double volumen;
    private volatile boolean fuga;
    private boolean bloqueada;
    private String operario = "-";
    private int segundosObjetivo;
    private int segundosTranscurridos;

    private final String broker;                 // null = sin Kafka (modo local)
    private KafkaProducer<String, String> productor;
    private boolean kafkaIniciado;
    private volatile String operarioManual;      // operario de la última petición hecha desde la consola
    private final Random azar = new Random();
    private final ConcurrentLinkedQueue<String> comandos = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> ordenes = new ConcurrentLinkedQueue<>();

    WM_WS_E(String broker) {
        this.broker = broker;
    }

    public String getId() { return id; }
    public String getEstado() { return estado; }
    public boolean estaRegando() { return riego; }
    public int getCaudal() { return caudal; }
    public double getVolumen() { return volumen; }
    public boolean hayFuga() { return fuga; }

    private static String formato(double v) {
        return String.format(Locale.US, "%.1f", v);
    }

    // ---------- Estado de la estación ----------

    private void recalcularEstado() {
        if (riego) estado = "REGANDO";
        else if (fuga) estado = "FUGA";
        else if (bloqueada) estado = "FUERA_DE_SERVICIO";
        else estado = "DISPONIBLE";
    }

    private void iniciarRiego(String op, int segundos) {
        if (riego || fuga || bloqueada) {
            System.out.println("Orden START ignorada (riego en curso, fuga o estación bloqueada)");
            return;
        }
        riego = true;
        operario = op;
        segundosObjetivo = segundos;
        segundosTranscurridos = 0;
        volumen = 0;
        caudal = 10 + azar.nextInt(6);            // entre 10 y 15 L/min
        recalcularEstado();
        System.out.println("Riego iniciado para " + op + " durante " + segundos + " s");
        mostrarEstado();
    }

    // Termina el riego en curso (por tiempo, parada o fuga) y se lo comunica a CENTRAL con un FIN.
    private void terminarRiego(String motivo) {
        if (!riego) return;
        riego = false;
        publicar("ws-estado", id, id + "#FIN#" + formato(volumen) + "#" + segundosTranscurridos + "#" + operario);
        System.out.println("Riego terminado (" + motivo + "): " + formato(volumen) + " L en " + segundosTranscurridos + " s");
        caudal = 0;
        operario = "-";
        recalcularEstado();
        mostrarEstado();
    }

    private void detectarFuga() {
        fuga = true;
        terminarRiego("fuga detectada");
        recalcularEstado();
    }

    private void arreglarFuga() {
        fuga = false;
        recalcularEstado();
    }

    // Respuesta al Monitor: KO mientras exista una fuga.
    private String comprobarSalud() {
        return fuga ? "KO" : "OK";
    }

    private void mostrarEstado() {
        System.out.printf("WS %s | %s | caudal: %d L/min | volumen: %.2f L | operario: %s%n",
                id == null ? "sin ID" : id, estado, caudal, volumen, operario);
    }

    // ---------- Reloj: un turno por segundo ----------

    private void iniciarReloj() {
        ScheduledExecutorService reloj = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        reloj.scheduleAtFixedRate(this::tick, 1, 1, TimeUnit.SECONDS);
    }

    private void tick() {
        try {
            if (id == null) return;                       // aún no conoce su ID (el Monitor no ha conectado)
            procesarComandos();
            procesarOrdenes();
            if (riego) {
                segundosTranscurridos++;
                volumen += caudal / 60.0;
                publicar("ws-estado", id, id + "#RIEGO#" + caudal + "#" + formato(volumen) + "#" + operario);
                mostrarEstado();
                if (segundosTranscurridos >= segundosObjetivo) terminarRiego("tiempo cumplido");
            }
        } catch (Exception e) {
            System.out.println("Error en el Engine: " + e.getMessage());
        }
    }

    // ---------- Órdenes de CENTRAL (idWS#ACCION[#idFO#segundos]) ----------

    private void procesarOrdenes() {
        String orden;
        while ((orden = ordenes.poll()) != null) {
            String[] c = orden.split("#");
            switch (c[1]) {
                case "START" -> {
                    int segundos = 30;
                    try { if (c.length >= 4) segundos = Integer.parseInt(c[3]); }
                    catch (NumberFormatException e) { /* se queda la duración por defecto */ }
                    iniciarRiego(c.length >= 3 ? c[2] : "CENTRAL", segundos);
                }
                case "STOP" -> terminarRiego("orden de CENTRAL");
                case "BLOCK" -> {
                    bloqueada = true;
                    terminarRiego("estación bloqueada");
                    recalcularEstado();
                    System.out.println("Estación fuera de servicio por orden de CENTRAL");
                    mostrarEstado();
                }
                case "ACTIVATE" -> {
                    bloqueada = false;
                    recalcularEstado();
                    System.out.println("Estación activada por CENTRAL");
                    mostrarEstado();
                }
                default -> System.out.println("Orden desconocida: " + orden);
            }
        }
    }

    // ---------- Teclado ----------

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
        System.out.println("Comandos del Engine: R [operario] [segundos] regar (lo autoriza CENTRAL), P parar, KO fuga, OK recuperar, ESTADO");
    }

    private void procesarComandos() {
        String linea;
        while ((linea = comandos.poll()) != null) {
            String[] p = linea.split("\\s+");
            switch (p[0]) {
                case "R" -> solicitarRiego(p.length > 1 ? p[1] : "FO-01", p.length > 2 ? p[2] : null);
                case "P" -> terminarRiego("parada manual");
                case "KO", "K" -> detectarFuga();
                case "OK", "O" -> arreglarFuga();
                case "ESTADO" -> { }
                default -> System.out.println("Comandos: R [operario] [segundos], P parar, KO fuga, OK recuperar, ESTADO");
            }
            mostrarEstado();
        }
    }

    // El riego manual pasa por CENTRAL como el de cualquier operario; el START llega después por ws-ordenes.
    private void solicitarRiego(String op, String segundos) {
        if (riego) { System.out.println("Ya hay un riego en curso"); return; }
        if (fuga || bloqueada) { System.out.println("La estación no admite riegos (" + estado + ")"); return; }
        if (productor == null) {
            System.out.println("Sin conexión con CENTRAL (Kafka): no se puede solicitar el riego");
            return;
        }
        operarioManual = op;
        String extra = (segundos != null && segundos.matches("\\d+")) ? "#" + segundos : "";
        publicar("fo-peticiones", op, op + "#" + id + extra);
        System.out.println("Petición enviada a CENTRAL como " + op + ". Esperando autorización...");
    }

    // ---------- Kafka ----------

    private void publicar(String topic, String clave, String valor) {
        if (productor == null) return;
        try {
            productor.send(new ProducerRecord<>(topic, clave, valor));
        } catch (Exception e) {
            System.out.println("Imposible conectar con CENTRAL (Kafka): " + e.getMessage());
        }
    }

    // Se llama una vez, cuando el Monitor le ha dado su ID.
    private void iniciarKafka() {
        if (broker == null || kafkaIniciado) return;
        kafkaIniciado = true;

        Properties pp = new Properties();
        pp.put("bootstrap.servers", broker);
        pp.put("key.serializer", SER);
        pp.put("value.serializer", SER);
        pp.put("max.block.ms", "3000");
        productor = new KafkaProducer<>(pp);

        // Grupo propio y único en cada arranque: recibe todas las órdenes nuevas y no repite las antiguas.
        String grupo = "engine-" + id + "-" + System.currentTimeMillis();

        Thread hiloOrdenes = new Thread(() -> escuchar("ws-ordenes", grupo, (clave, valor) -> {
            String[] c = valor.split("#");
            if (c.length >= 2 && c[0].equals(id)) ordenes.add(valor);
        }));
        hiloOrdenes.setDaemon(true);
        hiloOrdenes.start();

        // Para enseñar la respuesta de CENTRAL (OK / KO) a las peticiones hechas desde la consola.
        Thread hiloRespuestas = new Thread(() -> escuchar("fo-respuestas", grupo + "-r", (clave, valor) -> {
            String[] v = valor.split("#");
            String op = operarioManual;
            if (op != null && op.equals(clave) && v.length >= 2 && (v[1].equals("OK") || v[1].equals("KO"))) {
                System.out.println("CENTRAL -> " + valor);
            }
        }));
        hiloRespuestas.setDaemon(true);
        hiloRespuestas.start();
        System.out.println("Kafka: conectado a " + broker);
    }

    private void escuchar(String topic, String grupo, BiConsumer<String, String> accion) {
        Properties pc = new Properties();
        pc.put("bootstrap.servers", broker);
        pc.put("group.id", grupo);
        pc.put("key.deserializer", DES);
        pc.put("value.deserializer", DES);
        pc.put("auto.offset.reset", "latest");
        pc.put("enable.auto.commit", "false");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pc)) {
            consumer.subscribe(List.of(topic));
            while (consumer.assignment().isEmpty()) consumer.poll(Duration.ofMillis(200));
            for (var tp : consumer.assignment()) consumer.position(tp);   // fija el punto de lectura
            while (true) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    accion.accept(r.key(), r.value());
                }
            }
        } catch (Exception e) {
            System.out.println("Imposible conectar con CENTRAL (Kafka): " + e.getMessage());
        }
    }

    // ---------- Conexión con el Monitor ----------

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Uso: WM_WS_E [brokerHost:puerto] <hostMonitor> <puertoMonitor>");
            return;
        }

        String broker = args.length >= 3 ? args[0] : null;
        String hostMonitor = args.length == 1 ? "localhost" : args[args.length - 2];
        int puertoMonitor;
        try {
            puertoMonitor = Integer.parseInt(args[args.length - 1]);
        } catch (NumberFormatException e) {
            System.out.println("El puerto del Monitor debe ser un número");
            return;
        }
        if (broker == null) System.out.println("Sin broker de Kafka: el Engine funciona en modo local");

        WM_WS_E engine = new WM_WS_E(broker);
        engine.leerComandos();
        engine.iniciarReloj();

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
                engine.iniciarKafka();
                engine.mostrarEstado();

                String mensaje;
                while ((mensaje = in.readLine()) != null) {
                    if (mensaje.startsWith("SALUD#")) {
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