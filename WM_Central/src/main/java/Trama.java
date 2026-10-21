import java.io.*;
import java.nio.charset.StandardCharsets;

public class Trama {
    static final int STX = 0x02, ETX = 0x03, EOT = 0x04,
                     ENQ = 0x05, ACK = 0x06, NACK = 0x15;

    // LRC = XOR de todos los bytes del mensaje
    static byte lrc(byte[] datos) {
        byte r = 0;
        for (byte b : datos) r ^= b;
        return r;
    }

    // Cliente: abre la conversación (ENQ -> ACK)
    static void iniciar(InputStream in, OutputStream out) throws IOException {
        out.write(ENQ);
        out.flush();
        if (in.read() != ACK) throw new IOException("El servidor no aceptó la conexión");
    }

    // Envía <STX>mensaje<ETX><LRC> y espera ACK (reintenta si recibe NACK)
    static void enviar(InputStream in, OutputStream out, String mensaje) throws IOException {
        byte[] datos = mensaje.getBytes(StandardCharsets.UTF_8);
        byte[] trama = new byte[datos.length + 3];
        trama[0] = STX;
        System.arraycopy(datos, 0, trama, 1, datos.length);
        trama[datos.length + 1] = ETX;
        trama[datos.length + 2] = lrc(datos);

        for (int intento = 0; intento < 3; intento++) {
            out.write(trama);
            out.flush();
            int r = in.read();
            if (r == ACK) return;
            if (r == -1) throw new EOFException("Conexión cerrada");
        }
        throw new IOException("Trama rechazada (NACK) 3 veces");
    }

    // Recibe una trama, valida el LRC y contesta ACK/NACK.
    // Devuelve el mensaje, o null si el otro lado cierra (EOT o fin de conexión).
    static String recibir(InputStream in, OutputStream out) throws IOException {
        while (true) {
            int b = in.read();
            if (b == -1 || b == EOT) return null;
            if (b == ENQ) { out.write(ACK); out.flush(); continue; }
            if (b != STX) continue;                       // ignora basura

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            int c;
            while ((c = in.read()) != ETX) {
                if (c == -1) return null;
                buf.write(c);
            }
            int lrcRecibido = in.read();
            byte[] datos = buf.toByteArray();

            if (lrcRecibido == (lrc(datos) & 0xFF)) {
                out.write(ACK); out.flush();
                return new String(datos, StandardCharsets.UTF_8);
            }
            out.write(NACK); out.flush();                 // LRC incorrecto: esperamos el reenvío
        }
    }

    // Cliente: cierra la conversación
    static void cerrar(OutputStream out) throws IOException {
        out.write(EOT);
        out.flush();
    }
}