import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/*
Implementa el protocolo de trama que usa el Monitor para comunicarse con
CENTRAL. Cada mensaje lleva STX, texto, ETX y LRC; el receptor confirma con
ACK o solicita repetir la trama con NACK.
*/
public class Trama {
    static final int STX = 0x02, ETX = 0x03, EOT = 0x04,
            ENQ = 0x05, ACK = 0x06, NACK = 0x15;

    /*
    Calcula el LRC con XOR. El mismo cálculo se usa al enviar y validar
    una trama recibida.
    */
    static byte lrc(byte[] datos) {
        byte r = 0;
        for (byte b : datos) r ^= b;
        return r;
    }

    /*
    Inicia el intercambio: el cliente envía ENQ y espera ACK antes de
    transmitir mensajes.
    */
    static void iniciar(InputStream in, OutputStream out) throws IOException {
        out.write(ENQ);
        out.flush();
        if (in.read() != ACK) throw new IOException("El servidor no aceptó la conexión");
    }

    /*
    Construye una trama UTF-8 y espera confirmación. Si recibe NACK, vuelve
    a enviarla hasta tres veces.
    */
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
            int respuesta = in.read();
            if (respuesta == ACK) return;
            if (respuesta == -1) throw new EOFException("Conexión cerrada");
            if (respuesta != NACK) throw new IOException("Respuesta de trama inesperada");
        }
        throw new IOException("Trama rechazada (NACK) 3 veces");
    }

    /*
    Espera una trama completa, valida el LRC y responde ACK o NACK. Devuelve
    null si termina la conexión o CENTRAL envía EOT.
    */
    static String recibir(InputStream in, OutputStream out) throws IOException {
        while (true) {
            int b = in.read();
            if (b == -1 || b == EOT) return null;
            if (b == ENQ) {
                out.write(ACK);
                out.flush();
                continue;
            }
            if (b != STX) continue;

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            int c;
            while ((c = in.read()) != ETX) {
                if (c == -1) return null;
                buf.write(c);
            }
            int lrcRecibido = in.read();
            byte[] datos = buf.toByteArray();
            if (lrcRecibido == (lrc(datos) & 0xFF)) {
                out.write(ACK);
                out.flush();
                return new String(datos, StandardCharsets.UTF_8);
            }
            out.write(NACK);
            out.flush();
        }
    }

    // Indica a CENTRAL que termina la sesión del Monitor.
    static void cerrar(OutputStream out) throws IOException {
        out.write(EOT);
        out.flush();
    }
}
