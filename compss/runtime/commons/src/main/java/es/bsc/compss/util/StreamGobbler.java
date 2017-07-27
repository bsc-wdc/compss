package es.bsc.compss.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import org.apache.logging.log4j.Logger;


/**
 * Support class to retrieve external processes output
 * 
 */
public class StreamGobbler extends Thread {

    private static final int BUFFER_SIZE = 4_096;

    private final InputStream in;
    private final PrintStream out;
    private final Logger logger;


    /**
     * Creates a new StreamGobbler for is @in and prints information to os @out
     * 
     * @param in
     * @param out
     * @param logger
     */
    public StreamGobbler(InputStream in, PrintStream out, Logger logger) {
        this.setName("Stream Gobbler");

        this.in = in;
        this.out = out;
        this.logger = logger;
    }

    @Override
    public void run() {
        try {
            final byte[] buffer = new byte[BUFFER_SIZE];
            int nRead;
            while ((nRead = in.read(buffer, 0, buffer.length)) != -1) {
                byte[] readData = new byte[nRead];
                System.arraycopy(buffer, 0, readData, 0, nRead);
                out.print(new String(readData));
            }
        } catch (IOException ioe) {
            logger.error("Exception during reading/writing in output Stream", ioe);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ioe) {
                    logger.warn("Exception closing IN InputStream", ioe);
                }
            }

            if (out != null) {
                out.flush();
                out.close();
            }
        }
    }

}
