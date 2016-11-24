package integratedtoolkit.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;


/**
 * Support class to retrieve external processes output
 * 
 */
public class StreamGobbler extends Thread {

    private final InputStream in;
    private final PrintStream out;


    /**
     * Creates a new StreamGobbler for is @in and prints information to os @out
     * 
     * @param in
     * @param out
     */
    public StreamGobbler(InputStream in, PrintStream out) {
        this.setName("Stream Gobbler");

        this.in = in;
        this.out = out;
    }

    @Override
    public void run() {
        try {
            int nRead;
            byte[] buffer = new byte[4096];
            while ((nRead = in.read(buffer, 0, buffer.length)) != -1) {
                byte[] readData = new byte[nRead];
                System.arraycopy(buffer, 0, readData, 0, nRead);
                out.print(new String(readData));
            }
        } catch (IOException ioe) {
            System.err.println("Exception during reading/writing in output Stream");
            ioe.printStackTrace(System.err);
        } finally {
            out.flush();
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                }
            }
        }
    }

}
