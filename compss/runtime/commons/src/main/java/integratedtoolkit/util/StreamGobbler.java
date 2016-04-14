package integratedtoolkit.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;


public class StreamGobbler extends Thread {

    InputStream is;
    PrintStream out;

    public StreamGobbler(InputStream is, PrintStream out) {
    	this.setName("Stream Gobbler");
    	
        this.is = is;
        this.out = out;
    }

    public void run() {
        try {
            int nRead;
            byte[] buffer = new byte[4096];
            while ((nRead = is.read(buffer, 0, buffer.length)) != -1) {
                byte[] readData = new byte[nRead];
                System.arraycopy(buffer, 0, readData, 0, nRead);
                out.print(new String(readData));
            }
        } catch (IOException ioe) {
            System.err.println("Exception during reading/writing in output Stream");
            ioe.printStackTrace();
        } finally {
            out.flush();
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
