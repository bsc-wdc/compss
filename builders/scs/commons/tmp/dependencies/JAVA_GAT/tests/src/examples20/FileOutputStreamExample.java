package examples20;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.io.FileOutputStream;

public class FileOutputStreamExample {

    /**
     * This example shows the use of the FileOutputStream object in JavaGAT
     * 
     * This example requires one valid JavaGAT URI which should point to an
     * existing file. The content of this file will be overwritten with the text
     * "Hello World!".
     * 
     * @param args
     *                the String representation of the file to which "Hello
     *                World!" will be written.
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out
                    .println("\tUsage: bin/run_gat_app examples20.FileOutputStreamExample <location> (where location is a valid JavaGAT URI)\n");
            System.exit(1);
        }
        new FileOutputStreamExample().start(args[0]);
        GAT.end();
    }

    public void start(String location) {
        FileOutputStream out = null;
        try {
            out = GAT.createFileOutputStream(location);
        } catch (GATObjectCreationException e) {
            System.err.println("failed to create outputstream at location '"
                    + location + "': " + e);
            return;
        }
        OutputStreamWriter writer = new OutputStreamWriter(out);
        try {
            writer.write("Hello World!\n");
            writer.flush();
        } catch (IOException e) {
            System.err.println("failed to write to location '" + location
                    + "': " + e);
        }
        try {
            writer.close();
        } catch (IOException e) {
            System.err.println("failed to close writer at location '"
                    + location + "': " + e);
            return;
        }
    }

}
