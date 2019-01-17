package examples20;

import java.io.IOException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.io.RandomAccessFile;

public class RandomAccessFileExample {

    /**
     * This example shows the use of the RandomAccessFile object in JavaGAT
     * 
     * This example needs two URIs pointing to files. It reads from the first
     * (source) file, and writes to the second (target) file. It reverses the
     * order of bytes, so that the first byte of the source will be the last
     * byte of the target and so on.
     * 
     * @param args
     *                a String array of size 2 with each element containing a
     *                String representation of a valid URI.
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out
                    .println("\tUsage: bin/run_gat_app examples20.RandomAccessFileExample <source> <target> (where source and target are valid JavaGAT URIs)\n");
            System.exit(1);
        }
        new RandomAccessFileExample().start(args[0], args[1]);
        GAT.end();

    }

    public void start(String source, String target) {
        RandomAccessFile sourceFile = null;
        try {
            sourceFile = GAT.createRandomAccessFile(source, "r");
        } catch (GATObjectCreationException e) {
            System.err.println("Failed to create random access file '" + source
                    + "': " + e);
            return;
        }
        RandomAccessFile targetFile = null;
        try {
            targetFile = GAT.createRandomAccessFile(target, "rw");
        } catch (GATObjectCreationException e) {
            System.err.println("Failed to create random access file '" + target
                    + "': " + e);
            return;
        }
        try {
            targetFile.setLength(sourceFile.length());
        } catch (IOException e) {
            System.err.println("Failed to set/get the length: " + e);
            return;
        }
        try {
            for (int i = 0; i < sourceFile.length(); i++) {
                targetFile.seek(targetFile.length() - 1 - i);
                sourceFile.seek(i);
                targetFile.writeByte(sourceFile.readByte());
            }
        } catch (IOException e) {
            System.out.println("Failed to seek/read/write: " + e);
            return;
        }
    }
}
