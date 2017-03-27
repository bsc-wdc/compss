package blast.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import blast.exceptions.BlastException;


public class FilesManagement {

    /**
     * Moves the source file to the target location
     * 
     * @param source
     * @param target
     * @param debug
     * @throws BlastException
     */
    public static void copyResult(String source, String target, boolean debug) throws BlastException {
        if (debug) {
            System.out.println("");
            System.out.println("Moving last merged file: " + source + " to " + target);
            System.out.println("");
        }

        try (FileInputStream fis = new FileInputStream(source)) {
            copyFile(fis, new File(target));
        } catch (IOException ioe) {
            String msg = "ERROR: Cannot copy file " + source + " to " + target;
            System.err.print(msg);
            throw new BlastException(msg, ioe);
        }
    }

    /**
     * Copies the sourceFile to the destination location
     * 
     * @param sourceFile
     * @param destFile
     * @throws IOException
     */
    private static void copyFile(FileInputStream sourceFile, File destFile) throws IOException {
        try (FileChannel source = sourceFile.getChannel();
                FileOutputStream outputDest = new FileOutputStream(destFile);
                FileChannel destination = outputDest.getChannel()) {

            destination.transferFrom(source, 0, source.size());
        } catch (IOException ioe) {
            throw ioe;
        }
    }
}
