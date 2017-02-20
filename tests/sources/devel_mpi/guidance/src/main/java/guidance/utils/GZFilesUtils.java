package guidance.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class GZFilesUtils {

    /**
     * Method to copy a file
     * 
     * @param source
     * @param dest
     * @throws IOException
     */
    public static void copyFile(File source, File dest) throws IOException {
        Files.copy(source.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Method to compress a path to a dest file
     * 
     * @param sourceFilePath
     * @param destZipFilePath
     */
    public static void gzipFile(String sourceFilePath, String destZipFilePath) throws IOException {
        byte[] buffer = new byte[1024];

        try (FileInputStream fileInput = new FileInputStream(sourceFilePath);
                GZIPOutputStream gzipOuputStream = new GZIPOutputStream(new FileOutputStream(destZipFilePath))) {

            int bytes_read = 0;
            while ((bytes_read = fileInput.read(buffer)) > 0) {
                gzipOuputStream.write(buffer, 0, bytes_read);
            }
            gzipOuputStream.finish();
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
            ioe.printStackTrace();
            throw ioe;
        }
    }

    /**
     * Method to decompress a file
     * 
     * @param compressedFile
     * @param decompressedFile
     */
    public static void gunzipFile(String compressedFile, String decompressedFile) throws IOException {
        byte[] buffer = new byte[1024];

        try (GZIPInputStream gZIPInputStream = new GZIPInputStream(new FileInputStream(compressedFile));
                FileOutputStream fileOutputStream = new FileOutputStream(decompressedFile)) {

            int bytes_read;
            while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, bytes_read);
            }
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
            ioe.printStackTrace();
            throw ioe;
        }

        // System.out.println("The file was decompressed successfully!");
    }

}
