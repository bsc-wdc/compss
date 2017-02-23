package nmmb.utils;

import java.io.File;


public class FileManagement {

    /**
     * Deletes a folder recursively
     * 
     * @param file
     */
    public static void deleteAll(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                // File is a folder and it is not empty, delete recursively
                for (File f : files) {
                    deleteAll(f);
                }
            }
        }

        // File is either an empty folder or a file
        file.delete();
    }

}
