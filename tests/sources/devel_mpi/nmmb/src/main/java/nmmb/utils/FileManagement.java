package nmmb.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;


/**
 * Helper class to manage the file system
 *
 */
public class FileManagement {

    /**
     * Deletes the file specified by the filePath
     * 
     * @param filePath
     * @return
     */
    public static boolean deleteFile(String filePath) {
        File file = new File(filePath);
        try {
            return file.delete();
        } catch (SecurityException se) {
            return false;
        }
    }

    /**
     * Deletes the file given
     * 
     * @param file
     * @return
     */
    public static boolean deleteFile(File file) {
        try {
            return file.delete();
        } catch (SecurityException se) {
            return false;
        }
    }

    /**
     * Fault tolerant method to erase a file or folder recursively Does not raise any exception when file is not erased
     * 
     * @param fileOrFolder
     * @return
     */
    public static void deleteFileOrFolder(File fileOrFolder) {
        try {
            if (fileOrFolder != null) {
                if (fileOrFolder.isDirectory()) {
                    File folder = fileOrFolder;
                    // Clean all the childs
                    File[] childs = folder.listFiles();
                    if (childs != null) {
                        for (File child : childs) {
                            deleteFileOrFolder(child);
                        }
                    }
                    // Clean the empty directory
                    folder.delete();
                } else {
                    // It is a file, we can delete it automatically
                    File file = fileOrFolder;

                    file.delete();
                }
            } else {
                // The file is not valid, skip
            }
        } catch (SecurityException se) {
            // Skip
        }
    }

    public static boolean copyFile(String srcPath, String targetPath) {
        try {
            Files.copy(Paths.get(srcPath), Paths.get(targetPath), StandardCopyOption.REPLACE_EXISTING);
        } catch (UnsupportedOperationException | IOException | SecurityException e) {
            return false;
        }

        return true;
    }

    public static boolean moveFile(String srcPath, String targetPath) {
        try {
            Files.move(Paths.get(srcPath), Paths.get(targetPath), StandardCopyOption.REPLACE_EXISTING);
        } catch (UnsupportedOperationException | IOException | SecurityException e) {
            return false;
        }

        return true;
    }

    public static boolean createDir(String folderPath) {
        File folder = new File(folderPath);
        return folder.mkdirs();
    }

}
