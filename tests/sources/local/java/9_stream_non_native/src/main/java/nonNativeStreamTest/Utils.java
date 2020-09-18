package nonNativeStreamTest;

import java.io.File;


public class Utils {

    public static final void removeDirectory(String baseDirPath) {
        File baseDir = new File(baseDirPath);
        File[] files = baseDir.listFiles();
        if (files != null) {
            for (File f : files) {
                f.delete();
            }
            baseDir.delete();
        }
    }

    public static final void createDirectory(String baseDirPath) {
        File baseDir = new File(baseDirPath);
        if (!baseDir.exists()) {
            baseDir.mkdirs();
        }
    }

}