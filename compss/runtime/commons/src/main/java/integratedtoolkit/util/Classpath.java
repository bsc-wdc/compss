package integratedtoolkit.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import org.apache.log4j.Logger;

public class Classpath {

    public static void loadPath(String jarPath, Logger logger) throws FileNotFoundException {
        File directory = new File(jarPath);
        if (!directory.exists()) {
            throw new FileNotFoundException();
        }

        URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        Class<?> sysclass = URLClassLoader.class;
        Method method = null;
        try {
            method = sysclass.getDeclaredMethod("addURL", new Class[]{URL.class});
        } catch (NoSuchMethodException | SecurityException e) {
            //Method is always defined.
        }
        method.setAccessible(true);
        scanFolder(sysloader, method, directory, logger);
    }

    private static void scanFolder(Object callee, Method method, File file, Logger logger) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            for (File child : children) {
                scanFolder(callee, method, child, logger);
            }
        } else {
            try {
                method.invoke(callee, new Object[]{(new File(file.getAbsolutePath())).toURI().toURL()});
                logger.info(file.getAbsolutePath() + " ADDED TO THE CLASSPATH");
            } catch (Exception e) {
                logger.error("COULD NOT LOAD JAR " + file.getAbsolutePath(), e);
            }
        }
    }
}
