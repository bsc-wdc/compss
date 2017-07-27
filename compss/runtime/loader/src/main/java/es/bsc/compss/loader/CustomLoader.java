package es.bsc.compss.loader;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;


/*
 * Since this custom loader extends URLClassLoader, it will only take into
 * account the jars specified with the method "addFile" when searching for
 * classes (therefore, the classpath variable will not be taken into account).
 */
public class CustomLoader extends URLClassLoader {

    public CustomLoader(URL[] urls) {
        super(urls);
    }

    public void addFile(String path) throws MalformedURLException {
        String urlPath = "file:" + path;
        addURL(new URL(urlPath));
    }

    /*
     * When requested to load a class, by default a class loader delegates the search for the class to its parent class
     * loader before attempting to find the class itself. Furthermore, if a class loader CL is requested to load a class
     * C (then, it is the initiator of C) and it delegates to its parent class loader PL, then CL is never requested to
     * load any class referred to in the definition of the class C (CL will not be the initiator of those classes).
     * Instead, the parent class loader PL becomes their initiator and will be requested to load them. Notice, then, the
     * difference between the initiator of a class and its real loader. Therefore, we must override the loadClass method
     * to avoid delegation in certain cases.
     * 
     * @see java.lang.ClassLoader#loadClass(java.lang.String)
     */
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        // Have we already loaded this class?
        Class<?> c = findLoadedClass(name);

        if (c == null) {
            if (name.startsWith(LoaderConstants.CUSTOM_LOADER_PREFIX) || name.startsWith("javassist")) {
                /*
                 * Avoid delegation: This custom loader must load the ITModifier and ITApplicationEditor classes and the
                 * ones from our version of javassist
                 */
                c = findClass(name);
                return c;
            } else {
                // Let the rest of classes be loaded by the parent loader
                return getParent().loadClass(name);
            }
        } else
            return c;
    }

}
