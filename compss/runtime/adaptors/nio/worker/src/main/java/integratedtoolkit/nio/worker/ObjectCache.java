package integratedtoolkit.nio.worker;

import java.util.HashMap;


public class ObjectCache {

    private final HashMap<String, Object> objectCache = new HashMap<String, Object>();

    public synchronized void store(String name, Object value) {
        try {
            objectCache.put(name, value);
        } catch (NullPointerException e) {
            System.err.println("Object Cache " + objectCache + " dataId " + name + " object " + value);
        }
    }

    public synchronized Object get(String name) {
        return objectCache.get(name);
    }

    public synchronized void remove(String name) {
        objectCache.remove(name);
    }

    public synchronized boolean checkPresence(String name) {
        return objectCache.containsKey(name);
    }

}
