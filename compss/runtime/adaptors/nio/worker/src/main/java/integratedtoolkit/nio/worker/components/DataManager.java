package integratedtoolkit.nio.worker.components;

import java.util.HashMap;


public class DataManager {
	
	private final HashMap<String, Object> objectCache;
	

	public DataManager() {
		objectCache = new HashMap<String, Object>();
	}
	
	public void init() throws Exception {
		
	}
	
	public void stop() {
		
	}
	
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
