package storage;

import java.util.List;

public final class StorageItf {

	private static final String STORAGE_NOT_FOUND_MESSAGE = "You are trying to start a run with "
			+ "persistent object storage but any back-end client is loaded in the classpath.";
	
	public StorageItf() {
		
	}

	public static void init(String storageConf) throws StorageException {
		throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
		
	}

	public static void finish() throws StorageException {
		throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);	
	}

	public static List<String> getLocations(String pscoId) throws StorageException {
		throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
	}

	public static void newReplica(String id, String hostName) throws StorageException {
		throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
	}

	public static String newVersion(String id, String hostName) throws StorageException {
		throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
	}

	public static Object getByID(String id) throws StorageException {
			throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
	}

	public static String executeTask(String id, String descriptor,
			Object[] values, String hostName, CallbackHandler callback) throws StorageException {
		throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
	}

	public static Object getResult(CallbackEvent event) {
		// Nothing to do
		return null;
	}
	
	
	
	
	

}
