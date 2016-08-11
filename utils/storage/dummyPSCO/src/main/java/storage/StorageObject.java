package storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StorageObject implements StubItf {
	
	// Logger: According to Loggers.STORAGE
    private static final Logger logger = LogManager.getLogger("integratedtoolkit.Storage");

	private String id = null;

	/**
	 * Returns the persistent object ID
	 * 
	 * @return
	 */
	public String getID() {
		return this.id;
	}

	/**
	 * Persist the object
	 * 
	 * @param id
	 */
	public void makePersistent(String id) {
		try {
			StorageItf.makePersistent(this, id);
			this.id = id;
		} catch (StorageException e) {
			logger.error("Exception serializing object", e);
		}
	}

	/**
	 * Deletes the persistent object occurrences
	 */
	public void deletePersistent() {
		StorageItf.removeById(this.id);
		this.id = null;
	}

}
