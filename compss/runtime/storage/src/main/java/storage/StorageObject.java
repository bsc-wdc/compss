package storage;


public class StorageObject implements StubItf {

	/**
	 * Returns the persistent object ID
	 * 
	 * @return
	 */
	public String getID() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Persist the object
	 * 
	 * @param id
	 */
	public void makePersistent(String id) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Deletes the persistent object occurrences
	 */
	public void deletePersistent() {
		throw new UnsupportedOperationException();
	}

}
