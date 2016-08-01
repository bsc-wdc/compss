package storage.interfaces;


public interface StubItf {

	public abstract void deletePersistent();

	public abstract String getID();

	public abstract void makePersistent(String id);

}
