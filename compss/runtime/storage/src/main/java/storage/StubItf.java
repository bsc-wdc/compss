package storage;

public interface StubItf {
	public void deletePersistent();
	public String getID();
	public void makePersistent(String id);
}
