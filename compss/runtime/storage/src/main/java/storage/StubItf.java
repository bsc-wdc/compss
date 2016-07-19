package storage;


public interface StubItf {

        public abstract void deletePersistent();

        public abstract String getID();

        public abstract void makePersistent(String id);

}

