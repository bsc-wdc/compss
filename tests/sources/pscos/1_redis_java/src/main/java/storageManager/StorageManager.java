package storageManager;

import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


/*
 * WARN: THIS STORAGE MANAGER IS DESIGNED TO BE USED WITH THE
 *       COMPSS REDIS STORAGE (trunk/utils/storage/redisPSCO)
 */
public class StorageManager {

    public static void persist(StubItf o) throws StorageException {
        StorageItf.newVersion(o.getID(), false, "none");
    }

}
