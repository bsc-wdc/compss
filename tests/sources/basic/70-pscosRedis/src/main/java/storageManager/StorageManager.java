package storageManager;

import java.io.File;
import java.io.IOException;
import java.util.List;

import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


/*
 * WARN: THIS STORAGE MANAGER IS DESIGNED TO BE USED WITH THE
 *       COMPSS REDIS STORAGE (trunk/utils/storage/redisPSCO)
 */
public class StorageManager {

    private static final String PSCO_EXTENSION = ".PSCO";

    private static final String ERROR_SERIALIZE = "ERROR: Cannot serialize object to id=";
    private static final String BASE_WORKING_DIR = File.separator + "tmp" + File.separator + "PSCO" + File.separator;


    public static void persist(StubItf o) throws StorageException {
        StorageItf.newVersion(o.getID(), false, "none");
    }

}
