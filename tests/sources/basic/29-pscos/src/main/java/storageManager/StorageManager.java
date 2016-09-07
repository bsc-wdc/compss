package storageManager;

import java.io.File;
import java.io.IOException;
import java.util.List;

import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


/*
 * WARN: THIS STORAGE MANAGER IS DESIGNED TO BE USED WITH THE
 *       COMPSS DUMMY STORAGE (trunk/utils/storage/dummyPSCO)
 */
public class StorageManager {

    private static final String ERROR_SERIALIZE = "ERROR: Cannot serialize object to id=";
    private static final String BASE_WORKING_DIR = File.separator + "tmp" + File.separator + "PSCO" + File.separator;


    public static void persist(StubItf o) throws StorageException {
        String id = o.getID();
        if (id != null) {
            List<String> hostnames = StorageItf.getLocations(id);
            for (String h : hostnames) {
                String path = BASE_WORKING_DIR + h + File.separator + id;
                try {
                    Serializer.serialize(o, path);
                } catch (IOException e) {
                    throw new StorageException(ERROR_SERIALIZE + id, e);
                }
            }
        }
    }

}
