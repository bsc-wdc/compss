package testConcurrent;

import storage.StorageException;
import storageManager.StorageManager;
import model.MyFile;


public class TestConcurrentImpl {

    private static final String ERROR_PERSIST = "[ERROR] Cannot persist object";


    public static void taskPSCOConcurrent(MyFile f) {
        f.writeOne(); 
        // Manually persist object to storage
        try {
            StorageManager.persist(f);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }
    }

}
