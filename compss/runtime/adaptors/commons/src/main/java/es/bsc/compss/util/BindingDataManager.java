package es.bsc.compss.util;

import java.nio.ByteBuffer;

public class BindingDataManager {
    
    public native static boolean isInBinding(String id);
    
    public native static int removeData(String id);
    
    public native static int copyCachedData(String from_id, String to_id);
    
    public native static int moveCachedData(String from_id, String to_id);
    
    public native static int storeInFile(String id, String filepath);
    
    public native static int loadFromFile(String id, String filepath, int type, int elements);
    
    public native static ByteBuffer getByteArray(String id);
    
    public native static int setByteArray(String id, ByteBuffer b, int type, int elements);
    
    static {
        System.loadLibrary("bindings_common");
    }
}
