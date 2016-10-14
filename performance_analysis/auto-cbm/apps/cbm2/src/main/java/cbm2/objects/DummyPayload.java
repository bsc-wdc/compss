package cbm2.objects;

import java.io.Serializable;
import java.util.Random;


//This is a class that contains an array of bytes
//Just made it to be sure COMPSs can pass it by reference n stuff
public class DummyPayload implements Serializable {

    /**
     * SerialId for objects outside the runtime
     */
    private static final long serialVersionUID = 3L;

    public int size;
    private byte[] payload;
    
    public DummyPayload() {
        size = 1;
        payload = new byte[1];
    }

    public DummyPayload(int sizeInBytes) {
        regen(sizeInBytes);
    }

    public void regen(int sizeInBytes) {
        size = sizeInBytes;
        payload = new byte[sizeInBytes];
        new Random().nextBytes(payload); // fill with random bytes
    }

    public void foo() {
        // For sync
    } 

}
