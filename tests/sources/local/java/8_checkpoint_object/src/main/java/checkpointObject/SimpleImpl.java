package checkpointObject;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import checkpointObject.SimpObj;


public class SimpleImpl {

    public static SimpObj increment(SimpObj counter) {
        // Read value
        counter.setCounter(counter.getCounter() + 1);
        counter.setChars(counter.getChars() + "1");
        return counter;
    }
}
