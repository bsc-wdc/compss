package checkpointObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import checkpointObject.SimpObj;
import es.bsc.compss.api.COMPSs;


public class Simple {

    private static final String fileName = "counter";


    public static void main(String[] args) throws Exception {
        // Check and get parameters
        if (args.length != 1) {
            throw new Exception("[ERROR] Incorrect number of parameters");
        }
        int exception = Integer.parseInt(args[0]);
        // Write value
        SimpObj initialValue = new SimpObj(1, "1");

        // Write value
        System.out
            .println("Initial counter value is " + initialValue.getCounter() + " chars: " + initialValue.getChars());

        // Execute increment
        SimpObj initialValue1 = SimpleImpl.increment(initialValue);
        SimpObj initialValue2 = SimpleImpl.increment(initialValue1);
        if (exception == 1) {
            throw new Exception("Incorrect number of writers ");
        }
        SimpObj initialValue3 = SimpleImpl.increment(initialValue2);

        System.out
            .println("Final counter value is " + initialValue3.getCounter() + " chars: " + initialValue3.getChars());

    }

}
