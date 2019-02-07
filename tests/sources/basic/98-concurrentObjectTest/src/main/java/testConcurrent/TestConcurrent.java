package testConcurrent;

import java.io.File;
import java.io.IOException;

import model.MyFile;


public class TestConcurrent {

    public static String fileName = "/tmp/sharedDisk/file.txt";

    public static void main(String[] args) {
        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO INOUT");
        testPSCOConcurrent();
    }

    private static void testPSCOConcurrent() {
        
        File file = new File(fileName);
        file.mkdirs();
        if (file.exists()) {
            file.delete();
        }
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        new File(fileName);

        MyFile f = new MyFile(fileName);
       // f.makePersistent(fileName);
        for (int i = 0; i < 4; i++) {
            TestConcurrentImpl.taskPSCOConcurrent(f);
        }

        System.out.println("[LOG][PSCO_CONCURRENT] There have been " + f.getCount(fileName) + " writers");
    }

}
