package testConcurrent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import model.MyFile;


public class TestConcurrent {

   public static String fileName = "/tmp/sharedDisk/file.txt";
//    public static String fileName= "/tmp/trial/file.txt";
    public static int N = 3;
    public static int MAX_AVAILABLE = 1;
    
    public static void main(String[] args) throws Exception {
        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test DIRECTION CONCURRENT");
        testDirectionConcurrent();
        System.out.println("[LOG] Test PSCO CONCURRENT");
        testPSCOConcurrent();
        
    }
    
    private static void newFile() {
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

    }

    private static void testDirectionConcurrent() throws Exception {
    
        newFile(); 
        
        for (int i = 0; i < N; i++) {
            TestConcurrentImpl.write_one(fileName);

            System.out.println("[LOG]Write one");
        }
       

        for (int i=0; i < N; i++) {
            TestConcurrentImpl.write_two(fileName);
            System.out.println("[LOG]Write two");
        }
        
        FileInputStream fis;
        int lines = 0;
        try {
            fis = new FileInputStream(fileName);
            while (fis.read()>0) lines++;
            System.out.println("Final counter value is " + lines);
            fis.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        int M = N * 2;
        if (lines != M) {
            throw new Exception("Incorrect number of writers " + lines);
        }
        
    }
        
    private static void testPSCOConcurrent() throws Exception {
        
        newFile(); 
        
        String id = "myfile_" + UUID.randomUUID().toString();
        MyFile f = new MyFile(fileName);
        f.makePersistent(id);
        for (int i = 0; i < N; i++) {
            f.writeThree();
        }
        for (int i = 0; i < N; i++) {
            f.writeFour();
        }
        if (f.getCount(fileName)!=N+N) {
            throw new Exception("Incorrect number of writers " + f.getCount(fileName));
        }
        System.out.println("[LOG][PSCO_CONCURRENT] There have been " + f.getCount(fileName) + " writers");
        
        String id2 = "myfile_" + UUID.randomUUID().toString();
        MyFile f2 = new MyFile(fileName);
        f2.deleteContents();
        f2.makePersistent(id2);
        for (int i = 0; i < N; i++) {
            f2.writeFour();
        }
        for (int i = 0; i < N; i++) {
            f.writeThree();
        }
        int M = N + N;
        if (f2.getCount(fileName) != M) {
            throw new Exception("Incorrect number of writers " + f2.getCount(fileName));
        }
        System.out.println("[LOG][PSCO_CONCURRENT] There have been " + f2.getCount(fileName) + " writers");    
    }

}
