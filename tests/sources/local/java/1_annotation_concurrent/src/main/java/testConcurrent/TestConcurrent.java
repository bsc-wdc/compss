package testConcurrent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import model.MyFile;


public class TestConcurrent {

    public static final String FILE_NAME1 = "/tmp/sharedDisk/file1.txt";
    public static final String FILE_NAME2 = "/tmp/sharedDisk/file2.txt";
    public static final String FILE_NAME3 = "/tmp/sharedDisk/file3.txt";

    public static final int N = 3;
    public static final int MAX_AVAILABLE = 1;


    public static void main(String[] args) throws Exception {
        System.out.println("[LOG] Test DIRECTION CONCURRENT");
        testDirectionConcurrent();

        System.out.println("[LOG] Test PSCO CONCURRENT-INOUT");
        testPSCOConcurrentINOUT();

        System.out.println("[LOG] Test PSCO INOUT-CONCURRENT");
        testPSCOINOUTConcurrent();
    }

    private static void testDirectionConcurrent() throws Exception {
        // Initialize test file
        newFile(FILE_NAME1);

        // Launch N tasks writing 1
        for (int i = 0; i < N; i++) {
            System.out.println("[LOG] Write one");
            TestConcurrentImpl.writeOne(FILE_NAME1);
        }

        // Launch N tasks writing 2
        for (int i = 0; i < N; i++) {
            System.out.println("[LOG] Write two");
            TestConcurrentImpl.writeTwo(FILE_NAME1);
        }

        // Synchronize file
        int lines = 0;
        try (FileInputStream fis = new FileInputStream(FILE_NAME1)) {
            while (fis.read() > 0) {
                lines++;
            }
            System.out.println("Final counter value is " + lines);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        }

        // Check result
        int M = N * 2;
        if (lines != M) {
            throw new Exception("Incorrect number of writers " + lines);
        }
    }

    private static void testPSCOConcurrentINOUT() throws Exception {
        // Initialize test file
        newFile(FILE_NAME2);

        // Initialize test PSCO
        String id = "myfile_" + UUID.randomUUID().toString();
        MyFile f = new MyFile(FILE_NAME2);
        f.makePersistent(id);

        // Launch N tasks writing 3 (CONCURRENT)
        for (int i = 0; i < N; i++) {
            f.writeThree();
        }
        // Launch N tasks writing 4 (INOUT)
        for (int i = 0; i < N; i++) {
            f.writeFour();
        }

        // Synchronize PSCO object
        System.out.println("Synchronizing PSCO...");
        int M = N + N;
        int count = f.getCount(FILE_NAME2);
        if (count != M) {
            throw new Exception("Incorrect number of writers " + count);
        }
        System.out.println("[LOG][PSCO_CONCURRENT] There have been " + count + " writers");
    }

    private static void testPSCOINOUTConcurrent() throws Exception {
        // Initialize test file
        newFile(FILE_NAME3);
        // Initialize test PSCO
        String id = "myfile_" + UUID.randomUUID().toString();
        MyFile f = new MyFile(FILE_NAME3);
        f.makePersistent(id);

        // Launch N tasks writing 4 (INOUT)
        for (int i = 0; i < N; i++) {
            f.writeFour();
        }
        // Launch N tasks writing 3 (CONCURRENT)
        for (int i = 0; i < N; i++) {
            f.writeThree();
        }

        // Synchronize PSCO object
        int M = N + N;
        int count = f.getCount(FILE_NAME3);
        if (count != M) {
            throw new Exception("Incorrect number of writers " + count);
        }
        System.out.println("[LOG][PSCO_CONCURRENT] There have been " + count + " writers");
    }

    private static void newFile(String fileName) throws IOException {
        File file = new File(fileName);
        // Delete previous occurrences of the file
        if (file.exists()) {
            file.delete();
        }
        // Create the file and directories if required
        boolean createdFile = file.createNewFile();
        if (!createdFile) {
            throw new IOException("[ERROR] Cannot create test file");
        }
    }

}
