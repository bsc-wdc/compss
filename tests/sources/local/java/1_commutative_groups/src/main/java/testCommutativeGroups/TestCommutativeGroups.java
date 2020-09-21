package testCommutativeGroups;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import es.bsc.compss.api.COMPSs;
import model.MyFile;
import testCommutativeGroups.TestCommutativeGroupsImpl;


public class TestCommutativeGroups {

    public static final String FILE_NAME1 = "/tmp/sharedDisk/CGfile1.txt";
    public static final String FILE_NAME2 = "/tmp/sharedDisk/CGfile2.txt";
    public static final String FILE_NAME3 = "/tmp/sharedDisk/CGfile3.txt";
    public static final String FILE_NAME4 = "/tmp/sharedDisk/CGfile4.txt";
    public static final String FILE_NAME5 = "/tmp/sharedDisk/CGfile5.txt";
    public static final String FILE_NAME6 = "/tmp/sharedDisk/CGfile6.txt";
    public static final String FILE_NAME7 = "/tmp/sharedDisk/CGfile7.txt";
    public static final String FILE_NAME8 = "/tmp/sharedDisk/CGfile8.txt";
    public static final String FILE_NAME9 = "/tmp/sharedDisk/CGfile9.txt";

    public static final int N = 3;
    public static final int M = 4;
    public static final int MAX_AVAILABLE = 1;


    public static void main(String[] args) throws Exception {

        System.out.println("[LOG] Test task creation commutative");
        testTaskCreationCommutative();

        System.out.println("[LOG] Test DIRECTION COMMUTATIVE");
        testDirectionCommutative();

        System.out.println("[LOG] Test PSCO INOUT-CONCURRENT");
        testPSCOINOUTCommutative();

        System.out.println("[LOG] Test PSCO CONCURRENT-INOUT");
        testPSCOCommutativeINOUT();

    }

    private static void testTaskCreationCommutative() throws Exception {
        Integer[] a = new Integer[5];
        newFile(FILE_NAME9, true);
        TestCommutativeGroupsImpl.writeOne(FILE_NAME9);
        for (int i = 1; i < 5; i++) {
            a[i] = TestCommutativeGroupsImpl.task(i);
            TestCommutativeGroupsImpl.reduce_and_check_task(FILE_NAME9, a[i]);
            System.out.println("Round " + i);
        }
        TestCommutativeGroupsImpl.readFile(FILE_NAME9);
    }

    private static void testDirectionCommutative() throws Exception {
        // Initialize test file
        newFile(FILE_NAME1, false);
        newFile(FILE_NAME2, false);
        newFile(FILE_NAME3, false);
        newFile(FILE_NAME4, false);
        newFile(FILE_NAME5, true);
        newFile(FILE_NAME6, false);

        TestCommutativeGroupsImpl.writeOne(FILE_NAME6);

        // Launch tasks writing 2 in file 1 and 2
        System.out.println("[LOG] Write two");
        TestCommutativeGroupsImpl.writeTwoSlow(FILE_NAME1);
        TestCommutativeGroupsImpl.writeTwoSlow(FILE_NAME2);

        // Launch tasks writing 1 in file 3 and 4
        System.out.println("[LOG] Write one");
        TestCommutativeGroupsImpl.writeOne(FILE_NAME3);
        TestCommutativeGroupsImpl.writeOne(FILE_NAME4);

        // Launch 2 commutative tasks which write the sum of the numbers of the first two files to the third
        System.out.println("[LOG] Write commutative");
        TestCommutativeGroupsImpl.writeCommutative(FILE_NAME1, FILE_NAME2, FILE_NAME5);
        System.out.println("[LOG] Write commutative");
        TestCommutativeGroupsImpl.writeCommutative(FILE_NAME3, FILE_NAME4, FILE_NAME5);

        // Check results of file 5
        System.out.println("[LOG] Checking result");
        int result = TestCommutativeGroupsImpl.checkContents(FILE_NAME5);

        // Check result of file 5
        int M = 6;
        if (result != M) {
            throw new Exception("Incorrect number: " + result + "(expected 6)");
        }

        // Launch 3 commutative tasks adding one to the number of files
        System.out.println("[LOG] Add one commutative");
        TestCommutativeGroupsImpl.addOneCommutative(FILE_NAME5);
        System.out.println("[LOG] Add one commutative");
        TestCommutativeGroupsImpl.addOneCommutative(FILE_NAME5);
        System.out.println("[LOG] Add one commutative");
        TestCommutativeGroupsImpl.addOneCommutative(FILE_NAME5);

        // Launch 3 commutative tasks to accumulate results between the two files
        System.out.println("[LOG] Accumulate commutative");
        TestCommutativeGroupsImpl.accumulateCommutative(FILE_NAME5, FILE_NAME6);
        System.out.println("[LOG] Accumulate commutative");
        TestCommutativeGroupsImpl.accumulateCommutative(FILE_NAME5, FILE_NAME6);
        System.out.println("[LOG] Accumulate commutative");
        TestCommutativeGroupsImpl.accumulateCommutative(FILE_NAME5, FILE_NAME6);

        // Launch 3 commutative tasks adding one to the number of files
        System.out.println("[LOG] Add one commutative");
        TestCommutativeGroupsImpl.addOneCommutative(FILE_NAME6);
        System.out.println("[LOG] Add one commutative");
        TestCommutativeGroupsImpl.addOneCommutative(FILE_NAME6);
        System.out.println("[LOG] Add one commutative");
        TestCommutativeGroupsImpl.addOneCommutative(FILE_NAME6);

        // System.out.println("[LOG] Checking result");
        // result = TestCommutativeImpl.checkContents(FILE_NAME5);

        // Wait on on file 6
        result = TestCommutativeGroupsImpl.readFile(FILE_NAME6);
        System.out.println("The final result is " + result);

        // Check result of file 6
        M = 31;
        if (result != M) {
            throw new Exception("Incorrect number: " + result + "(Expected 31)");
        }
    }

    private static void testPSCOCommutativeINOUT() throws Exception {
        // Initialize test file
        newFile(FILE_NAME7, true);

        // Initialize test PSCO
        String id = "myfile_" + UUID.randomUUID().toString();
        MyFile f = new MyFile(FILE_NAME7);
        f.makePersistent(id);

        // Launch N tasks writing 3 (COMMUTATIVE)
        for (int i = 0; i < N; i++) {
            f.writeThree();
        }

        // Launch N tasks writing 3 (INOUT)
        for (int i = 0; i < N; i++) {
            f.writeFour();
        }

        // Synchronize PSCO object
        System.out.println("Synchronizing PSCO...");
        int M = N + N;
        int count = f.getCount(FILE_NAME7);
        System.out.println(count + " in first ");
        if (count != M) {
            throw new Exception("Incorrect number of writers " + count);
        }
        System.out.println("[LOG][PSCO_COMMUTATIVE] There have been " + count + " writers");
    }

    private static void testPSCOINOUTCommutative() throws Exception {
        // Initialize test file
        newFile(FILE_NAME8, true);
        // Initialize test PSCO
        String id = "myfile_" + UUID.randomUUID().toString();
        MyFile f = new MyFile(FILE_NAME8);
        f.makePersistent(id);

        // Launch N tasks writing 4 (INOUT)
        for (int i = 0; i < N; i++) {
            f.writeFour();
        }
        // Launch N tasks writing 3 (COMMUTATIVE)
        for (int i = 0; i < N; i++) {
            f.writeThree();
        }

        // Synchronize PSCO object
        int M = N + N;
        int count = f.getCount(FILE_NAME8);
        System.out.println(count + " in second ");
        if (count != M) {
            throw new Exception("Incorrect number of writers " + count);
        }
        System.out.println("[LOG][PSCO_COMMUTATIVE] There have been " + count + " writers");

        // Barrier with noMoreTasks false
        COMPSs.barrier(false);
    }

    private static void newFile(String fileName, boolean create) throws IOException {
        File file = new File(fileName);
        // Delete previous occurrences of the file
        if (file.exists()) {
            file.delete();
        }
        if (create) {
            // Create the file and directories if required
            boolean createdFile = file.createNewFile();
            if (!createdFile) {
                throw new IOException("[ERROR] Cannot create test file");
            }
        }

    }

}
