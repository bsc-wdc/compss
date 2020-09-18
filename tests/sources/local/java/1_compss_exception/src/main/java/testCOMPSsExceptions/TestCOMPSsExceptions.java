package testCOMPSsExceptions;

import java.io.File;
import java.io.IOException;

import es.bsc.compss.api.COMPSs;
import es.bsc.compss.api.COMPSsGroup;
import es.bsc.compss.worker.COMPSsException;


public class TestCOMPSsExceptions {

    public static final int N = 3;
    public static final int M = 4;
    public static final int MAX_AVAILABLE = 1;

    public static final String FILE_NAME = "/tmp/sharedDisk/taskGroups.txt";


    public static void main(String[] args) throws Exception {
        newFile(FILE_NAME, true);

        System.out.println("[LOG] Test task group exceptions");
        testGroupExceptions();

        System.out.println("[LOG] Test task group exceptions");
        testGroupExceptionsBarrier();

        COMPSs.getFile(FILE_NAME);
    }

    private static void testGroupExceptions() throws InterruptedException {
        try (COMPSsGroup a = new COMPSsGroup("FailedGroup", true)) {
            System.out.println("Executing write One ");
            for (int j = 0; j < N; j++) {
                TestCOMPSsExceptionsImpl.writeOne(FILE_NAME);
            }
        } catch (COMPSsException e) {
            TestCOMPSsExceptionsImpl.writeThree(FILE_NAME);
            System.out.println("Exception caught!!");

        } catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            TestCOMPSsExceptionsImpl.writeFour(FILE_NAME);
        }
    }

    private static void testGroupExceptionsBarrier() throws InterruptedException {
        try (COMPSsGroup a = new COMPSsGroup("FailedGroup2", false)) {
            System.out.println("Executing write One ");
            for (int j = 0; j < N; j++) {
                TestCOMPSsExceptionsImpl.writeOne(FILE_NAME);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // The group exception will be thrown from the barrier
        try {
            COMPSs.barrierGroup("FailedGroup2");
        } catch (COMPSsException e) {
            System.out.println("Exception caught in barrier!!");
            TestCOMPSsExceptionsImpl.writeThree(FILE_NAME);
        } finally {
            TestCOMPSsExceptionsImpl.writeFour(FILE_NAME);
        }
    }

    // Creation of a new blank file
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
