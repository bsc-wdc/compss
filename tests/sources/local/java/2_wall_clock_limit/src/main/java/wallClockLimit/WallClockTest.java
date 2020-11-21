package wallClockLimit;

import java.io.File;
import java.io.IOException;

import es.bsc.compss.api.COMPSs;
import es.bsc.compss.api.COMPSsGroup;
import es.bsc.compss.worker.COMPSsException;


public class WallClockTest {

    public static final int N = 3;
    public static final int M = 4;

    public static final String FILE_NAME = "/tmp/sharedDisk/taskGroups.txt";


    public static void main(String[] args) throws Exception {
        newFile(FILE_NAME, true);

        System.out.println("[LOG] Test wall clock limit");
        testCancelation();

    }

    private static void testCancelation() throws Exception {

        System.out.println("Executing tasks...");
        // Long tasks that will be cancelled while being executed
        WallClockTestImpl.longTask(FILE_NAME);
        WallClockTestImpl.longTask(FILE_NAME);
        // Short task correctly executed
        WallClockTestImpl.executedTask(FILE_NAME);
        // The exception is thrown by the second task of the group
        WallClockTestImpl.inoutLongTask(FILE_NAME);
        // These two tasks are cancelled before being executed
        WallClockTestImpl.cancelledTask(FILE_NAME);
        WallClockTestImpl.cancelledTask(FILE_NAME);

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
