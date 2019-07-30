package testTimeOut;

import java.io.File;
import java.io.IOException;


public class TestTimeOut {

    public static final int N = 3;
    public static final int M = 4;
    public static final int MAX_AVAILABLE = 1;

    public static final String FILE_NAME = "/tmp/sharedDisk/taskGroups.txt";


    public static void main(String[] args) throws Exception {
        newFile(FILE_NAME, true);

        System.out.println("[LOG] Test task time out");
        testTaskTimeOut();
    }

    // Two tasks to check time out. The second takes more time than expected
    private static void testTaskTimeOut() throws Exception {
        TestTimeOutImpl.timeOutTaskFast(FILE_NAME);
        TestTimeOutImpl.timeOutTaskSlow(FILE_NAME);
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
