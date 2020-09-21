package onFailureFail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import es.bsc.compss.api.*;


public class OnFailureFail {

    private static final String FILE_NAME = "/tmp/sharedDisk/onFailure1.txt";
    private static final int M = 5; // number of tasks to be executed


    public static void main(String[] args) {

        // Direct fail behavior
        System.out.println("Init on failure : DIRECT FAIL");
        try {
            onDirectFail();
        } catch (numberException e) {
            catchException(e);
        }
    }

    // If task fails, no retries
    private static void onDirectFail() throws numberException {

        // Create and write first number to file
        initFiles();
        writeFile();

        // Process file contents and end execution if a task fails
        int i = 0;
        for (i = 0; i < M; i++) {
            OnFailureFailImpl.processParamDirectFail(FILE_NAME);
        }

        // Wait for all tasks to finish
        COMPSs.barrier();
    }

    private static void initFiles() {
        // Initialize the test file
        try {
            newFile(FILE_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeFile() throws numberException {
        // Write first number to file
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(FILE_NAME, false));
            writer.write(String.valueOf(1));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Function that creates a new empty file
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

    private static void catchException(numberException e) {
        System.out.println("exception caught!");
    }

}
