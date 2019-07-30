package onFailureIgnore;

import es.bsc.compss.api.COMPSs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


public class OnFailureIgnore {

    private static final String FILE_NAME = "/tmp/sharedDisk/onFailure1.txt";
    private static final String FILEOUT_NAME1 = "/tmp/sharedDisk/onFailureOut1.txt";
    private static final String FILEOUT_NAME2 = "/tmp/sharedDisk/onFailureOut2.txt";
    private static final int M = 5; // number of tasks to be executed


    public static void main(String[] args) {

        // Failure ignored behavior
        System.out.println("Init on failure : IGNORE FAILURE");
        try {
            onIgnoreFailureFileOutNotGenerated();
            onIgnoreFailure();
        } catch (numberException e) {
            catchException(e);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private static void onIgnoreFailureFileOutNotGenerated() throws numberException {
        deleteFile(FILEOUT_NAME1);
        deleteFile(FILEOUT_NAME2);
        OnFailureIgnoreImpl.processOutParamIgnoreFailure(FILEOUT_NAME1, FILEOUT_NAME2);
        COMPSs.barrier();
    }

    // Ignore the task failure and continue with other tasks execution
    private static void onIgnoreFailure() throws Exception {

        // Create and write first number to file
        initFiles();
        writeFile();

        // Process file contents and if failed continue executing other tasks
        int i = 0;
        for (i = 0; i < M; i++) {
            OnFailureIgnoreImpl.processParamIgnoreFailure(FILE_NAME);
        }

        // Wait for all tasks to finish
        COMPSs.barrier();

        // Get the renamed file
        COMPSs.getFile(FILE_NAME);

        // Shell commands to execute to check contents of file
        ArrayList<String> commands = new ArrayList<String>();
        commands.add("/bin/cat");
        commands.add(FILE_NAME);

        // ProcessBuilder start
        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.redirectErrorStream(true);
        Process process = null;
        try {
            process = pb.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Read file content
        readContents(process);

        // Check result
        try {
            if (process.waitFor() != 0) {
                System.out.println("Error: Process cat return value different from 0");
                System.exit(1);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void readContents(Process process) throws Exception {
        // Read output of file
        StringBuilder out = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = null, previous = null;
        int count = 0;
        try {
            while ((line = br.readLine()) != null)
                if (!line.equals(previous)) {
                    previous = line;
                    out.append(line).append('\n');
                    count = count + 1;
                }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Exception if number of writers has not been correct
        if (count != 1) {
            throw new Exception("Incorrect number of writers " + count);
        }
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

    private static void deleteFile(String fileName) {
        File file = new File(fileName);
        // Delete previous occurrences of the file
        if (file.exists()) {
            file.delete();
        }
    }

    private static void catchException(numberException e) {
        System.out.println("exception caught!");
    }

}
