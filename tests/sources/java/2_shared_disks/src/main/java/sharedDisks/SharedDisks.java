package sharedDisks;

import java.io.File;
import java.io.FileOutputStream;


public class SharedDisks {

    public static void main(String[] args) {
        // Set parameters
        String fileNameInput = "/tmp/sharedDisk/input_file_test";
        String fileNameOutput = "output_file_test";

        // INPUT TEST
        System.out.println("[LOG] Input file test");
        System.out.println("[LOG] Writing input file from master.");
        try {
            FileOutputStream fos = new FileOutputStream(fileNameInput, false);
            fos.write(1);
            fos.close();
        } catch (Exception ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
        File f = new File(fileNameInput);
        if (!f.exists()) {
            System.out.println("[ERROR] File doesn't exists");
            System.exit(-1);
        }

        System.out.println("[LOG] Reading input file from worker.");
        int retVal = SharedDisksImpl.inputTask(fileNameInput, fileNameInput);

        if (retVal == 0) {
            System.out.println("[SUCCESS] File shared correctly.");
        } else if (retVal == -1) {
            System.out.println("[ERROR] The file is not beeing shared correctly.");
        } else {
            System.out.println("[ERROR] Unknown");
        }

        // OUTPUT TEST
        System.out.println("[LOG] Output file test");
        SharedDisksImpl.outputTaskWriter(fileNameOutput);
        SharedDisksImpl.outputTaskReader(fileNameOutput);

        System.out.println("[LOG] All tasks created.");
        System.out.println("[LOG] No more jobs for main. Waiting all tasks to finish.");
    }

}
