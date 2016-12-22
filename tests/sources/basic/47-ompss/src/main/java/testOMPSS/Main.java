package testOMPSS;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import ompss.OMPSS;


public class Main {

    private static final int WAIT_RUNTIME = 5_000;


    public static void main(String[] args) {

        // ----------------------------------------------------------------------------
        // Wait for Runtime to have the worker available
        System.out.println("[LOG] Wait for Runtime to be ready");
        try {
            Thread.sleep(WAIT_RUNTIME);
        } catch (InterruptedException e) {
            // No need to handle such exception
        }

        // ----------------------------------------------------------------------------
        // Launch a OMPSS task
        System.out.println("[LOG] Launch OMPSS task");
        testOMPSS();
    }

    private static void testOMPSS() {
        // Task call
        String msg = "Hello World!";
        String outputFile = "output.txt";
        int ev = OMPSS.ompssTask(msg, outputFile);

        // Synchronize result
        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }
        try (BufferedReader br = new BufferedReader(new FileReader(outputFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("[RESULT] OMPSS Task: " + line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot read output file " + outputFile);
            System.exit(1);
        }
    }

}
