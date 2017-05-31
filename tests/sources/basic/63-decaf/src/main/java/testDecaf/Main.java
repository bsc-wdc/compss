package testDecaf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import decaf.Decaf;


public class Main {

    private static final int SLEEP_TIME = 5_000;

    private static final int N = 8;


    public static void main(String[] args) {
        // ------------------------------------------------------------------------
        System.out.println("[LOG] Check args");
        if (args.length != 0) {
            usage();
        }

        // ------------------------------------------------------------------------
        // Wait for workers to load to ensure both of them are available during the test
        System.out.println("[LOG] Wait workers to initialize");
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
            // No need to handle such exception
        }

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test Decaf with 1 node");
        testDecafSingleNode();

        System.out.println("[LOG] Test Decaf with 2 node");
        testDecafMultipleNodes();

	System.out.println("[LOG] Test Concurrent Decaf with 2 node");
        testDecafConcurrentMultipleNodes();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Decaf Test finished");
    }

    private static void usage() {
        System.err.println("ERROR: Invalid arguments");
        System.err.println("Usage: main");

        System.exit(1);
    }

    private static void testDecafSingleNode() {
        String outputFile = "decafSingleOutput.txt";
        int ev = Decaf.taskSingleDecaf(outputFile);

        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }
        try (BufferedReader br = new BufferedReader(new FileReader(outputFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("[RESULT] Decaf Task1: " + line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot read output file " + outputFile);
            System.exit(1);
        }
        System.out.println("[LOG] Result must be checked on result script");
    }

    private static void testDecafMultipleNodes() {
        String outputFile = "decafMultipleOutput.txt";
        Integer ev = Decaf.taskMultipleDecaf(outputFile);

        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }
        try (BufferedReader br = new BufferedReader(new FileReader(outputFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("[RESULT] Decaf Task2: " + line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot read output file " + outputFile);
            System.exit(1);
        }
        System.out.println("[LOG] Result must be checked on result script");
    }

    private static void testDecafConcurrentMultipleNodes() {
        String outputFile1 = "decafMultipleOutput1.txt";
        String outputFile2 = "decafMultipleOutput2.txt";
        Integer ev1 = Decaf.taskConcurrentMultipleDecaf(outputFile1);
        Integer ev2 = Decaf.taskConcurrentMultipleDecaf(outputFile2);

        if (ev1 != 0 || ev2 != 0) {
            System.err.println("[ERROR] One process returned non-zero exit value: " + ev1 + " or " + ev2);
            System.exit(1);
        }
        try (BufferedReader br = new BufferedReader(new FileReader(outputFile1))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("[RESULT] Decaf CONC Task1: " + line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot read output file " + outputFile1);
            System.exit(1);
        }
        try (BufferedReader br = new BufferedReader(new FileReader(outputFile2))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("[RESULT] Decaf CONC Task2: " + line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot read output file " + outputFile2);
            System.exit(1);
        }

        System.out.println("[LOG] Result must be checked on result script");
    }

}
