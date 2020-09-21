package nonNativeTasks;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import binary.BINARY;
import es.bsc.compss.api.COMPSs;


public class Main {

    private static final int WAIT_RUNTIME = 5_000;

    private static final String FILE_IN = "input.txt";
    private static final String FILE_OUT = "output.txt";
    private static final String FILE_ERR = "error.txt";

    private static String message;
    private static String input;


    public static void main(String[] args) {
        // ----------------------------------------------------------------------------
        // Initialize data
        System.out.println("[LOG] Initialize data");
        initializeData();

        // ----------------------------------------------------------------------------
        // Wait for Runtime to have the worker available
        System.out.println("[LOG] Wait for Runtime to be ready");
        try {
            Thread.sleep(WAIT_RUNTIME);
        } catch (InterruptedException e) {
            // No need to handle such exception
        }

        // ------------------------------------------------------------------------
        System.out.println("");
        System.out.println("[LOG] Test Return Types");
        testReturnType();

        // ------------------------------------------------------------------------
        System.out.println("");
        System.out.println("[LOG] Test prefixes");
        testPrefixes();

        // ------------------------------------------------------------------------
        System.out.println("");
        System.out.println("[LOG] Test INPUT Redirection");
        testInputRedirection();

        // ------------------------------------------------------------------------
        System.out.println("");
        System.out.println("[LOG] Test OUTPUT Redirection");
        testOutputRedirection();

        // ------------------------------------------------------------------------
        System.out.println("");
        System.out.println("[LOG] Test ERROR Redirection");
        testErrorRedirection();

        // ------------------------------------------------------------------------
        System.out.println("");
        System.out.println("[LOG] Test Complex mode");
        testComplexCalls();

        // ------------------------------------------------------------------------
        System.out.println("");
        System.out.println("[LOG] Non-Native tasks Test finished");
    }

    private static void initializeData() {
        message = "Hello World";
        input = "Good Bye";

        // Create data as file
        try (BufferedWriter outputWriter = new BufferedWriter(new FileWriter(FILE_IN))) {
            outputWriter.write(input);
            outputWriter.newLine();
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot write data file " + FILE_IN);
            ioe.printStackTrace();
            System.exit(1);
        }
    }

    private static void testReturnType() {
        // Task without EV
        System.out.println("[LOG] Launch EV");
        BINARY.simpleTask1(message);

        COMPSs.barrier();

        // Task with EV
        System.out.println("[LOG] Launch EV");
        int ev = BINARY.simpleTask2(message);

        System.out.println("[LOG] EV = " + ev);

        // Task with EV syncrhonized on access
        System.out.println("[LOG] Launch EV");
        Integer ev2 = BINARY.simpleTask3(message);
        System.out.println("[LOG] EV = " + ev2);
    }

    private static void testPrefixes() {
        // Task without EV
        System.out.println("[LOG] Launch Prefixes");
        BINARY.taskWithPrefix(message);

        COMPSs.barrier();
    }

    private static void testInputRedirection() {
        // Task with FILE IN redirection
        System.out.println("[LOG] Launch FILE IN redirection");
        BINARY.taskSTDINFileRedirection(message, FILE_IN);

        System.out.println("[LOG] Wait for FILE IN redirection");
        COMPSs.barrier();

        // ------------------------------------------------------------------------
        // Task with FILE IN redirection and EV
        System.out.println("[LOG] Launch FILE IN redirection and EV");
        int ev = BINARY.taskSTDINFileRedirectionWithEV(message, FILE_IN);

        System.out.println("[LOG] FILE IN - EV = " + ev);
    }

    private static void testOutputRedirection() {
        // Task with FILE OUT redirection
        System.out.println("[LOG] Launch FILE OUT redirection");
        BINARY.taskSTDOUTFileRedirection(message, FILE_OUT);

        try {
            FileDumper.dumpFile("[LOG] Content FILE OUT redirection:", FILE_OUT);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_OUT);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection and EV
        System.out.println("[LOG] Launch FILE OUT redirection and EV");
        int ev = BINARY.taskSTDOUTFileRedirectionWithEV(message, FILE_OUT);

        System.out.println("[LOG] FILE OUT redirection - EV = " + ev);
        try {
            FileDumper.dumpFile("[LOG] Content FILE OUT redirection - EV:", FILE_OUT);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_OUT);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection append mode
        System.out.println("[LOG] Launch FILE OUT redirection append");
        BINARY.taskSTDOUTFileRedirectionAppend(message, FILE_OUT);

        try {
            FileDumper.dumpFile("[LOG] Content FILE OUT redirection append:", FILE_OUT);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_OUT);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection append mode and EV
        System.out.println("[LOG] Launch FILE OUT redirection append and EV");
        int ev2 = BINARY.taskSTDOUTFileRedirectionWithEVAppend(message, FILE_OUT);

        System.out.println("[LOG] FILE OUT redirection append - EV = " + ev2);
        try {
            FileDumper.dumpFile("[LOG] Content FILE OUT redirection append - EV:", FILE_OUT);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_OUT);
            System.exit(1);
        }
    }

    private static void testErrorRedirection() {
        // Task with FILE ERR redirection
        System.out.println("[LOG] Launch FILE ERR redirection");
        BINARY.taskSTDERRFileRedirection(message, FILE_ERR);

        try {
            FileDumper.dumpFile("[LOG] Content FILE ERR redirection:", FILE_ERR);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_ERR);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection and EV
        System.out.println("[LOG] Launch FILE ERR redirection and EV");
        int ev = BINARY.taskSTDERRFileRedirectionWithEV(message, FILE_ERR);

        System.out.println("[LOG] FILE ERR redirection - EV = " + ev);
        System.out.println("[LOG] Content FILE ERR redirection - EV:");
        try {
            FileDumper.dumpFile("[LOG] Content FILE ERR redirection - EV:", FILE_ERR);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_ERR);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection append mode
        System.out.println("[LOG] Launch FILE ERR redirection append");
        BINARY.taskSTDERRFileRedirectionAppend(message, FILE_ERR);

        System.out.println("[LOG] Content FILE ERR redirection append:");
        try {
            FileDumper.dumpFile("[LOG] Content FILE ERR redirection append:", FILE_ERR);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_ERR);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection append mode and EV
        System.out.println("[LOG] Launch FILE ERR redirection append and EV");
        int ev2 = BINARY.taskSTDERRFileRedirectionWithEVAppend(message, FILE_ERR);

        System.out.println("[LOG] FILE ERR redirection append - EV = " + ev2);
        try {
            FileDumper.dumpFile("[LOG] Content FILE ERR redirection append - EV:", FILE_ERR);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_ERR);
            System.exit(1);
        }
    }

    private static void testComplexCalls() {
        // Full task with no EV
        System.out.println("[LOG] Launch FULL redirection");
        BINARY.fullTask1(message, FILE_IN, FILE_OUT, FILE_ERR);

        try {
            FileDumper.dumpFile("[LOG] Content FULL OUT:", FILE_OUT);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_OUT);
            System.exit(1);
        }
        try {
            FileDumper.dumpFile("[LOG] Content FULL ERR:", FILE_ERR);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_ERR);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Full task with no EV
        System.out.println("[LOG] Launch FULL2 redirection");
        int ev = BINARY.fullTask2(message, FILE_IN, FILE_OUT, FILE_ERR);

        System.out.println("[LOG] FULL2 - EV = " + ev);
        try {
            FileDumper.dumpFile("[LOG] Content FULL2 OUT:", FILE_OUT);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_OUT);
            System.exit(1);
        }
        try {
            FileDumper.dumpFile("[LOG] Content FULL2 ERR:", FILE_ERR);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + FILE_ERR);
            System.exit(1);
        }
    }

}
