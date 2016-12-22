package nonNativeTasks;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import binary.BINARY;
import integratedtoolkit.api.COMPSs;


public class Main {

    private static final int WAIT_RUNTIME = 5_000;
    
    private static final String fileIn = "input.txt";
    private static final String fileOut = "output.txt";
    private static final String fileErr = "error.txt";
    
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
        try (BufferedWriter outputWriter = new BufferedWriter(new FileWriter(fileIn))) {
            outputWriter.write(input);
            outputWriter.newLine();
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot write data file " + fileIn);
            ioe.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void testReturnType() {
        // Task without EV
        System.out.println("[LOG] Launch EV");
        BINARY.simpleTask1(message);

        COMPSs.waitForAllTasks();
        
        // Task with EV
        System.out.println("[LOG] Launch EV");
        int ev = BINARY.simpleTask2(message);

        System.out.println("[LOG] EV = " + ev);
    }
    


    private static void testInputRedirection() {
        // Task with FILE IN redirection
        System.out.println("[LOG] Launch FILE IN redirection");
        BINARY.taskSTDINFileRedirection(message, fileIn);

        System.out.println("[LOG] Wait for FILE IN redirection");
        COMPSs.waitForAllTasks();

        // ------------------------------------------------------------------------
        // Task with FILE IN redirection and EV
        System.out.println("[LOG] Launch FILE IN redirection and EV");
        int ev = BINARY.taskSTDINFileRedirectionWithEV(message, fileIn);

        System.out.println("[LOG] FILE IN - EV = " + ev);
    }

    private static void testOutputRedirection() {
        // Task with FILE OUT redirection
        System.out.println("[LOG] Launch FILE OUT redirection");
        BINARY.taskSTDOUTFileRedirection(message, fileOut);

        System.out.println("[LOG] Content FILE OUT redirection:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileOut))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileOut);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection and EV
        System.out.println("[LOG] Launch FILE OUT redirection and EV");
        int ev = BINARY.taskSTDOUTFileRedirectionWithEV(message, fileOut);

        System.out.println("[LOG] FILE OUT redirection - EV = " + ev);
        System.out.println("[LOG] Content FILE OUT redirection - EV:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileOut))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileOut);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection append mode
        System.out.println("[LOG] Launch FILE OUT redirection append");
        BINARY.taskSTDOUTFileRedirectionAppend(message, fileOut);

        System.out.println("[LOG] Content FILE OUT redirection append:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileOut))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileOut);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection append mode and EV
        System.out.println("[LOG] Launch FILE OUT redirection append and EV");
        int ev2 = BINARY.taskSTDOUTFileRedirectionWithEVAppend(message, fileOut);

        System.out.println("[LOG] FILE OUT redirection append - EV = " + ev2);
        System.out.println("[LOG] Content FILE OUT redirection append - EV:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileOut))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileOut);
            System.exit(1);
        }
    }

    private static void testErrorRedirection() {
        // Task with FILE ERR redirection
        System.out.println("[LOG] Launch FILE ERR redirection");
        BINARY.taskSTDERRFileRedirection(message, fileErr);

        System.out.println("[LOG] Content FILE ERR redirection:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileErr))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileErr);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection and EV
        System.out.println("[LOG] Launch FILE ERR redirection and EV");
        int ev = BINARY.taskSTDERRFileRedirectionWithEV(message, fileErr);

        System.out.println("[LOG] FILE ERR redirection - EV = " + ev);
        System.out.println("[LOG] Content FILE ERR redirection - EV:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileErr))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileErr);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection append mode
        System.out.println("[LOG] Launch FILE ERR redirection append");
        BINARY.taskSTDERRFileRedirectionAppend(message, fileErr);

        System.out.println("[LOG] Content FILE ERR redirection append:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileErr))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileErr);
            System.exit(1);
        }

        // ------------------------------------------------------------------------
        // Task with FILE OUT redirection append mode and EV
        System.out.println("[LOG] Launch FILE ERR redirection append and EV");
        int ev2 = BINARY.taskSTDERRFileRedirectionWithEVAppend(message, fileErr);

        System.out.println("[LOG] FILE ERR redirection append - EV = " + ev2);
        System.out.println("[LOG] Content FILE ERR redirection append - EV:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileErr))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileErr);
            System.exit(1);
        }
    }

    private static void testComplexCalls() {
        // Full task with no EV
        System.out.println("[LOG] Launch FULL redirection");
        BINARY.fullTask1(message, fileIn, fileOut, fileErr);

        System.out.println("[LOG] Content FULL OUT:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileOut))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileOut);
            System.exit(1);
        }
        System.out.println("[LOG] Content FULL ERR:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileErr))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileErr);
            System.exit(1);
        }
        
        // ------------------------------------------------------------------------
        // Full task with no EV
        System.out.println("[LOG] Launch FULL2 redirection");
        int ev = BINARY.fullTask2(message, fileIn, fileOut, fileErr);

        System.out.println("[LOG] FULL2 - EV = " + ev);
        System.out.println("[LOG] Content FULL2 OUT:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileOut))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileOut);
            System.exit(1);
        }
        System.out.println("[LOG] Content FULL2 ERR:");
        try (BufferedReader br = new BufferedReader(new FileReader(fileErr))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error reading file " + fileErr);
            System.exit(1);
        }
    }

}
