package testDecaf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import decaf.DECAF;


public class Main {

    private static final int SLEEP_TIME = 5_000;
    /*private static final String[] lines = {"Executing decaf data-flow generator: "+System.getenv("PWD")+"/decaf/test.py mpirun -H ",
    	"Executing python script.", "Executing decaf data-flow: ./test.sh", "Executing binary"};
    */

    private static final String[] lines = {"Executing decaf data-flow generator: "+System.getenv("PWD")+"/decaf/test.py ",
        "Executing python script.", "Executing decaf data-flow: ./test.sh", "Executing binary"};
    private static final String endLine0 = ".decafHostfile --args=";


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
        testDecafSingleNode("argument1", 2);

        System.out.println("[LOG] Test Decaf with 2 node");
        testDecafMultipleNodes("argument1", 2);

	System.out.println("[LOG] Test Concurrent Decaf with 2 node");
        testDecafConcurrentMultipleNodes("argument1", 2);

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Decaf Test finished");
    }

    private static void usage() {
        System.err.println("ERROR: Invalid arguments");
        System.err.println("Usage: main");

        System.exit(1);
    }

    private static void testDecafSingleNode(String arg1, int arg2) {
        String outputFile = "decafSingleOutput.txt";
        int ev = DECAF.taskSingleDecaf(arg1, arg2, outputFile);

        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }
        try (BufferedReader br = new BufferedReader(new FileReader(outputFile))) {
            String line;
            int lineNum=0;
            while ((line = br.readLine()) != null) { 
                if (lineNum<lines.length){
                	//System.out.println("[RESULT] Decaf Task1: " + line);
                	checkLine(lineNum, 1 , 2, arg1 + " " + arg2 , line);
                	lineNum++;
                }else{
                	System.err.println("[ERROR] Process returned a file with more than "+ lines.length +" lines ");
                    System.exit(1);
                }
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot read output file " + outputFile);
            System.exit(1);
        }
        System.out.println("[LOG] Result must be checked on result script");
    }

    private static void checkLine(int lineNum, int nodes, int tasks, String arguments, String line) {
    	String hostname = "localhost";
    	try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            System.err.println("Cannot obtain hostname. Loading default value " + hostname);
        }
    	
    	if (lineNum==0){
    		String startLine=lines[0] + "-n "+ tasks + " --hostfile ";
    		String endLine= endLine0 + "\""+arguments+"\"";
    		if (!line.startsWith(startLine)){
    			System.err.println("[ERROR] line 0 is not starting correctly : \""+line+
    					"\" options is: \""+startLine+"\"");
    			System.exit(1);
    		}
    		if (!line.endsWith(endLine)){
    			System.err.println("[ERROR] line 0 is not ending correctly : \""+line+
    					"\" options is: \""+endLine+"\"");
    			System.exit(1);
    		}

		}else{
			if (!line.equals(lines[lineNum])){
				System.err.println("[ERROR] line " + lineNum + " is not correct: "+line);
				System.exit(1);
			}
		}
	}

	private static void testDecafMultipleNodes(String arg1, int arg2) {
        String outputFile = "decafMultipleOutput.txt";
        Integer ev = DECAF.taskMultipleDecaf(arg1, arg2, outputFile);

        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }
        try (BufferedReader br = new BufferedReader(new FileReader(outputFile))) {
            String line;
            int lineNum = 0;
            while ((line = br.readLine()) != null) {
                if (lineNum<lines.length){
                	//System.out.println("[RESULT] Decaf Task1: " + line);
                	checkLine(lineNum, 2 , 4, arg1 + " " + arg2, line);
                	lineNum++;
                }else{
                	System.err.println("[ERROR] Process returned a file with more than "+ lines.length +" lines ");
                    System.exit(1);
                }
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot read output file " + outputFile);
            System.exit(1);
        }
        System.out.println("[LOG] Result must be checked on result script");
    }

    private static void testDecafConcurrentMultipleNodes(String arg1, int arg2) {
        String outputFile1 = "decafMultipleOutput1.txt";
        String outputFile2 = "decafMultipleOutput2.txt";
        Integer ev1 = DECAF.taskConcurrentMultipleDecaf(arg1, arg2, outputFile1);
        Integer ev2 = DECAF.taskConcurrentMultipleDecaf(arg1, arg2, outputFile2);

        if (ev1 != 0 || ev2 != 0) {
            System.err.println("[ERROR] One process returned non-zero exit value: " + ev1 + " or " + ev2);
            System.exit(1);
        }
        try (BufferedReader br = new BufferedReader(new FileReader(outputFile1))) {
            String line;
            int lineNum = 0;
            while ((line = br.readLine()) != null) {
                if (lineNum<lines.length){
                	//System.out.println("[RESULT] Decaf Task1: " + line);
                	checkLine(lineNum, 2 , 2, arg1 + " " + arg2, line);
                	lineNum++;
                }else{
                	System.err.println("[ERROR] Process returned a file with more than "+ lines.length +" lines ");
                    System.exit(1);
                }
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot read output file " + outputFile1);
            System.exit(1);
        }
        try (BufferedReader br = new BufferedReader(new FileReader(outputFile2))) {
            String line;
            int lineNum=0;
            while ((line = br.readLine()) != null) {
                if (lineNum<lines.length){
                	//System.out.println("[RESULT] Decaf Task1: " + line);
                	checkLine(lineNum, 2 , 2, arg1 + " " + arg2, line);
                	lineNum++;
                }else{
                	System.err.println("[ERROR] Process returned a file with more than "+ lines.length +" lines ");
                    System.exit(1);
                }
            }
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot read output file " + outputFile2);
            System.exit(1);
        }

        System.out.println("[LOG] Result must be checked on result script");
    }

}
