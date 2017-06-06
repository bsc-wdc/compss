package testDecaf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import decaf.DECAF;


public class Main {

    private static final int SLEEP_TIME = 5_000;
    private static final String[] lines = {"Executing decaf data-flow generator: "+System.getenv("PWD")+"/decaf/test.py mpirun -H ",
    	"Executing python script.", "Executing decaf data-flow: ./test.sh", "Executing binary"};


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
        int ev = DECAF.taskSingleDecaf(outputFile);

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
                	checkLine(lineNum, 1 , 2, line);
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

    private static void checkLine(int lineNum, int nodes, int tasks, String line) {
    	String hostname = "localhost";
    	try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            System.err.println("Cannot obtain hostname. Loading default value " + hostname);
        }
    	
    	if (lineNum==0){
			if (nodes == 1 && tasks ==2){
				String alt1=lines[0] + "COMPSsWorker01,COMPSsWorker01 -n 2";
				String alt2=lines[0] + "COMPSsWorker02,COMPSsWorker02 -n 2";
				String alt3=lines[0] + hostname + ","+hostname+" -n 2";
				if (!line.equals(alt1) && !line.equals(alt2) && !line.equals(alt3)){
					System.err.println("[ERROR] line 0 is not correct for 1,2: \""+line+"\" options are: \""+alt1+","+alt2+"\"");
					System.exit(1);
				}
			}else if (nodes == 2 && tasks == 4){
				String alt1=lines[0]+"COMPSsWorker01,COMPSsWorker01,COMPSsWorker02,COMPSsWorker02 -n 4";
				String alt2=lines[0]+"COMPSsWorker02,COMPSsWorker02,COMPSsWorker01,COMPSsWorker01 -n 4";
				String alt3=lines[0]+hostname+","+hostname+",COMPSsWorker02,COMPSsWorker02 -n 4";
				String alt4=lines[0]+hostname+","+hostname+",COMPSsWorker01,COMPSsWorker01 -n 4";
				String alt5=lines[0]+"COMPSsWorker02,COMPSsWorker02,"+hostname+","+hostname+" -n 4";
				String alt6=lines[0]+"COMPSsWorker01,COMPSsWorker01,"+hostname+","+hostname+" -n 4";
				if (!line.equals(alt1) && !line.equals(alt2) && !line.equals(alt3) && !line.equals(alt4) 
						&& !line.equals(alt5) && !line.equals(alt6)){
					System.err.println("[ERROR] line 0 is not correct for 2,4: \""+line+"\" options are: \""+alt1+","+alt2+"\"");
					System.exit(1);
				}
			}else if (nodes == 2 && tasks == 2){
				String alt1=lines[0]+"COMPSsWorker01,COMPSsWorker01 -n 2";
				String alt2=lines[0]+"COMPSsWorker02,COMPSsWorker02 -n 2";
				String alt3=lines[0]+"COMPSsWorker01,COMPSsWorker02 -n 2";
				String alt4=lines[0]+"COMPSsWorker02,COMPSsWorker01 -n 2";
				String alt5=lines[0]+ hostname +",COMPSsWorker01 -n 2";
				String alt6=lines[0]+ hostname +",COMPSsWorker02 -n 2";
				String alt7=lines[0]+ "COMPSsWorker01,"+ hostname +" -n 2";
				String alt8=lines[0]+ "COMPSsWorker02,"+ hostname +" -n 2";
				if (!line.equals(alt1) && !line.equals(alt2) && !line.equals(alt3) && !line.equals(alt4)
						&& !line.equals(alt5) && !line.equals(alt6)&& !line.equals(alt7) && !line.equals(alt8)){
					System.err.println("[ERROR] line 0 is not correct for 2 , 2 : \""+line+"\" options are: \""+alt1+"\n"+alt2+"\n"+alt3+"\n"+alt4+
							"\n"+alt5+"\n"+alt6+"\n"+alt7+"\n"+alt8+"\"");
					System.exit(1);
				}
			}else{
				System.err.println("[ERROR] incorrect number of nodes or tasks (nodes: "+nodes+", tasks: "+tasks+")");
				System.exit(1);
			}
		}else{
			if (!line.equals(lines[lineNum])){
				System.err.println("[ERROR] line " + lineNum + " is not correct: "+line);
				System.exit(1);
			}
		}
	}

	private static void testDecafMultipleNodes() {
        String outputFile = "decafMultipleOutput.txt";
        Integer ev = DECAF.taskMultipleDecaf(outputFile);

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
                	checkLine(lineNum, 2 , 4, line);
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

    private static void testDecafConcurrentMultipleNodes() {
        String outputFile1 = "decafMultipleOutput1.txt";
        String outputFile2 = "decafMultipleOutput2.txt";
        Integer ev1 = DECAF.taskConcurrentMultipleDecaf(outputFile1);
        Integer ev2 = DECAF.taskConcurrentMultipleDecaf(outputFile2);

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
                	checkLine(lineNum, 2 , 2, line);
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
                	checkLine(lineNum, 2 , 2, line);
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
