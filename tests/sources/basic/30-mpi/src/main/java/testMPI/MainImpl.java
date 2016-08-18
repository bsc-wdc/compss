package testMPI;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class MainImpl {
	
	private static final String ERROR_CLOSE_READER 	= "ERROR: Cannot close reader CMD output";
	private static final String ERROR_PROC_EXEC 	= "ERROR: Exception executing MPI command";
	
	private static final String COMPSS_MPI_WORKER_NODES = "COMPSS_MPI_WORKER_NODES";
	private static final String COMPSS_MPI_CUS 			= "COMPSS_MPI_CUS";
	
	private static final int NUM_BASE_MPI_ARGS = 10;
	
	
	public static int taskSingleMPI(String binary, int[] data) {
		String value = executeMPICommand(binary, data);
		return (int) Integer.valueOf(value);
	}
	
	public static int taskMultipleMPI(String binary, int[] data) {
		String value = executeMPICommand(binary, data);
		return (int) Integer.valueOf(value);
	}
		

	private static String executeMPICommand(String binary, int[] data) {
		// Command similar to 
		//     mpirun -H worker1,worker2 -n 1 --bind-to core --map-by slot:PE=${computingUnits} exec args
		
		// Get COMPSS ENV VARS
		//TODO
		String workers = "COMPSsWorker01";//System.getenv(COMPSS_MPI_WORKER_NODES);
		String computingUnits = "1";//System.getenv(COMPSS_MPI_CUS);
		
		// Prepare command
		String[] cmd = new String[NUM_BASE_MPI_ARGS + data.length];
		cmd[0] = "mpirun";
		cmd[1] = "-H";
		cmd[2] = workers;
		cmd[3] = "-n";
		cmd[4] = "1";
		cmd[5] = "--bind-to";
		cmd[6] = "core";
		cmd[7] = "--map-by";
		cmd[8] = "slot:PE=" + computingUnits;
		cmd[9] = binary;
		for (int i = 0; i < data.length; ++i) {
			cmd[NUM_BASE_MPI_ARGS + i] = String.valueOf(data[i]);
		}
		
		// Debug command
		System.out.print("MPI CMD: ");
		for (int i = 0; i < cmd.length; ++i) {
			System.out.print(cmd[i] + " ");
		}
		System.out.println("");
		
		// Launch command
		StringBuffer output = new StringBuffer();
		Process p;
		BufferedReader reader = null;
		try {
			p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			
			reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}
		} catch (Exception e) {
			System.err.println(ERROR_PROC_EXEC);
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					System.err.println(ERROR_CLOSE_READER);
					e.printStackTrace();
				}
			}
		}

		// Return command output
		return output.toString();
	}
	
}
