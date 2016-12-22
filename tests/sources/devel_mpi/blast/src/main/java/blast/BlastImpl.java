/*
 *  Copyright 2002-2015 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package blast;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class BlastImpl {

	// Debug
	private static final boolean debug = true;

	public static void align(String databasePath, String partitionFile,
			String partitionOutput, String blastBinary, String commandArgs)
			throws IOException, InterruptedException, Exception {

		if (debug) {
			System.out.println("\nRunning Blast with parameters:");
			System.out.println(" - Binary: " + blastBinary);
			System.out.println(" - Database Path: " + databasePath);
			System.out.println(" - Input partition file: " + partitionFile);
			System.out.println(" - Output partition file: " + partitionOutput);
			System.out.println(" - Command line arguments: " + commandArgs);
		}

		FileReader seq = null;
		StringBuffer str = new StringBuffer();
		try {
			seq = new FileReader(partitionFile);
			int c;
			while ((c = seq.read()) != -1) {
				str.append((char) c);
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		System.out.println("\nSeq -> " + str);
		String cmd = null;
		cmd = blastBinary + " " + "-p blastx -d " + databasePath + " -i " + partitionFile + " -o " + partitionOutput + " " + commandArgs;

		if (debug) {
			System.out.println("\nBlast Cmd -> " + cmd);
			System.out.println(" ");
		}

		Long startAlignment = System.currentTimeMillis();
		Process blastProc = Runtime.getRuntime().exec(cmd);

		byte[] b = new byte[1024];
		int read;

		// Check the proper ending of the process
		int exitValue = blastProc.waitFor();

		Long alignmentTime = (System.currentTimeMillis() - startAlignment) / 1000;
		System.out.println("Sequence length: " + str.length() + " | Alignment time " + alignmentTime + " seconds \n");

		if (exitValue != 0) {
			BufferedInputStream bisErr = new BufferedInputStream(blastProc.getErrorStream());
			BufferedOutputStream bosErr = new BufferedOutputStream(new FileOutputStream(partitionFile + ".err"));

			while ((read = bisErr.read(b)) >= 0) {
				bosErr.write(b, 0, read);
			}
			bisErr.close();
			bosErr.close();
			System.err.println("Error executing Blast job, exit value is: " + exitValue);
			throw new Exception("Error executing Blast job, exit value is: " + exitValue);
		}
	}

	public static void assemblyPartitions(String partialFileA, String partialFileB) {
		String line = null;
		boolean append = true;

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(partialFileA, append));
			BufferedReader bfB = new BufferedReader(new FileReader(partialFileB));

			if (debug) {
				System.out.println("\nAssembling partial outputs -> " + partialFileA + " to " + partialFileB);
			}

			while ((line = bfB.readLine()) != null) {
				bw.write(line);
				bw.newLine();
			}

			// Closing final output file
			bfB.close();
			bw.close();

			// Cleaning intermediate results file
			// File fB = new File(partialFileB);
			// fB.delete();
		} catch (Exception e) {
			System.err.println("Error assembling partial results to final result");
		}
	}
	
}
