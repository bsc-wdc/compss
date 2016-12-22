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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import blast.BlastImpl;


public class Blast {

	private static boolean debug;
	private static List<String> partialOutputs = null;
	private static List<String> partialInputs = null;

	public static void main(String args[]) throws Exception {
		/*
		 * Parameters: - 0: Debug - 1: Blast binary location - 2: Database Name
		 * - 3: Input sequences path - 4: Fragments number - 5: Temporary
		 * directory - 6: Output file -7: Command line Arguments
		 */

		debug = Boolean.parseBoolean(args[0]);
		String blastBinary = args[1];
		String databaseName = args[2];
		String inputFileName = args[3];
		String numFragments = args[4];
		String temporaryDir = args[5];
		String outputFileName = args[6];

		String commandArgs = " ";
		for (int i = 7; i < args.length; i++) {
			commandArgs += args[i] + " ";
		}

		print_header();

		// Parsing database name
		// Splitting the files model path string using a forward slash as
		// delimiter
		StringTokenizer st = new StringTokenizer(databaseName, "/");
		String dbName = null;

		while (st.hasMoreElements()) {
			dbName = st.nextToken();
		}

		if (debug) {
			System.out.println("Parameters: ");
			System.out.println("- Debug Enabled");
			System.out.println("- Blast binary: " + blastBinary);
			System.out.println("- Number of expected fragments: "
					+ numFragments);
			System.out.println("- Database Name with Path: " + databaseName);
			System.out.println("- Database Name: " + dbName);
			System.out.println("- Input Sequences File: " + inputFileName);
			System.out.println("- Temporary Directory: " + temporaryDir);
			System.out.println("- Output File: " + outputFileName);
			System.out.println("- Command Line Arguments: " + commandArgs);
			System.out.println(" ");
		}

		Long startTotalTime = System.currentTimeMillis();

		try {
			String lastMerge = "";
			// Splitting sequences in desired number of fragments
			try {
				splitSequenceFile(inputFileName, temporaryDir, Integer.parseInt(numFragments));
			} catch (Exception e) {
				System.out.println("Error splitting input sequences.");
				e.printStackTrace();
			}

			// Submitting the tasks
			System.out.println("\nAligning Sequences:");
			for (int i = 0; i < partialInputs.size(); i++) {
				BlastImpl.align(databaseName, partialInputs.get(i), partialOutputs.get(i), blastBinary, commandArgs);
			}

			if (debug) {
				System.out.println("\n - Number of fragments to assemble -> " + partialOutputs.size());
			}

			// Final assembly process
			try {
				// Final Assembly process -> Merge 2 by 2
				int neighbor = 1;
				while (neighbor < partialOutputs.size()) {
					for (int result = 0; result < partialOutputs.size(); result += 2 * neighbor) {
						if (result + neighbor < partialOutputs.size()) {
							BlastImpl.assemblyPartitions( partialOutputs.get(result), partialOutputs.get(result + neighbor));
							if (debug) {
								System.out.println(" - Merging files -> " + partialOutputs.get(result) + " and " + partialOutputs.get(result + neighbor));
							}
							lastMerge = partialOutputs.get(result);
						}
					}
					neighbor *= 2;
				}
			} catch (Exception e) {
				System.out.println("Error assembling partial results to final result file.");
				e.printStackTrace();
			}

			FileInputStream fis = new FileInputStream(lastMerge);
			if (debug) {
				System.out.println("\nMoving last merged file: " + lastMerge + " to " + outputFileName + " \n");
			}
			copyFile(fis, new File(outputFileName));
			fis.close();

			// Cleaning up partial results
			CleanUp();

			Long stopTotalTime = System.currentTimeMillis();
			Long totalTime = (stopTotalTime - startTotalTime) / 1000;
			System.out.println("- " + inputFileName + " sequences aligned successfully in " + totalTime + " seconds \n");
		} catch (Exception e) {
			System.out.println("Error:");
			e.printStackTrace();
		}
	}

	private static void print_header() {
		System.out.println("\nBLAST Sequence Alignment Tool:\n");
	}

	private static void splitSequenceFile(String inputFileName, String temporaryDir, Integer numFragments) throws Exception {
		int nsequences = 0;
		int seqsPerFragment = 0;
		String line = "";
		BufferedReader bf = new BufferedReader(new FileReader(inputFileName));

		Long startSplit = System.currentTimeMillis();

		// Counting number of sequences
		while ((line = bf.readLine()) != null) {
			if (line.contains(">")) {
				nsequences++;
			}
		}
		bf.close();

		System.out.println("- The total number of sequences is: " + nsequences);

		seqsPerFragment = (int) Math.round(((double) nsequences/(double) numFragments));

		partialInputs = new ArrayList<String>(numFragments);
		partialOutputs = new ArrayList<String>(numFragments);

		if (debug) {
			System.out.println("- The total number of sequences of a fragment is: " + seqsPerFragment);
			System.out.println("\n- Splitting sequences among fragment files...");
		}

		int frag = 0;
		BufferedWriter bw = null;
		boolean append = true;
		bf = new BufferedReader(new FileReader(inputFileName));

		while ((line = bf.readLine()) != null) {
			if (line.contains(">")) {
				if (bw != null) {
					bw.close();
				}
				if (frag < numFragments) {
					// Creating fragment
					UUID index = UUID.randomUUID();
					String partitionFile = temporaryDir + "seqFile" + index + ".sqf";
					String partitionOutput = temporaryDir + "resFile" + index + ".result.txt";
					partialInputs.add(partitionFile);
					partialOutputs.add(partitionOutput);
				}
				// Preparing for writing to next fragment
				bw = new BufferedWriter(new FileWriter(partialInputs.get((frag % numFragments)), append));
				frag++;
			}
			bw.write(line);
			bw.newLine();
		}

		// Closing the last intermediate file
		bw.close();
		bf.close();

		Long splitTime = (System.currentTimeMillis() - startSplit)/1000;
		System.out.println("- Sequences splitted in " + splitTime + " seconds \n");
	}

	private static void CleanUp() {
		// Cleaning intermediate sequence input files
		for (int i = 0; i < partialInputs.size(); i++) {
			File fSeq = new File(partialInputs.get(i));
			fSeq.delete();
		}

		for (int i = 0; i < partialOutputs.size(); i++) {
			File fres = new File(partialOutputs.get(i));
			fres.delete();
		}
	}

	private static void copyFile(FileInputStream sourceFile, File destFile) throws IOException {
		try (FileChannel source = sourceFile.getChannel();
		        FileOutputStream outputDest = new FileOutputStream(destFile);
		        FileChannel destination = outputDest.getChannel()) {
		    
		    destination.transferFrom(source, 0, source.size()); 
		} catch (IOException ioe) {
		    throw ioe;
		}
	}

	/*
	 * private static void assemblyPartitions(List<String> partialOutputs,
	 * String outputFileName, String temporaryDir){
	 * 
	 * String line = null; Long startAssemblyTime = System.currentTimeMillis();
	 * 
	 * try { //Cleaning intermediate sequence input files for(int i=0; i <
	 * partialOutputs.size(); i++){ File fSeq = new File(temporaryDir+"seqFile"
	 * + i + ".sqf"); fSeq.delete(); }
	 * 
	 * BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName));
	 * 
	 * for(int i=0; i < partialOutputs.size(); i++){ BufferedReader bf = new
	 * BufferedReader(new FileReader(partialOutputs.get(i))); if(debug){
	 * System.out
	 * .println(" - Assembling partial output -> "+partialOutputs.get(i
	 * )+" to final output file -> "+outputFileName); }
	 * 
	 * while ((line = bf.readLine()) != null) { bw.write(line); bw.newLine(); }
	 * 
	 * //Cleaning intermediate results file bf.close(); File partOut = new
	 * File(partialOutputs.get(i)); partOut.delete();
	 * 
	 * } //Closing final output file bw.close(); } catch (Exception e){
	 * System.out.println("Error assembling partial results to final result"); }
	 * 
	 * Long stopAssemblyTime = System.currentTimeMillis(); Long assemblyTime =
	 * (stopAssemblyTime - startAssemblyTime)/1000;
	 * System.out.println("- Sequences assembled in "
	 * +assemblyTime+" seconds \n"); }
	 */
}
