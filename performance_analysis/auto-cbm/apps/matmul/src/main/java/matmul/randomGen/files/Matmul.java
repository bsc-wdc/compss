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
package matmul.randomGen.files;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;


public class Matmul {
	private static int MSIZE;
	private static int BSIZE;
	private static int RANDOM_SEED;

	private static String[][] AfileNames;
	private static String[][] BfileNames;
	private static String[][] CfileNames;
	
	
	private static void usage() {
		System.out.println("    Usage: matmul.files.Matmul <MSize> <BSize> <seed>");
	}
	
	public static void main(String[] args) throws Exception {
		// Check and get parameters
		if (args.length != 3) {
			usage();
			throw new Exception("[ERROR] Incorrect number of parameters");
		}
		MSIZE = Integer.parseInt(args[0]);
		BSIZE = Integer.parseInt(args[1]);
		RANDOM_SEED = Integer.parseInt(args[1]);
		
		// Initialize matrices
		System.out.println("[LOG] MSIZE parameter value = " + MSIZE);
		System.out.println("[LOG] BSIZE parameter value = " + BSIZE);
		System.out.println("[LOG] RANDOM_SEED parameter value = " + RANDOM_SEED);
		initializeVariables();
		initializeMatrix(AfileNames, true);
		initializeMatrix(BfileNames, true);
		initializeMatrix(CfileNames, false);
		
		// Compute matrix multiplication C = A x B
		computeMultiplication();
		
		// Uncomment the following line if you wish to see the result in the stdout
		//printMatrix(CfileNames, "C (Result)");
		// Uncomment the following line if you wish to store the result in a file
		//storeMatrix("c_result.txt");
		
		// End
		System.out.println("[LOG] Main program finished.");
	}
	
	private static void initializeVariables () {
		AfileNames = new String[MSIZE][MSIZE];
		BfileNames = new String[MSIZE][MSIZE];
		CfileNames = new String[MSIZE][MSIZE];
		for ( int i = 0; i < MSIZE; i ++ ) {
			for ( int j = 0; j < MSIZE; j ++ ) {
				AfileNames[i][j] = "A." + i + "." + j;
				BfileNames[i][j] = "B." + i + "." + j;
				CfileNames[i][j] = "C." + i + "." + j;
			}
		}
	}
	
	private static void initializeMatrix(String[][] fileNames, boolean initRand) throws Exception {
		Random generator = new Random(RANDOM_SEED);
		
		for (int i = 0; i < MSIZE; ++i) {
			for (int j = 0; j < MSIZE; ++j) {
				FileOutputStream fos = null;
				try {
					fos = new FileOutputStream(fileNames[i][j]);
					for (int iblock = 0; iblock < BSIZE; ++iblock) {
						for (int jblock = 0; jblock < BSIZE; ++jblock) {
							double value = (double)0.0;
							if (initRand) {
								value = (double)(generator.nextDouble()*10.0);
							}
							fos.write(String.valueOf(value).getBytes());
							fos.write(" ".getBytes());
						}
						fos.write("\n".getBytes());
					}
					fos.write("\n".getBytes());
				} catch (IOException e) {
					throw new Exception("[ERROR] Error initializing matrix", e);
				} finally {
					if (fos != null) {
						try {
							fos.close();
						} catch (Exception e) {
							throw new Exception("[ERROR] Error closing matrix file", e);
						}
					}
				}
			}
		}
	}
	
	private static void computeMultiplication() {
		System.out.println("[LOG] Computing result");
		for (int i = 0; i < MSIZE; i++) {
			for (int j = 0; j < MSIZE; j++) {
				for (int k = 0; k < MSIZE; k++) {
					MatmulImpl.multiplyAccumulative(CfileNames[i][j], AfileNames[i][k], BfileNames[k][j], BSIZE);
				}
            }
		}
	}
		
	@SuppressWarnings("unused")
	private static void storeMatrix (String fileName) throws Exception {
		try {
			FileOutputStream fos = new FileOutputStream(fileName);
			for (int i = 0; i < MSIZE; ++i) {
				for (int j = 0; j < MSIZE; ++j) {
					FileReader filereader = new FileReader(CfileNames[i][j]);
					BufferedReader br = new BufferedReader(filereader);
					StringTokenizer tokens;
					String nextLine;
					for (int iblock = 0; iblock < BSIZE; ++iblock) {
						nextLine = br.readLine();
						tokens = new StringTokenizer(nextLine);
						for (int jblock = 0; jblock < BSIZE && tokens.hasMoreTokens(); ++jblock) {
							String value = tokens.nextToken() + " ";
							fos.write(value.getBytes());
						}
					}
					fos.write("\n".getBytes());
					br.close();
					filereader.close();
				}
				fos.write("\n".getBytes());
			}
			fos.close();
		} catch (FileNotFoundException fnfe) {
			throw new Exception("[ERROR] Error storing result matrix", fnfe);
		} catch (IOException ioe) {
			throw new Exception("[ERROR] Error storing result matrix", ioe);
		}
	}

	@SuppressWarnings("unused")
	private static void printMatrix(String[][] fileNames, String name) throws Exception {
		System.out.println("MATRIX " + name);
		for (int i = 0; i < MSIZE; i++) {
			 for (int j = 0; j < MSIZE; j++) {
				Block aux = new Block(fileNames[i][j], BSIZE);
				aux.printBlock();
			 }
			 System.out.println("");
		 }
	}
	
}

