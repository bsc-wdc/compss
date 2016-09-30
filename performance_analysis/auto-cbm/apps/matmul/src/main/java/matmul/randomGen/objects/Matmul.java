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
package matmul.randomGen.objects;

import java.io.FileOutputStream;
import java.io.IOException;


public class Matmul {
	private static int MSIZE;
	private static int BSIZE;
	private static int RANDOM_SEED;

	private static Block[][] A;
	private static Block[][] B;
	private static Block[][] C;
	
	private static void usage() {
		System.out.println("    Usage: matmul.objects.Matmul <MSize> <BSize> <seed>");
	}
	
	public static void main(String args[]) throws Exception {
		// Check and get parameters
		if (args.length != 3) {
			usage();
			throw new Exception("[ERROR] Incorrect number of parameters");
		}
		MSIZE = Integer.parseInt(args[0]);
		BSIZE = Integer.parseInt(args[1]);
		RANDOM_SEED = Integer.parseInt(args[2]);
		
		// Initialize matrices
		System.out.println("[LOG] MSIZE parameter value = " + MSIZE);
		System.out.println("[LOG] BSIZE parameter value = " + BSIZE);
		System.out.println("[LOG] RANDOM_SEED parameter value = " + RANDOM_SEED);
		initializeVariables();
		A = initializeMatrix();
		B = initializeMatrix();
		
		// Compute matrix multiplication C = A x B
		computeMultiplication();
		
		// Uncomment the following line if you wish to see the result in the stdout
		//printMatrix(C, "C (Result)");
		// Uncomment the following line if you wish to store the result in a file
		//storeMatrix("c_result.txt");
		
		// End
		System.out.println("[LOG] Main program finished.");
	}
	
	private static void initializeVariables() {
		System.out.println("[LOG] Allocating A/B/C matrix space");
		A = new Block[MSIZE][MSIZE];
		B = new Block[MSIZE][MSIZE];
		C = new Block[MSIZE][MSIZE];
	}
	
	private static Block[][] initializeMatrix() {
		Block[][] matrix = new Block[MSIZE][MSIZE];
		for (int i = 0; i < MSIZE; ++i) {
			for (int j = 0; j < MSIZE; ++j) {
				matrix[i][j] = Block.initBlock(BSIZE, RANDOM_SEED);
			}
		}
		
		return matrix;
	}

	
	private static void computeMultiplication () {		
		//Compute result
		System.out.println("[LOG] Computing Result");
		for (int i = 0; i < MSIZE; i++) {
			for (int j = 0; j < MSIZE; j++) {
				C[i][j] = new Block(BSIZE);
				for (int k = 0; k < MSIZE; k++) {
					C[i][j].multiplyAccumulative(A[i][k], B[k][j]);
				}
            }
		}
	}
	
	@SuppressWarnings("unused")
	private static void storeMatrix(String fileName) throws Exception {
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(fileName);
			for (int i = 0; i < MSIZE; ++i) {
				for (int j = 0; j < MSIZE; ++j) {
					double[][] aux = C[i][j].getData();
					for (int iblock = 0; iblock < BSIZE; ++iblock) {
						for (int jblock = 0; jblock < BSIZE; ++jblock) {
							String value = String.valueOf(aux[iblock][jblock]) + " ";
							fos.write(value.getBytes());
						}
					}
					fos.write("\n".getBytes());
				}
				fos.write("\n".getBytes());
			}
    	} catch(IOException ioe) {
			throw new Exception ("[ERROR] Error storing result matrix", ioe);
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					throw new Exception ("[ERROR] Error storing result matrix", e);
				}
			}
		}
	}
	
	@SuppressWarnings("unused")
	private static void printMatrix(Block[][] matrix, String name) {
		System.out.println("MATRIX " + name);
		for (int i = 0; i < MSIZE; i++) {
			 for (int j = 0; j < MSIZE; j++) {
				matrix[i][j].printBlock();
			 }
			 System.out.println("");
		 }
	}
	
}
