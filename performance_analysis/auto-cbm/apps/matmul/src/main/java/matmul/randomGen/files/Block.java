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
import java.util.StringTokenizer;


public class Block {	
	private int BLOCK_SIZE;
	private double [][] data;

	public Block(String filename, int BSIZE) {
		BLOCK_SIZE = BSIZE;
		data = new double[BLOCK_SIZE][BLOCK_SIZE];
		
		FileReader filereader = null;
		BufferedReader br = null;
		try	{
			filereader = new FileReader(filename);
			br = new BufferedReader(filereader);			// Get a buffered reader. More Efficient
			StringTokenizer tokens;
			String nextLine;
			for (int i = 0; i < BLOCK_SIZE; i++) {
				nextLine = br.readLine();
				System.err.println("NEXT : " + nextLine);
				tokens = new StringTokenizer( nextLine );
				for (int j = 0; j < BLOCK_SIZE && tokens.hasMoreTokens(); j++) {
					data[i][j] = Double.parseDouble( tokens.nextToken() );
				}
			}
		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			try {
				if (br != null) {
					br.close();
				}
				if (filereader != null) {
					filereader.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
	
	public void multiplyAccum (Block a, Block b) {
		for (int i = 0; i < BLOCK_SIZE; i++) {					// rows
			for (int j = 0; j < BLOCK_SIZE; j++) {				// cols
				for (int k = 0; k < BLOCK_SIZE; k++) {			// cols
					this.data[i][j] += a.data[i][k]*b.data[k][j];
				}
			}
		}
	}

	protected void printBlock() {
		for (int i = 0; i < BLOCK_SIZE; i++) {
			for(int j = 0; j < BLOCK_SIZE; j++) {
				System.out.print(data[i][j] + " " );
			}
			System.out.println();
		}
	}

	public void blockToDisk(String filename) {
		FileOutputStream fos = null;
		try	{
			fos = new FileOutputStream (filename);
			
			for (int i = 0; i < BLOCK_SIZE; i++) {
				for (int j = 0; j < BLOCK_SIZE; j++) {
					String str = (new Double (data[i][j])).toString() + " ";
					fos.write(str.getBytes());
				}
				fos.write("\n".getBytes());
			}
			fos.close();
		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}
	}

}
