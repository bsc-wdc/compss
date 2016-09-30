package matmul.input.files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


public class Matmul {
	private static int MSIZE;
	private static int BSIZE;
	private static String DATA_FOLDER;

	private String [][]_A;
	private String [][]_B;
	private String [][]_C;
	
	public static void main(String args[]) {
		//Get parameters
		if (args.length != 3) {
			System.out.println("[ERROR] Usage: matmul <MSIZE> <BSIZE> <DATA_FOLDER>");
			System.exit(-1);
		}
		MSIZE = Integer.valueOf(args[0]);
		BSIZE = Integer.valueOf(args[1]);
		DATA_FOLDER = args[2];
		
		System.out.println("[LOG] MSIZE parameter value = " + MSIZE);
		System.out.println("[LOG] BSIZE parameter value = " + BSIZE);
		System.out.println("[LOG] DATA_FOLDER parameter value = " + DATA_FOLDER);
		
		//Run matmul app
		Matmul matmul = new Matmul();
		matmul.Run();
		
		//Store result
		//System.out.println("[LOG] Storing C matrix obtained");
		//matmul.storeMatrix(fC);
		
		System.out.println("[LOG] Main program finished.");
	}
	
	private void Run() {
		//Initialize file Names
		System.out.println("[LOG] Initialising filenames for each matrix");
		initializeVariables();
		
		//Create result files
		System.out.println("[LOG] Creating files for result computation");
		fillMatrix();
		
		//Compute result
		System.out.println("[LOG] Computing result");
		for (int i = 0; i < MSIZE; i++) {
			for (int j = 0; j < MSIZE; j++) {
				for (int k = 0; k < MSIZE; k++) {
					MatmulImpl.multiplyAccumulative( _C[i][j], _A[i][k], _B[k][j] );
				}
            }
		}
	}

	private void initializeVariables () {
		_A = new String[MSIZE][MSIZE];
		_B = new String[MSIZE][MSIZE];
		_C = new String[MSIZE][MSIZE];
		for ( int i = 0; i < MSIZE; i ++ ) {
			for ( int j = 0; j < MSIZE; j ++ ) {
				_A[i][j] = DATA_FOLDER + File.separator + "A." + i + "." + j;
				_B[i][j] = DATA_FOLDER + File.separator + "B." + i + "." + j;
				_C[i][j] = "C." + i + "." + j;
			}
		}
	}
	
	private void fillMatrix () {
		try {
	        for ( int i = 0; i < MSIZE; i++ ) {
	            for ( int j = 0; j < MSIZE; j++ ) {
	                FileOutputStream fos = new FileOutputStream(_C[i][j]);
	                for (int ii = 0; ii < BSIZE; ii++ ) {
	                    for (int jj = 0; jj < BSIZE; jj ++) {
	                        fos.write("0.0 ".getBytes());
	                    }
	                    fos.write("\n".getBytes());
	                }
	                fos.close();
	            }
	        }
		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	@SuppressWarnings("unused")
	private void storeMatrix (String fileName) {
		try {
			FileOutputStream fos = new FileOutputStream(fileName);
			for (int i = 0; i < MSIZE; ++i) {
				for (int j = 0; j < MSIZE; ++j) {
					FileReader filereader = new FileReader(_C[i][j]);
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
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

}
