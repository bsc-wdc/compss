package generation;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;


public class MatmulGeneration {
	private static int MSIZE;
	private static int BSIZE;
	private static String DEST_FOLDER;
	private static int RANDOM_SEED;
	
	private static double[][][] A;
	private static double[][][] B;

	
	public static void main(String args[]) {
		//Get parameters
		if (args.length != 4) {
			System.out.println("[ERROR] Usage: matmul.generation.MatmulGeneration <MSIZE> <BSIZE> <DestFolder> <seed>");
			System.exit(-1);
		}
		MSIZE = Integer.parseInt(args[0]);
		BSIZE = Integer.parseInt(args[1]);
		DEST_FOLDER = args[2];
		RANDOM_SEED = Integer.parseInt(args[3]);

		System.out.println("[LOG] MSIZE parameter value = " + MSIZE);
		System.out.println("[LOG] BSIZE parameter value = " + BSIZE);
		System.out.println("[LOG] DEST_FOLDER parameter value = " + DEST_FOLDER);
		System.out.println("[LOG] SEED parameter value = " + RANDOM_SEED);

		// Generate A input matrix
		A = new double[MSIZE][MSIZE][BSIZE*BSIZE];
		System.out.println("[LOG] Generating Matrix A");
		generateA();
		System.out.println("[LOG] Storing A matrix");
		storeMatrix("A");
		
		// Generate B input matrix		
		B = new double[MSIZE][MSIZE][BSIZE*BSIZE];
		System.out.println("[LOG] Generating Matrix B");
		generateB();
		System.out.println("[LOG] Storing B matrix");
		storeMatrix("B");
		
		System.out.println("[LOG] Main program finished");
	}
	
	private static void generateA() {
		Random generator = new Random(RANDOM_SEED);
		
		for (int i = 0; i < MSIZE; ++i) {
			for (int j = 0; j < MSIZE; ++j) {
				for (int block = 0; block < BSIZE*BSIZE; ++block) {
					A[i][j][block] = generator.nextDouble()*10.0;
				}
			}
		}
	}
	
	private static void generateB() {
		Random generator = new Random(RANDOM_SEED);
		
		for (int i = 0; i < MSIZE; ++i) {
			for (int j = 0; j < MSIZE; ++j) {
				for (int block = 0; block < BSIZE*BSIZE; ++block) {
					B[i][j][block] = generator.nextDouble()*10.0;
				}
			}
		}
	}
	
	private static void storeMatrix(String tag) {
		if (!tag.equals("A") && !tag.equals("B")) {
			System.err.println("[ERROR] Bad store code. Only A or B allowed.");
			System.exit(-1);
		}
		
		for (int i = 0; i < MSIZE; ++i) {
			for (int j = 0; j < MSIZE; ++j) {
				String fileName = DEST_FOLDER + File.separator + tag 
						+ "." + String.valueOf(i) + "." + String.valueOf(j);
				FileOutputStream fos = null;
				try {
					fos = new FileOutputStream(fileName);
					for (int iblock = 0; iblock < BSIZE; iblock++) {
						for (int jblock = 0; jblock < BSIZE; jblock++) {
							String value = "";
							if (tag.equals("A")) {
								value = String.valueOf(A[i][j][iblock*BSIZE+jblock]);
							} else {
								value = String.valueOf(B[i][j][iblock*BSIZE+jblock]);
							}
							fos.write(value.getBytes());
							fos.write(" ".getBytes());
						}
						fos.write("\n".getBytes());
					}
				} catch (Exception e) {
					System.err.println("[ERROR] Cannot store matrix in file");
					e.printStackTrace();
					System.exit(-1);
				} finally {
					if (fos != null) {
						try {
							fos.close();
						} catch (Exception e2) {
							System.err.println("[ERROR] Cannot close file " + fileName);
							e2.printStackTrace();
							System.exit(-1);
						}
					}
				}
			}
		}
	}
	
}
