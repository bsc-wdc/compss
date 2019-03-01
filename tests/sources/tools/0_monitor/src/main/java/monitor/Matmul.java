package monitor;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


public class Matmul {

    private static final int MSIZE = 8;
    private static final int BSIZE = 2;

    private double[][][] A;
    private double[][][] B;
    private double[][][] C;


    public static void main(String args[]) {
        // Get parameters
        if (args.length != 3) {
            System.out.println("[ERROR] Usage: matmul <Ain> <Bin> <Cout>");
            System.exit(-1);
        }
        String fA = args[0];
        String fB = args[1];
        String fC = args[2];
        System.out.println("[LOG] MSIZE parameter value = " + MSIZE);
        System.out.println("[LOG] BSIZE parameter value = " + BSIZE);

        // Run matmul app
        Matmul matmul = new Matmul();
        matmul.Run(fA, fB);

        // Check result
        System.out.println("[LOG] Storing C matrix obtained");
        matmul.storeMatrix(fC);
        System.out.println("[LOG] Main program finished. Result needs to be checked (result script)");
    }

    private void Run(String fileA, String fileB) {
        // Load Matrices
        System.out.println("[LOG] Allocating A/B/C matrix space");
        A = new double[MSIZE][MSIZE][BSIZE * BSIZE];
        B = new double[MSIZE][MSIZE][BSIZE * BSIZE];
        C = new double[MSIZE][MSIZE][BSIZE * BSIZE];
        System.out.println("[LOG] Loading A Matrix from file");
        loadMatrix(A, fileA);
        System.out.println("[LOG] Loading B Matrix from file");
        loadMatrix(B, fileB);

        // Compute result
        System.out.println("[LOG] Computing Result");
        for (int i = 0; i < MSIZE; i++) {
            for (int j = 0; j < MSIZE; j++) {
                for (int k = 0; k < MSIZE; k++) {
                    MatmulImpl.multiplyAccumulative(A[i][k], B[k][j], C[i][j]);
                }
            }
        }
    }

    private void loadMatrix(double[][][] matrix, String fileName) {
        try {
            FileReader filereader = new FileReader(fileName);
            BufferedReader br = new BufferedReader(filereader);
            StringTokenizer tokens;
            String nextLine;
            for (int i = 0; i < MSIZE; ++i) {
                for (int j = 0; j < MSIZE; ++j) {
                    nextLine = br.readLine();
                    tokens = new StringTokenizer(nextLine);
                    for (int block = 0; block < BSIZE * BSIZE && tokens.hasMoreTokens(); ++block) {
                        String value = tokens.nextToken();
                        matrix[i][j][block] = Double.parseDouble(value);
                    }
                }
                nextLine = br.readLine();
            }
            br.close();
            filereader.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

    private void storeMatrix(String fileName) {
        try {
            FileOutputStream fos = new FileOutputStream(fileName);
            for (int i = 0; i < MSIZE; ++i) {
                for (int j = 0; j < MSIZE; ++j) {
                    for (int block = 0; block < BSIZE * BSIZE; ++block) {
                        String value = String.valueOf(C[i][j][block]) + " ";
                        fos.write(value.getBytes());
                    }
                    fos.write("\n".getBytes());
                }
                fos.write("\n".getBytes());
            }
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

    @SuppressWarnings("unused")
    private void printMatrix(double[][][] matrix, String name) {
        System.out.println("MATRIX " + name);
        for (int i = 0; i < MSIZE; i++) {
            for (int j = 0; j < MSIZE; j++) {
                MatmulImpl.printBlock(matrix[i][j]);
            }
            System.out.println("");
        }
    }

}
