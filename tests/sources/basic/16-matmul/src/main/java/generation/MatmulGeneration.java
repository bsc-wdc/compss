package generation;

import java.io.FileOutputStream;
import java.io.IOException;


public class MatmulGeneration {

    private static int MSIZE;
    private static final int BSIZE = 2;

    private static double[][][] A;
    private static double[][][] B;
    private static double[][][] C;


    public static void main(String args[]) {
        // Get parameters
        if (args.length != 4) {
            System.out.println("[ERROR] Usage: matmul <MSIZE> <Ain> <Bin> <Cout>");
            System.exit(-1);
        }
        MSIZE = Integer.parseInt(args[0]);
        String fA = args[1];
        String fB = args[2];
        String fC = args[3];
        System.out.println("[LOG] MSIZE parameter value = " + MSIZE);
        System.out.println("[LOG] BSIZE parameter value = " + BSIZE);

        // Generate input matrices
        A = new double[MSIZE][MSIZE][BSIZE * BSIZE];
        B = new double[MSIZE][MSIZE][BSIZE * BSIZE];
        C = new double[MSIZE][MSIZE][BSIZE * BSIZE];
        System.out.println("[LOG] Generating Matrix A");
        generate("A");
        System.out.println("[LOG] Generating Matrix B");
        generate("B");
        System.out.println("[LOG] Calculating Matrix C");
        calculate();

        // Storing results
        System.out.println("[LOG] Storing A matrix");
        storeMatrix("A", fA);
        System.out.println("[LOG] Storing B matrix");
        storeMatrix("B", fB);
        System.out.println("[LOG] Storing C matrix");
        storeMatrix("C", fC);
        System.out.println("[LOG] Main program finished");
    }

    private static void generate(String tag) {
        if (!tag.equals("A") && !tag.equals("B")) {
            System.out.println("[ERROR] Bad generation");
            System.exit(-1);
        }

        for (int i = 0; i < MSIZE; ++i) {
            for (int j = 0; j < MSIZE; ++j) {
                for (int block = 0; block < BSIZE * BSIZE; ++block) {
                    if (tag.equals("A"))
                        A[i][j][block] = (Math.random()) * 10.0;
                    else
                        B[i][j][block] = (Math.random()) * 10.0;
                }
            }
        }
    }

    private static void calculate() {
        for (int i = 0; i < MSIZE; i++) {
            for (int j = 0; j < MSIZE; j++) {
                for (int k = 0; k < MSIZE; k++) {
                    for (int iblock = 0; iblock < BSIZE; iblock++) {
                        for (int jblock = 0; jblock < BSIZE; jblock++) {
                            for (int kblock = 0; kblock < BSIZE; kblock++) {
                                C[i][j][iblock * BSIZE + jblock] += A[i][k][iblock * BSIZE + kblock] * B[k][j][kblock * BSIZE + jblock];
                            }
                        }
                    }
                }
            }
        }
    }

    private static void storeMatrix(String tag, String fileName) {
        if (!tag.equals("A") && !tag.equals("B") && !tag.equals("C")) {
            System.out.println("[ERROR] Bad store code");
            System.exit(-1);
        }

        try {
            FileOutputStream fos = new FileOutputStream(fileName);
            for (int i = 0; i < MSIZE; ++i) {
                for (int j = 0; j < MSIZE; ++j) {
                    for (int block = 0; block < BSIZE * BSIZE; ++block) {
                        String value;
                        if (tag.equals("A"))
                            value = String.valueOf(A[i][j][block]) + " ";
                        else if (tag.equals("B"))
                            value = String.valueOf(B[i][j][block]) + " ";
                        else
                            value = String.valueOf(C[i][j][block]) + " ";
                        fos.write(value.getBytes());
                    }
                    fos.write("\n".getBytes());
                }
                fos.write("\n".getBytes());
            }
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
