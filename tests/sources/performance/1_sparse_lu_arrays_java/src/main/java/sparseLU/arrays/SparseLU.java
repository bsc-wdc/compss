package sparseLU.arrays;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


public class SparseLU {

    public static final int N = 16; // Matrix size
    public static final int M = 2; // Block size

    private static double[][][] _A;
    private static double[][][] _A_OUT_EXPECTED;


    public static void main(String args[]) {
        // Load file locations
        if (args.length != 2) {
            System.out.println("Usage: sparselu <A.in> <AExpected.out>");
            System.exit(-1);
        }
        String path_A_in = args[0];
        String path_A_out = args[1];

        // Report parameters
        System.out.println("[LOG] Running with the following parameters:");
        System.out.println("[LOG]  - N: " + N);
        System.out.println("[LOG]  - M: " + M);

        // Initializing A values
        System.out.println("[LOG] Initializing Matrix from file");
        _A = new double[N][N][M * M];
        loadMat(_A, path_A_in);

        // Computing Sparse LU algorithm
        System.out.println("[LOG] Computing SparseLU algorithm on A");
        for (int kk = 0; kk < N; kk++) {
            SparseLUImpl.lu0(_A[kk][kk]);
            for (int jj = kk + 1; jj < N; jj++)
                if (_A[kk][jj] != null)
                    SparseLUImpl.fwd(_A[kk][kk], _A[kk][jj]);
            for (int ii = kk + 1; ii < N; ii++) {
                if (_A[ii][kk] != null) {
                    SparseLUImpl.bdiv(_A[kk][kk], _A[ii][kk]);
                    for (int jj = kk + 1; jj < N; jj++) {
                        if (_A[kk][jj] != null) {
                            if (_A[ii][jj] == null)
                                _A[ii][jj] = SparseLUImpl.bmodAlloc(_A[ii][kk], _A[kk][jj]);
                            else
                                SparseLUImpl.bmod(_A[ii][kk], _A[kk][jj], _A[ii][jj]);
                        }
                    }
                }
            }
        }

        // Loading expected results
        System.out.println("[LOG] Loading output matrix file expected");
        _A_OUT_EXPECTED = new double[N][N][M * M];
        loadMat(_A_OUT_EXPECTED, path_A_out);

        // Verifying obtained result
        System.out.println("[LOG] Checking that result is correct");
        for (int i = 0; i < _A_OUT_EXPECTED.length; ++i) {
            for (int j = 0; j < _A_OUT_EXPECTED[i].length; ++j) {
                if (_A_OUT_EXPECTED[i][j] == null) {
                    if (_A[i][j] != null) {
                        System.out.println("[ERROR] Matrix values do not match. GOT a value. EXPECTED: null");
                        System.exit(-1);
                    }
                } else {
                    for (int k = 0; k < _A_OUT_EXPECTED[i][j].length; ++k) {
                        if (_A[i][j][k] != _A_OUT_EXPECTED[i][j][k]) {
                            System.out.println(
                                    "[ERROR] Matrix values do not match. GOT: " + _A[i][j][k] + " EXPECTED: " + _A_OUT_EXPECTED[i][j][k]);
                            System.exit(-1);
                        }
                    }
                }
            }
        }
        System.out.println("[SUCCESS] All values match.");
    }

    private static void loadMat(double[][][] mat, String fileName) {
        try {
            FileReader filereader = new FileReader(fileName);
            BufferedReader br = new BufferedReader(filereader);
            StringTokenizer tokens;
            String nextLine;

            for (int row = 0; row < N; row++) {
                for (int col = 0; col < N; col++) {
                    nextLine = br.readLine();
                    tokens = new StringTokenizer(nextLine);
                    boolean empty = false;
                    for (int block = 0; block < M * M && !empty && tokens.hasMoreTokens(); block++) {
                        String value = tokens.nextToken();
                        if (value.equals("null")) {
                            mat[row][col] = null;
                            empty = true;
                        } else {
                            mat[row][col][block] = Double.parseDouble(value);
                        }
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

    @SuppressWarnings("unused")
    private static void printMatrix(double[][][] matrix, String name) {
        System.out.println("MATRIX " + name + ":");
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                SparseLUImpl.printBlock(matrix[i][j], M);
            }
            System.out.println("");
        }
    }

}
