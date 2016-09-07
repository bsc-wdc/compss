package graph.objects;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


public class SparseLU {

    private static final int N = 16; // N = matrix size
    private static final int M = 2; // M = block size
    private static Block[][] A;


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
        A = new Block[N][N];
        loadMat(A, path_A_in);

        // Computing Sparse LU algorithm
        for (int kk = 0; kk < N; kk++) {
            A[kk][kk].lu0();
            for (int jj = kk + 1; jj < N; jj++)
                if (A[kk][jj] != null)
                    A[kk][jj].fwd(A[kk][kk]);
            for (int ii = kk + 1; ii < N; ii++) {
                if (A[ii][kk] != null) {
                    A[ii][kk].bdiv(A[kk][kk]);
                    for (int jj = kk + 1; jj < N; jj++) {
                        if (A[kk][jj] != null) {
                            if (A[ii][jj] == null)
                                A[ii][jj] = Block.bmodAlloc(A[ii][kk], A[kk][jj]);
                            else
                                A[ii][jj].bmod(A[ii][kk], A[kk][jj]);
                        }
                    }
                }
            }
        }

        // Loading expected results
        System.out.println("[LOG] Loading output matrix file expected");
        Block[][] A_OUT_EXPECTED = new Block[N][N];
        loadMat(A_OUT_EXPECTED, path_A_out);

        // Verifying obtained result
        System.out.println("[LOG] Checking that result is correct");
        for (int i = 0; i < A_OUT_EXPECTED.length; ++i) {
            for (int j = 0; j < A_OUT_EXPECTED[i].length; ++j) {
                if (A_OUT_EXPECTED[i][j] == null) {
                    if (A[i][j] != null) {
                        System.out.println("[ERROR] Matrix values do not match. GOT a value. EXPECTED: null");
                        System.exit(-1);
                    }
                } else {
                    for (int i_block = 0; i_block < A_OUT_EXPECTED[i][j].getData().length; ++i_block) {
                        for (int j_block = 0; j_block < A_OUT_EXPECTED[i][j].getData()[i_block].length; ++j_block) {
                            if (A[i][j].getData()[i_block][j_block] != A_OUT_EXPECTED[i][j].getData()[i_block][j_block]) {
                                System.out.println("[ERROR] Matrix values do not match. GOT: " + A[i][j].getData()[i_block][j_block]
                                        + " EXPECTED: " + A_OUT_EXPECTED[i][j].getData()[i_block][j_block]);
                                System.exit(-1);
                            }
                        }
                    }
                }
            }
        }
        System.out.println("[SUCCESS] All values match.");
    }

    private static void loadMat(Block[][] mat, String fileName) {
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
                    double[][] aux = new double[M][M];
                    for (int i_block = 0; i_block < M && !empty && tokens.hasMoreTokens(); i_block++) {
                        for (int j_block = 0; j_block < M && !empty && tokens.hasMoreTokens(); j_block++) {
                            String value = tokens.nextToken();
                            if (value.equals("null")) {
                                aux = null;
                                empty = true;
                            } else {
                                aux[i_block][j_block] = Double.parseDouble(value);
                            }
                        }
                    }
                    if (aux != null) {
                        Block aux2 = new Block(M);
                        aux2.setData(aux);
                        mat[row][col] = aux2;
                    } else {
                        mat[row][col] = null;
                    }
                }
                nextLine = br.readLine();
            }
            br.close();
            filereader.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    @SuppressWarnings("unused")
    private static void printMatrix(Block[][] matrix, String name) {
        System.out.println("MATRIX " + name + ":");
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                // matrix[i][j].blockToDisk(i, j, name);
                if (matrix[i][j] == null)
                    System.out.println("null");
                else
                    matrix[i][j].printBlock();
            }
        }
    }

}
