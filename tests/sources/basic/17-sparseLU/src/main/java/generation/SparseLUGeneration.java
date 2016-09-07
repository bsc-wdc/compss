package generation;

import java.io.FileOutputStream;
import java.io.IOException;


public class SparseLUGeneration {

    public static int N; // Matrix size
    public static final int M = 2; // Block size
    private static double[][][] A;


    public static void main(String args[]) {
        // Load parameters
        if (args.length != 3) {
            System.out.println("Usage: sparselu <MatrixSize> <A.in> <A.out>");
            System.exit(-1);
        }
        N = Integer.parseInt(args[0]);
        String path_A_in = args[1];
        String path_A_out = args[2];
        // Report parameters
        System.out.println("[LOG] Running with the following parameters:");
        System.out.println("[LOG]  - N: " + N);
        System.out.println("[LOG]  - M: " + M);

        // Initializing A values
        System.out.println("[LOG] Initializing A Matrix");
        initialise();
        // Storing A in matrix
        System.out.println("[LOG] Storing A.in Matrix");
        storeMatrix(path_A_in);

        // Computing Sparse LU algorithm
        System.out.println("[LOG] Computing SparseLU algorithm on A");
        for (int kk = 0; kk < N; kk++) {
            lu0(A[kk][kk]);
            for (int jj = kk + 1; jj < N; jj++)
                if (A[kk][jj] != null)
                    fwd(A[kk][kk], A[kk][jj]);
            for (int ii = kk + 1; ii < N; ii++) {
                if (A[ii][kk] != null) {
                    bdiv(A[kk][kk], A[ii][kk]);
                    for (int jj = kk + 1; jj < N; jj++) {
                        if (A[kk][jj] != null) {
                            if (A[ii][jj] == null)
                                A[ii][jj] = bmodAlloc(A[ii][kk], A[kk][jj]);
                            else
                                bmod(A[ii][kk], A[kk][jj], A[ii][jj]);
                        }
                    }
                }
            }
        }

        // Loading expected results
        System.out.println("[LOG] Storing A.out Matrix");
        storeMatrix(path_A_out);
        System.out.println("[LOG] Main program finished");
    }

    private static void initialise() {
        A = new double[N][N][];
        for (int ii = 0; ii < N; ii++) {
            for (int jj = 0; jj < N; jj++) {
                if (isNull(ii, jj))
                    A[ii][jj] = null;
                else
                    A[ii][jj] = initBlock(ii, jj);
            }
        }
    }

    private static boolean isNull(int ii, int jj) {
        boolean nullEntry = false;
        if ((ii < jj) && (ii % 3 != 0))
            nullEntry = true;
        if ((ii > jj) && (jj % 3 != 0))
            nullEntry = true;
        if (ii % 2 == 1)
            nullEntry = true;
        if (jj % 2 == 1)
            nullEntry = true;
        if (ii == jj)
            nullEntry = false;
        if (ii == jj - 1)
            nullEntry = false;
        if (ii - 1 == jj)
            nullEntry = false;

        return nullEntry;
    }

    private static double[] initBlock(int ii, int jj) {
        double[] block = new double[M * M];
        if (!isNull(ii, jj)) {
            for (int i = 0; i < M; i++) {
                for (int j = 0; j < M; j++) {
                    block[i * M + j] = (Math.random()) * 100.0;
                }
            }
        }
        return block;
    }

    private static void lu0(double[] diag) {
        int M = (int) Math.sqrt(diag.length);
        for (int k = 0; k < M; k++) {
            for (int i = k + 1; i < M; i++) {
                diag[i * M + k] /= diag[k * M + k];
                for (int j = k + 1; j < M; j++) {
                    diag[i * M + j] -= diag[i * M + k] * diag[k * M + j];
                }
            }
        }
    }

    private static void bdiv(double[] diag, double[] row) {
        int M = (int) Math.sqrt(diag.length);
        for (int i = 0; i < M; i++) {
            for (int k = 0; k < M; k++) {
                row[i * M + k] /= diag[k * M + k];
                for (int j = k + 1; j < M; j++) {
                    row[i * M + j] -= row[i * M + k] * diag[k * M + j];
                }
            }
        }
    }

    private static void bmod(double[] row, double[] col, double[] inner) {
        int M = (int) Math.sqrt(row.length);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < M; j++)
                for (int k = 0; k < M; k++)
                    inner[i * M + j] -= row[i * M + k] * col[k * M + j];
    }

    private static void fwd(double[] diag, double[] col) {
        int M = (int) Math.sqrt(diag.length);
        for (int j = 0; j < M; j++)
            for (int k = 0; k < M; k++)
                for (int i = k + 1; i < M; i++)
                    col[i * M + j] -= diag[i * M + k] * col[k * M + j];
    }

    private static double[] bmodAlloc(double[] row, double[] col) {
        int M = (int) Math.sqrt(row.length);

        double[] inner = new double[M * M];
        for (int i = 0; i < inner.length; i++)
            inner[i] = 0.0;

        for (int i = 0; i < M; i++)
            for (int j = 0; j < M; j++)
                for (int k = 0; k < M; k++)
                    inner[i * M + j] -= row[i * M + k] * col[k * M + j];
        return inner;
    }

    private static void storeMatrix(String fileName) {
        try {
            FileOutputStream fos = new FileOutputStream(fileName);
            for (int i = 0; i < N; ++i) {
                for (int j = 0; j < N; ++j) {
                    if (A[i][j] == null) {
                        fos.write("null\n".getBytes());
                    } else {
                        for (int k = 0; k < M * M; ++k) {
                            String value = String.valueOf(A[i][j][k]) + " ";
                            fos.write(value.getBytes());
                        }
                        fos.write("\n".getBytes());
                    }
                }
                fos.write("\n".getBytes());
            }
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
