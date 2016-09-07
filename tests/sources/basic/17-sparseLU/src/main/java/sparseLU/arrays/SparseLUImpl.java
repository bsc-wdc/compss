package sparseLU.arrays;

public class SparseLUImpl {

    public static void lu0(double[] diag) {
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

    public static void bdiv(double[] diag, double[] row) {
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

    public static void bmod(double[] row, double[] col, double[] inner) {
        int M = (int) Math.sqrt(row.length);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < M; j++)
                for (int k = 0; k < M; k++)
                    inner[i * M + j] -= row[i * M + k] * col[k * M + j];
    }

    public static void fwd(double[] diag, double[] col) {
        int M = (int) Math.sqrt(diag.length);
        for (int j = 0; j < M; j++)
            for (int k = 0; k < M; k++)
                for (int i = k + 1; i < M; i++)
                    col[i * M + j] -= diag[i * M + k] * col[k * M + j];
    }

    public static double[] bmodAlloc(double[] row, double[] col) {
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

    public static double[] initBlock(int ii, int jj, int N, int M) {
        double[] block = new double[M * M];
        int initVal = 1325;
        for (int k = 0; k < N; k++) {
            for (int l = 0; l < N; l++) {
                if (!isNull(k, l)) {
                    for (int i = 0; i < M; i++) {
                        for (int j = 0; j < M; j++) {
                            initVal = (3125 * initVal) % 65536;
                            if (k == ii && l == jj)
                                block[i * M + j] = ((initVal - 32768.0) / 16384.0);
                        }
                    }
                }
            }
        }
        return block;
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
            nullEntry = false;//
        if (ii - 1 == jj)
            nullEntry = false;//

        return nullEntry;
    }

    public static void printBlock(double[] block, int M) {
        if (block == null)
            System.out.println("null");
        else {
            for (int k = 0; k < M * M; k++) {
                System.out.print(block[k] + " ");
            }
            System.out.println("");
        }
    }

}
