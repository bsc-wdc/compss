package graph.objects;

import java.io.FileOutputStream;
import java.io.Serializable;


@SuppressWarnings("serial")
public class Block implements Serializable {

    private int M;
    private double[][] data;


    public Block() {
    }

    public Block(int bSize) {
        M = bSize;
        data = new double[M][M];

        for (int i = 0; i < M; i++)
            for (int j = 0; j < M; j++)
                data[i][j] = 0.0;
    }

    public int getM() {
        return M;
    }

    public void setM(int i) {
        M = i;
    }

    public double[][] getData() {
        return data;
    }

    public void setData(double[][] d) {
        data = d;
    }

    public void printBlock() {
        for (int i = 0; i < M; i++)
            for (int j = 0; j < M; j++)
                System.out.print(data[i][j] + " ");
        System.out.println("");
    }

    public void blockToDisk(int i, int j, String name) {
        try {
            FileOutputStream fos = new FileOutputStream(name + "." + i + "." + j);

            for (int k1 = 0; k1 < M; k1++) {
                for (int k2 = 0; k2 < M; k2++) {
                    String str = data[k1][k2] + " ";
                    fos.write(str.getBytes());
                }
                fos.write("\n".getBytes());
            }
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void lu0() {
        for (int k = 0; k < M; k++) {
            for (int i = k + 1; i < M; i++) {
                data[i][k] /= data[k][k];
                for (int j = k + 1; j < M; j++) {
                    data[i][j] -= data[i][k] * data[k][j];
                }
            }
        }
    }

    public void bdiv(Block diag) {
        for (int i = 0; i < M; i++) {
            for (int k = 0; k < M; k++) {
                data[i][k] /= diag.data[k][k];
                for (int j = k + 1; j < M; j++) {
                    data[i][j] -= data[i][k] * diag.data[k][j];
                }
            }
        }
    }

    public void bmod(Block row, Block col) {
        for (int i = 0; i < M; i++)
            for (int j = 0; j < M; j++)
                for (int k = 0; k < M; k++)
                    data[i][j] -= row.data[i][k] * col.data[k][j];
    }

    public void fwd(Block diag) {
        for (int j = 0; j < M; j++)
            for (int k = 0; k < M; k++)
                for (int i = k + 1; i < M; i++)
                    data[i][j] -= diag.data[i][k] * data[k][j];
    }

    public static Block bmodAlloc(Block row, Block col) {
        Block block = new Block(row.M);
        block.bmod(row, col);
        return block;
    }

    public static Block initBlock(int ii, int jj, int N, int M) {
        Block block = new Block(M);
        block.init(ii, jj, N);
        return block;
    }

    private void init(int ii, int jj, int N) {
        int initVal = 1325;
        for (int k = 0; k < N; k++) {
            for (int l = 0; l < N; l++) {
                if (!isNull(k, l)) {
                    for (int i = 0; i < M; i++) {
                        for (int j = 0; j < M; j++) {
                            initVal = (3125 * initVal) % 65536;
                            if (k == ii && l == jj)
                                data[i][j] = ((initVal - 32768.0) / 16384.0);
                        }
                    }
                }
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
            nullEntry = false;//
        if (ii - 1 == jj)
            nullEntry = false;//

        return nullEntry;
    }

}
