package complexTest;

import java.io.FileOutputStream;
import java.io.Serializable;


@SuppressWarnings("serial")
public class Block implements Serializable {

    private int M;
    private double[][] data;


    // Constructors
    public Block() {
    }

    public Block(int bSize) {
        M = bSize;
        data = new double[M][M];

        for (int i = 0; i < M; i++)
            for (int j = 0; j < M; j++)
                data[i][j] = 0.0;
    }

    public static Block initBlock(int M) {
        Block block = new Block(M);
        for (int i = 0; i < M; i++) {
            for (int j = 0; j < M; j++) {
                block.data[i][j] = Math.random() * 10.0;
            }
        }
        return block;
    }

    // Getters
    public int getM() {
        return M;
    }

    public double[][] getData() {
        return data;
    }

    // Setters
    public void setM(int i) {
        M = i;
    }

    public void setData(double[][] d) {
        data = d;
    }

    // Print
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

    // Tasks for main job
    public void multiplyAccumulative(Block a, Block b) {
        for (int i = 0; i < M; i++) {
            for (int j = 0; j < M; j++) {
                for (int k = 0; k < M; k++) {
                    data[i][j] += a.data[i][k] * b.data[k][j];
                }
            }
        }
    }

}
