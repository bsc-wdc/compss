package matmul.files;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


public class Block {

    private static final byte[] NEW_LINE = "\n".getBytes();

    private final int bSize;
    private final double[][] data;


    public Block(int bSize, String filename) {
        this.bSize = bSize;
        this.data = new double[this.bSize][this.bSize];

        // Retrieve values from file
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            StringTokenizer tokens;
            String nextLine;

            for (int i = 0; i < this.bSize; i++) {
                nextLine = br.readLine();
                tokens = new StringTokenizer(nextLine);
                for (int j = 0; j < bSize && tokens.hasMoreTokens(); j++) {
                    this.data[i][j] = Double.parseDouble(tokens.nextToken());
                }
            }
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

    }

    protected void printBlock() {
        for (int i = 0; i < this.bSize; i++) {
            for (int j = 0; j < this.bSize; j++) {
                System.out.print(this.data[i][j] + " ");
            }
            System.out.println();
        }
    }

    public void blockToDisk(String filename) {
        try (FileOutputStream fos = new FileOutputStream(filename)) {
            for (int i = 0; i < this.bSize; i++) {
                for (int j = 0; j < this.bSize; j++) {
                    String str = String.valueOf(this.data[i][j]) + " ";
                    fos.write(str.getBytes());
                }
                fos.write(NEW_LINE);
            }
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

    public void multiplyAccum(Block a, Block b) {
        for (int i = 0; i < this.bSize; i++) {
            for (int j = 0; j < this.bSize; j++) {
                for (int k = 0; k < this.bSize; k++) {
                    this.data[i][j] += a.data[i][k] * b.data[k][j];
                }
            }
        }
    }

}
