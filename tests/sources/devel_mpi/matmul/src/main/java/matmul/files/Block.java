package matmul.files;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Block representation
 *
 */
public class Block {

    private static final byte[] NEW_LINE = "\n".getBytes();

    private final int bSize;
    private final double[][] data;


    /**
     * Initialization of a block of size @bSize from file @filename
     * 
     * @param bSize
     * @param filename
     */
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

    /**
     * Dumps the content of the block to file @filename
     * 
     * @param filename
     */
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

    /**
     * Multiplicates blocks @a and @b and accumulates result on the current block
     * 
     * @param a
     * @param b
     */
    public void multiplyAccum(Block a, Block b) {
        for (int i = 0; i < this.bSize; ++i) {
            for (int k = 0; k < this.bSize; ++k) {
                for (int j = 0; j < this.bSize; ++j) {
                    this.data[i][j] += a.data[i][k] * b.data[k][j];
                }
            }
        }
    }

}
