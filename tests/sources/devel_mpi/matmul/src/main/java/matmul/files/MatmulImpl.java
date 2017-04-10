package matmul.files;

import java.io.FileOutputStream;
import java.io.IOException;

import matmul.exceptions.MatmulException;


/**
 * Implementation of Task methods
 *
 */
public class MatmulImpl {

    private static final byte[] NEW_LINE = "\n".getBytes();


    /**
     * Initializes a block of size @BSIZE and stores it to @filename
     * 
     * @param filename
     * @param BSIZE
     * @param initRand
     * @throws IOException
     */
    public static void initializeBlock(String filename, int BSIZE, boolean initRand) throws MatmulException {
        try (FileOutputStream fos = new FileOutputStream(filename)) {
            for (int iblock = 0; iblock < BSIZE; ++iblock) {
                for (int jblock = 0; jblock < BSIZE; ++jblock) {
                    double value = (double) 0.0;
                    if (initRand) {
                        value = (double) (Math.random() * 10.0);
                    }
                    fos.write(String.valueOf(value).getBytes());
                    fos.write(" ".getBytes());
                }
                fos.write(NEW_LINE);
            }
            fos.write(NEW_LINE);
        } catch (IOException e) {
            throw new MatmulException("[ERROR] Error initializing matrix", e);
        }
    }

    /**
     * Multiplies blocks @aFile and @bFile, accumulates result on block @cFile and stores it to disk
     * 
     * @param BSIZE
     * @param aFile
     * @param bFile
     * @param cFile
     * @return
     */
    public static Integer multiplyAccumulativeNative(int BSIZE, String aFile, String bFile, String cFile) throws MatmulException {
        Block a = new Block(BSIZE, aFile);
        Block b = new Block(BSIZE, bFile);
        Block c = new Block(BSIZE, cFile);

        c.multiplyAccum(a, b);
        c.blockToDisk(cFile);

        return 0;
    }

}
