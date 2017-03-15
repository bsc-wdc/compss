package matmul.files;

import java.io.FileOutputStream;
import java.io.IOException;


public class MatmulImpl {

    private static final byte[] NEW_LINE = "\n".getBytes();


    public static void initializeBlock(String filename, int BSIZE, boolean initRand) throws IOException {
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
            throw new IOException("[ERROR] Error initializing matrix", e);
        }
    }

    public static Integer multiplyAccumulativeNative(int BSIZE, String aFile, String bFile, String cFile) {
        Block a = new Block(BSIZE, aFile);
        Block b = new Block(BSIZE, bFile);
        Block c = new Block(BSIZE, cFile);

        c.multiplyAccum(a, b);
        c.blockToDisk(cFile);

        return 0;
    }

}
