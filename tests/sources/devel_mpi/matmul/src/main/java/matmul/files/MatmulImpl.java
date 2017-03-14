package matmul.files;

public class MatmulImpl {

    public static Integer multiplyAccumulativeNative(int BSIZE, String aFile, String bFile, String cFile) {
        Block a = new Block(BSIZE, aFile);
        Block b = new Block(BSIZE, bFile);
        Block c = new Block(BSIZE, cFile);

        c.multiplyAccum(a, b);
        c.blockToDisk(cFile);

        return 0;
    }

}
