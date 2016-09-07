package matmul.files;

public class MatmulImpl {

    public static void multiplyAccumulative(String f3, String f1, String f2) {
        Block a = new Block(f1);
        Block b = new Block(f2);
        Block c = new Block(f3);

        c.multiplyAccum(a, b);
        c.blockToDisk(f3);
    }

}
