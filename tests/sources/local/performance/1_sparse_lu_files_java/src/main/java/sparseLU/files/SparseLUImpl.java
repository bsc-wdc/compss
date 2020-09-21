package sparseLU.files;

public class SparseLUImpl {

    public static void lu0(String diag) throws SparseLUAppException {
        Block a = new Block(diag);
        a.lu0();
        a.blockToDisk(diag);
    }

    public static void bdiv(String diag, String row) throws SparseLUAppException {
        Block a = new Block(diag);
        Block b = new Block(row);
        b.bdiv(a);
        b.blockToDisk(row);
    }

    public static void bmod(String row, String col, String inner) throws SparseLUAppException {
        Block a = new Block(row);
        Block b = new Block(col);
        Block c = new Block(inner);

        c.bmod(a, b);
        c.blockToDisk(inner);
    }

    public static void fwd(String diag, String col) throws SparseLUAppException {
        Block a = new Block(diag);
        Block b = new Block(col);
        b.fwd(a);
        b.blockToDisk(col);
    }

}
