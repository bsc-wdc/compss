package complexTest;

public class DataLocality {

    private static int MSIZE;
    private static final int BSIZE = 2;

    private Block[][] A;
    private Block[][] B;
    private Block[][] C;


    public static void main(String[] args) {
        // Check and obtain parameters
        if (args.length != 1) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("Usage: dataLocality <msize>");
            System.exit(-1);
        }
        // Add for test stability
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // Nothing to do
        }

        MSIZE = Integer.parseInt(args[0]);

        // Show parameters
        System.out.println("[LOG] MSIZE parameter value = " + MSIZE);
        System.out.println("[LOG] BSIZE parameter value = " + BSIZE);

        // Run matmul app
        DataLocality matmul = new DataLocality();
        matmul.Run();

        // Check result
        System.out.println("[LOG] Main program finished. Result needs to be checked (result script)");
    }

    private void Run() {
        // Load Matrices
        System.out.println("[LOG] Creating matrix A");
        A = new Block[MSIZE][MSIZE];
        create("A");
        System.out.println("[LOG] Creating matrix B");
        B = new Block[MSIZE][MSIZE];
        create("B");
        System.out.println("[LOG] Allocating C Matrix space");
        C = new Block[MSIZE][MSIZE];

        // Compute result
        System.out.println("[LOG] Computing Result");
        for (int i = 0; i < MSIZE; i++) {
            for (int j = 0; j < MSIZE; j++) {
                C[i][j] = new Block(BSIZE);
                for (int k = 0; k < MSIZE; k++) {
                    C[i][j].multiplyAccumulative(A[i][k], B[k][j]);
                }
            }
        }
    }

    private void create(String tag) {
        for (int i = 0; i < MSIZE; ++i) {
            for (int j = 0; j < MSIZE; ++j) {
                if (tag.equals("A"))
                    A[i][j] = Block.initBlock(BSIZE);
                else if (tag.equals("B"))
                    B[i][j] = Block.initBlock(BSIZE);
            }
        }
    }

    @SuppressWarnings("unused")
    private void printMatrix(Block[][] matrix, String name) {
        System.out.println("MATRIX " + name);
        for (int i = 0; i < MSIZE; i++) {
            for (int j = 0; j < MSIZE; j++) {
                matrix[i][j].printBlock();
            }
            System.out.println("");
        }
    }

}
