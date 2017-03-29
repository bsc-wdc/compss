package matmul.files;

import java.io.IOException;

import integratedtoolkit.api.COMPSs;
import mpi.MPI;


/**
 * MATMUL Implementation
 *
 */
public class Matmul {

    private static int TYPE;
    private static int MSIZE;
    private static int BSIZE;

    private static String[][] AfileNames;
    private static String[][] BfileNames;
    private static String[][] CfileNames;


    /**
     * MAIN CODE
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Check and get parameters
        if (args.length != 3) {
            System.out.println("Usage: matmul.files.Matmul <type> <MSize> <BSize>");
            throw new Exception("[ERROR] Incorrect number of parameters");
        }
        TYPE = Integer.parseInt(args[0]);
        MSIZE = Integer.parseInt(args[1]);
        BSIZE = Integer.parseInt(args[2]);

        // Initialize matrices
        System.out.println("[LOG] TYPE parameter value = " + TYPE);
        System.out.println("[LOG] MSIZE parameter value = " + MSIZE);
        System.out.println("[LOG] BSIZE parameter value = " + BSIZE);
        initializeVariables();
        initializeMatrix(AfileNames, true);
        initializeMatrix(BfileNames, true);
        initializeMatrix(CfileNames, false);

        // Wait for initialization
        COMPSs.barrier();

        // Compute matrix multiplication C = A x B
        long startTime = System.currentTimeMillis();
        computeMultiplication();
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("[TIME] EXECUTION TIME = " + estimatedTime);

        // End
        System.out.println("[LOG] Main program finished.");
    }

    /**
     * Initializes the filenames
     * 
     */
    private static void initializeVariables() {
        AfileNames = new String[MSIZE][MSIZE];
        BfileNames = new String[MSIZE][MSIZE];
        CfileNames = new String[MSIZE][MSIZE];
        for (int i = 0; i < MSIZE; i++) {
            for (int j = 0; j < MSIZE; j++) {
                AfileNames[i][j] = "A." + i + "." + j;
                BfileNames[i][j] = "B." + i + "." + j;
                CfileNames[i][j] = "C." + i + "." + j;
            }
        }
    }

    /**
     * Initializes each fileName with random values or 0s
     * 
     * @param fileNames
     * @param initRand
     * @throws IOException
     */
    private static void initializeMatrix(String[][] fileNames, boolean initRand) throws IOException {
        for (int i = 0; i < MSIZE; ++i) {
            for (int j = 0; j < MSIZE; ++j) {
                MatmulImpl.initializeBlock(fileNames[i][j], BSIZE, initRand);
            }
        }
    }

    /**
     * Main loop of matrix multiplication
     * 
     */
    private static void computeMultiplication() {
        System.out.println("[LOG] Computing result");
        Integer[][][] exitValues = new Integer[MSIZE][MSIZE][MSIZE];

        // Launch tasks
        for (int i = 0; i < MSIZE; ++i) {
            for (int k = 0; k < MSIZE; ++k) {
                for (int j = 0; j < MSIZE; ++j) {
                    switch (TYPE) {
                        case 1:
                            exitValues[i][j][k] = MatmulImpl.multiplyAccumulativeNative(BSIZE, AfileNames[i][k], BfileNames[k][j],
                                    CfileNames[i][j]);
                            break;
                        case 2:
                            exitValues[i][j][k] = MPI.multiplyAccumulativeMPI(BSIZE, AfileNames[i][k], BfileNames[k][j], CfileNames[i][j]);
                            break;
                        default:
                            System.err.println("[ERROR] Invalid type");
                            System.exit(1);
                            break;
                    }
                }
            }
        }

        // Sync: Wait loop
        for (int i = 0; i < MSIZE; i++) {
            for (int j = 0; j < MSIZE; j++) {
                for (int k = 0; k < MSIZE; k++) {
                    if (exitValues[i][j][k] != 0) {
                        System.err.println("[ERROR] Some task failed with exitValue " + exitValues[i][j][k]);
                    }
                }
            }
        }
    }

}
