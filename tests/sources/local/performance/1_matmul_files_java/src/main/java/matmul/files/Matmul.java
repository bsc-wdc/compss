package matmul.files;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


public class Matmul {

    private static final int MSIZE = 8;
    private static final int BSIZE = 2;

    private String[][] _A;
    private String[][] _B;
    private String[][] _C;


    public static void main(String[] args) {
        // Get parameters
        if (args.length != 3) {
            System.out.println("[ERROR] Usage: matmul <Ain> <Bin> <Cout>");
            System.exit(-1);
        }
        String fA = args[0];
        String fB = args[1];
        String fC = args[2];
        System.out.println("[LOG] MSIZE parameter value = " + MSIZE);
        System.out.println("[LOG] BSIZE parameter value = " + BSIZE);

        // Run matmul app
        Matmul matmul = new Matmul();
        matmul.Run(fA, fB);

        // Check result
        System.out.println("[LOG] Storing C matrix obtained");
        matmul.storeMatrix(fC);
        System.out.println("[LOG] Main program finished. Result needs to be checked (result script)");
    }

    private void Run(String fileA, String fileB) {
        // Initialize file Names
        System.out.println("[LOG] Initialising files' names for each matrix");
        initializeVariables();

        // Load A and B matrices
        System.out.println("[LOG] Loading matrix A from file");
        loadMatrix("A", fileA);
        System.out.println("[LOG] Loading matrix B from file");
        loadMatrix("B", fileB);

        // Create result files
        System.out.println("[LOG] Creating files for result computation");
        fillMatrix();

        // Compute result
        System.out.println("[LOG] Computing result");
        for (int i = 0; i < MSIZE; i++) {
            for (int j = 0; j < MSIZE; j++) {
                for (int k = 0; k < MSIZE; k++) {
                    MatmulImpl.multiplyAccumulative(_C[i][j], _A[i][k], _B[k][j]);
                }
            }
        }
    }

    private void initializeVariables() {
        _A = new String[MSIZE][MSIZE];
        _B = new String[MSIZE][MSIZE];
        _C = new String[MSIZE][MSIZE];
        for (int i = 0; i < MSIZE; i++) {
            for (int j = 0; j < MSIZE; j++) {
                _A[i][j] = "A." + i + "." + j;
                _B[i][j] = "B." + i + "." + j;
                _C[i][j] = "C." + i + "." + j;
            }
        }
    }

    private void loadMatrix(String tag, String fileName) {
        try {
            FileReader filereader = new FileReader(fileName);
            BufferedReader br = new BufferedReader(filereader);
            StringTokenizer tokens;
            String nextLine;
            for (int i = 0; i < MSIZE; ++i) {
                for (int j = 0; j < MSIZE; ++j) {
                    nextLine = br.readLine();
                    tokens = new StringTokenizer(nextLine);
                    FileOutputStream fos = null;
                    if (tag.equals("A"))
                        fos = new FileOutputStream(_A[i][j]);
                    else if (tag.equals("B"))
                        fos = new FileOutputStream(_B[i][j]);
                    else {
                        System.out.println("[ERROR] Bad load parameter.");
                        System.exit(1);
                    }
                    for (int iblock = 0; iblock < BSIZE && tokens.hasMoreTokens(); ++iblock) {
                        for (int jblock = 0; jblock < BSIZE && tokens.hasMoreTokens(); ++jblock) {
                            String value = tokens.nextToken() + " ";
                            fos.write(value.getBytes());
                        }
                        fos.write("\n".getBytes());
                    }
                    fos.close();
                }
                nextLine = br.readLine();
            }
            br.close();
            filereader.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

    private void fillMatrix() {
        try {
            for (int i = 0; i < MSIZE; i++) {
                for (int j = 0; j < MSIZE; j++) {
                    FileOutputStream fos = new FileOutputStream(_C[i][j]);
                    for (int ii = 0; ii < BSIZE; ii++) {
                        for (int jj = 0; jj < BSIZE; jj++) {
                            fos.write("0.0 ".getBytes());
                        }
                        fos.write("\n".getBytes());
                    }
                    fos.close();
                }
            }
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private void storeMatrix(String fileName) {
        try {
            FileOutputStream fos = new FileOutputStream(fileName);
            for (int i = 0; i < MSIZE; ++i) {
                for (int j = 0; j < MSIZE; ++j) {
                    FileReader filereader = new FileReader(_C[i][j]);
                    BufferedReader br = new BufferedReader(filereader);
                    StringTokenizer tokens;
                    String nextLine;
                    for (int iblock = 0; iblock < BSIZE; ++iblock) {
                        nextLine = br.readLine();
                        tokens = new StringTokenizer(nextLine);
                        for (int jblock = 0; jblock < BSIZE && tokens.hasMoreTokens(); ++jblock) {
                            String value = tokens.nextToken() + " ";
                            fos.write(value.getBytes());
                        }
                    }
                    fos.write("\n".getBytes());
                    br.close();
                    filereader.close();
                }
                fos.write("\n".getBytes());
            }
            fos.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
