package sparseLU.files;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


public class SparseLU {

    public static final int N = 16; // Matrix size
    public static final int BLOCK_SIZE = 2; // Block size
    private static boolean[][] matriu;


    public static void main(String args[]) {
        // Load file locations
        if (args.length != 2) {
            System.out.println("Usage: sparselu <A.in> <AGot.out>");
            System.exit(-1);
        }
        String path_A_in = args[0];
        String path_A_out = args[1];

        // Report parameters
        System.out.println("[LOG] Running with the following parameters:");
        System.out.println("[LOG]  - N: " + N);
        System.out.println("[LOG]  - M: " + BLOCK_SIZE);

        // Initializing A values
        System.out.println("[LOG] Initializing Matrix from file");
        matriu = new boolean[N][N];
        loadInitialMat(path_A_in);

        // Computing Sparse LU algorithm
        System.out.println("[LOG] Computing SparseLU algorithm on A");
        String file, file1, file2, file3, file4;
        try {
            for (int k = 0; k < N; k++) {
                file = "A." + k + "." + k;
                SparseLUImpl.lu0(file);
                for (int j = k + 1; j < N; j++) {
                    if (matriu[k][j]) {
                        file1 = "A." + k + "." + j;
                        SparseLUImpl.fwd(file, file1);
                    }
                }
                for (int i = k + 1; i < N; i++) {
                    if (matriu[i][k]) {
                        file2 = "A." + i + "." + k;
                        SparseLUImpl.bdiv(file, file2);
                        for (int j = k + 1; j < N; j++) {
                            if (matriu[k][j]) {
                                file3 = "A." + k + "." + j;
                                file4 = "A." + i + "." + j;
                                if (!matriu[i][j])
                                    matriu[i][j] = true;
                                SparseLUImpl.bmod(file2, file3, file4);
                            }
                        }
                    }
                }
            }
        } catch (SparseLUAppException se) {
            System.out.println("[ERROR] Error computing SPARSELU algorithm");
            System.exit(-1);
        }

        // Store obtained results
        System.out.println("[LOG] Storing output matrix file obtained");
        storeMatrix(path_A_out);

        System.out.println("[LOG] Main program finished. Result needs to be checked (script).");
    }

    private static void loadInitialMat(String fileName) {
        double[][][] m = loadFileToArray(fileName);
        for (int i = 0; i < m.length; ++i) {
            for (int j = 0; j < m[i].length; ++j) {
                String name = "A." + i + "." + j;
                try {
                    FileOutputStream fos = new FileOutputStream(name);
                    for (int i_block = 0; i_block < BLOCK_SIZE; i_block++) {
                        for (int j_block = 0; j_block < BLOCK_SIZE; j_block++) {
                            if (m[i][j] != null) {
                                matriu[i][j] = true;
                                String str = String.valueOf(m[i][j][i_block * BLOCK_SIZE + j_block]) + " ";
                                fos.write(str.getBytes());
                            } else {
                                matriu[i][j] = false;
                                fos.write("0.0 ".getBytes());
                            }
                        }
                        fos.write("\n".getBytes());
                    }
                    fos.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                    System.out.println("[ERROR] Error loading input file.");
                    System.exit(-1);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("[ERROR] Error loading input file.");
                    System.exit(-1);
                }
            }
        }
    }

    private static double[][][] loadFileToArray(String fileName) {
        double[][][] mat = new double[N][N][BLOCK_SIZE * BLOCK_SIZE];
        try {
            FileReader filereader = new FileReader(fileName);
            BufferedReader br = new BufferedReader(filereader);
            StringTokenizer tokens;
            String nextLine;

            for (int row = 0; row < N; row++) {
                for (int col = 0; col < N; col++) {
                    nextLine = br.readLine();
                    tokens = new StringTokenizer(nextLine);
                    boolean empty = false;
                    for (int block = 0; block < BLOCK_SIZE * BLOCK_SIZE && !empty && tokens.hasMoreTokens(); block++) {
                        String value = tokens.nextToken();
                        if (value.equals("null")) {
                            mat[row][col] = null;
                            empty = true;
                        } else {
                            mat[row][col][block] = Double.parseDouble(value);
                        }
                    }
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
        return mat;
    }

    private static void storeMatrix(String fileName) {
        try {
            FileOutputStream fos = new FileOutputStream(fileName);
            for (int i = 0; i < N; ++i) {
                for (int j = 0; j < N; ++j) {
                    if (!matriu[i][j]) {
                        fos.write("null\n".getBytes());
                    } else {
                        String blockName = "A." + i + "." + j;
                        FileReader filereader = new FileReader(blockName);
                        BufferedReader br = new BufferedReader(filereader);
                        StringTokenizer tokens;
                        String nextLine;
                        for (int iblock = 0; iblock < BLOCK_SIZE; ++iblock) {
                            nextLine = br.readLine();
                            tokens = new StringTokenizer(nextLine);
                            for (int jblock = 0; jblock < BLOCK_SIZE && tokens.hasMoreTokens(); ++jblock) {
                                String value = tokens.nextToken() + " ";
                                fos.write(value.getBytes());
                            }
                        }
                        br.close();
                        filereader.close();
                        fos.write("\n".getBytes());
                    }
                }
                fos.write("\n".getBytes());
            }
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }
}
