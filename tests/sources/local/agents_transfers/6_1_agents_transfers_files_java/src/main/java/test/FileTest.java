package test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;


/**
 * Tests transfers between agents.
 */
public class FileTest {

    public static final int MATRIX_SIZE_Y = 5;
    public static final int MATRIX_SIZE_X = 5;

    public static final String TEST_FILE_PATH = "/tmp/files_test_java/";


    private static int[][] loadMatrix(String path) throws Exception {
        FileInputStream fis = new FileInputStream(path);
        ObjectInputStream ois = new ObjectInputStream(fis);
        Object readCase = ois.readObject();
        ois.close();
        fis.close();
        return (int[][]) readCase;
    }

    private static void saveMatrix(int[][] matrix, String path) throws Exception {
        FileOutputStream fos = new FileOutputStream(path);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(matrix);
        oos.close();
        fos.close();
    }

    public static void print_mat(String path, String label) throws Exception {
        System.out.println("printing matrix " + label + ":");

        int[][] mat = loadMatrix(path);

        if (mat == null || mat[0] == null) {
            System.out.println("Empty matrix");
            return;
        }

        for (int i = 0; i < MATRIX_SIZE_Y; i++) {
            System.out.println(Arrays.toString(mat[i]));
        }
        System.out.println("------------------------------------");
    }

    public static void create_mat(String path, int value) {
        int[][] matrix = new int[MATRIX_SIZE_Y][MATRIX_SIZE_X];
        for (int i = 0; i < MATRIX_SIZE_Y; i++) {
            for (int j = 0; j < MATRIX_SIZE_X; j++) {
                matrix[i][j] = value;
            }
        }
        try {
            saveMatrix(matrix, path);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void nested_in_return(String pathA, String pathC, String label) throws Exception {
        int[][] matA = loadMatrix(pathA);
        print_mat(pathA, "input " + label);
        int currentValue = matA[0][0];
        create_mat(pathC, currentValue + 1);
        print_mat(pathC, "output " + label);
    }

    public static void in_return(String pathA, String pathC) throws Exception {
        nested_in_return(pathA, pathC, "nested in_return");
    }

    public static void in_return_w_print(String pathA, String pathC) throws Exception {
        print_mat(pathA, "input in_return_w_print");
        nested_in_return(pathA, pathC, "nested_in_return_w_print");
        print_mat(pathC, "output in_return_w_print");
    }

    public static void nested_inout(String pathC, String label) throws Exception {
        print_mat(pathC, "input " + label);
        int[][] matC = loadMatrix(pathC);

        for (int i = 0; i < MATRIX_SIZE_Y; i++) {
            for (int j = 0; j < MATRIX_SIZE_X; j++) {
                matC[i][j] += 1;
            }
        }

        saveMatrix(matC, pathC);
        print_mat(pathC, "output " + label);
    }

    public static void inout(String pathC) throws Exception {
        nested_inout(pathC, "nested_inout");
    }

    public static void inout_w_print(String pathC) throws Exception {
        print_mat(pathC, "input inout_w_print");
        nested_inout(pathC, "nested_inout_w_print");
        print_mat(pathC, "output inout_w_print");
    }

    public static void print_task(String pathC, String label) throws Exception {
        print_mat(pathC, label);
    }

    public static void nested_generation_return(String pathC) throws Exception {
        create_mat(pathC, 30);
        print_mat(pathC, "output nested_generation_return");
    }

    public static void generation_return(String pathC) throws Exception {
        nested_generation_return(pathC);
    }

    public static void consumption(String pathC, String label) throws Exception {
        print_task(pathC, label);
    }

    public static void nested_generation_inout(String pathC) throws Exception {
        print_mat(pathC, "input nested_generation_inout");

        int[][] matC = loadMatrix(pathC);
        for (int i = 0; i < MATRIX_SIZE_Y; i++) {
            for (int j = 0; j < MATRIX_SIZE_X; j++) {
                matC[i][j] += 1;
            }
        }
        saveMatrix(matC, pathC);
        print_mat(pathC, "output nested_generation_inout");
    }

    public static void generation_inout(String pathC) throws Exception {
        nested_generation_inout(pathC);
    }

    public static void main(String[] args) throws Exception {

        Path path = Paths.get(TEST_FILE_PATH);
        Files.createDirectories(path);

        String pathA = "matA";
        String pathC = "matC";

        // create_mat(pathA, 0);
        // in_return(pathA, pathC);
        // print_mat(pathC, "main result in_return");

        // create_mat(pathA, 10);
        // in_return_w_print(pathA, pathC);
        // print_mat(pathC, "main result in_return_w_print");

        create_mat(pathC, 20);

        inout_w_print(pathC);
        print_mat(pathC, "main result inout_w_print");

        // inout(pathC);
        // print_mat(pathC, "main result inout");

        // generation_return(pathC);
        // consumption(pathC, "consumption_return");

        // generation_inout(pathC);
        // consumption(pathC, "consumption_inout");

        // print_mat(pathC, "end in main");

    }

    public static void main_agents(String[] args) throws Exception {
        main(args);
    }

}
