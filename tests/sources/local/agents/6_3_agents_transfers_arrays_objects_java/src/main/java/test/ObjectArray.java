package test;

import java.util.Arrays;


/**
 * Tests transfers between agents.
 */
public class ObjectArray {

    public static final int MATRIX_SIZE_Y = 5;
    public static final int MATRIX_SIZE_X = 5;


    public static void print_mat(Integer[][] mat, String label) {
        System.out.println("printing matrix " + label + ":");

        if (mat == null || mat[0] == null) {
            System.out.println("Empty matrix");
            return;
        }

        for (int i = 0; i < MATRIX_SIZE_Y; i++) {
            System.out.println(Arrays.toString(mat[i]));
        }
        System.out.println("------------------------------------");
    }

    public static Integer[][] create_mat(int value) {
        Integer[][] mat = new Integer[MATRIX_SIZE_Y][MATRIX_SIZE_X];
        for (int i = 0; i < MATRIX_SIZE_Y; i++) {
            for (int j = 0; j < MATRIX_SIZE_X; j++) {
                mat[i][j] = value;
            }
        }
        return mat;
    }

    public static Integer[][] nested_in_return(Integer[][] matA, String label) {
        print_mat(matA, "input " + label);
        int currentValue = matA[0][0];
        Integer[][] matC = create_mat(currentValue + 1);
        print_mat(matC, "output " + label);
        return matC;
    }

    public static Integer[][] in_return(Integer[][] matA) {
        Integer[][] matC = nested_in_return(matA, "nested in_return");
        System.out.println(matC);
        return matC;
    }

    public static Integer[][] in_return_w_print(Integer[][] matA) {
        print_mat(matA, "input in_return_w_print");
        Integer[][] matC = nested_in_return(matA, "nested_in_return_w_print");
        print_mat(matC, "output in_return_w_print");
        return matC;
    }

    public static void nested_inout(Integer[][] matC, String label) {
        print_mat(matC, "input " + label);

        for (int i = 0; i < MATRIX_SIZE_Y; i++) {
            for (int j = 0; j < MATRIX_SIZE_X; j++) {
                matC[i][j] += 1;
            }
        }

        print_mat(matC, "output " + label);
    }

    public static void inout(Integer[][] matC) {
        nested_inout(matC, "nested_inout");
    }

    public static void inout_w_print(Integer[][] matC) {
        print_mat(matC, "input inout_w_print");
        nested_inout(matC, "nested_inout_w_print");
        print_mat(matC, "output inout_w_print");
    }

    public static void print_task(Integer[][] matC, String label) {
        print_mat(matC, label);
    }

    public static Integer[][] nested_generation_return() {
        Integer[][] matC = create_mat(30);
        print_mat(matC, "output nested_generation_return");
        return matC;
    }

    public static Integer[][] generation_return() {
        Integer[][] matC = nested_generation_return();
        System.out.println(matC);
        return matC;
    }

    public static void consumption(Integer[][] matC, String label) {
        print_task(matC, label);
    }

    public static void nested_generation_inout(Integer[][] matC) {
        print_mat(matC, "input nested_generation_inout");

        for (int i = 0; i < MATRIX_SIZE_Y; i++) {
            for (int j = 0; j < MATRIX_SIZE_X; j++) {
                matC[i][j] += 1;
            }
        }
        print_mat(matC, "output nested_generation_inout");
    }

    public static void generation_inout(Integer[][] matC) {
        nested_generation_inout(matC);
    }

    public static void main(String[] args) {
        Integer[][] matA;
        Integer[][] matC;

        matA = create_mat(0);
        matC = in_return(matA);
        print_mat(matC, "main result in_return");

        matA = create_mat(10);
        matC = in_return_w_print(matA);
        print_mat(matC, "main result in_return_w_print");

        matC = create_mat(20);

        inout_w_print(matC);
        print_mat(matC, "main result inout_w_print");

        inout(matC);
        print_mat(matC, "main result inout");

        matC = generation_return();
        consumption(matC, "consumption_return");

        generation_inout(matC);
        consumption(matC, "consumption_inout");

        print_mat(matC, "end in main");

    }

    public static void main_agents(String[] args) {
        main(args);
    }

}
