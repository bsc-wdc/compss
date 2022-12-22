package test;

/**
 * Tests transfers between agents.
 */
public class ObjectTest {

    public static final int MATRIX_SIZE_Y = 5;
    public static final int MATRIX_SIZE_X = 5;


    public static void print_mat(Matrix mat, String label) {
        System.out.println("printing matrix " + label + ":");

        System.out.println(mat.toString());
        System.out.println("------------------------------------");
    }

    public static Matrix create_mat(int value) {
        return new Matrix(MATRIX_SIZE_Y, MATRIX_SIZE_X, value);
    }

    public static Matrix nested_in_return(Matrix matA, String label) {
        print_mat(matA, "input " + label);
        int currentValue = matA.getVal();
        Matrix matC = create_mat(currentValue + 1);
        print_mat(matC, "output " + label);
        return matC;
    }

    public static Matrix in_return(Matrix matA) {
        Matrix matC = nested_in_return(matA, "nested in_return");
        return matC;
    }

    public static Matrix in_return_w_print(Matrix matA) {
        print_mat(matA, "input in_return_w_print");
        Matrix matC = nested_in_return(matA, "nested_in_return_w_print");
        print_mat(matC, "output in_return_w_print");
        return matC;
    }

    public static void nested_inout(Matrix matC, String label) {
        print_mat(matC, "input " + label);

        matC.setVal(matC.getVal());

        print_mat(matC, "output " + label);
    }

    public static void inout(Matrix matC) {
        nested_inout(matC, "nested_inout");
    }

    public static void inout_w_print(Matrix matC) {
        print_mat(matC, "input inout_w_print");
        nested_inout(matC, "nested_inout_w_print");
        print_mat(matC, "output inout_w_print");
    }

    public static void print_task(Matrix matC, String label) {
        print_mat(matC, label);
    }

    public static Matrix nested_generation_return() {
        Matrix matC = create_mat(30);
        print_mat(matC, "output nested_generation_return");
        return matC;
    }

    public static Matrix generation_return() {
        Matrix matC = nested_generation_return();
        return matC;
    }

    public static void consumption(Matrix matC, String label) {
        print_task(matC, label);
    }

    public static void nested_generation_inout(Matrix matC) {
        print_mat(matC, "input nested_generation_inout");

        matC.setVal(matC.getVal());

        print_mat(matC, "output nested_generation_inout");
    }

    public static void generation_inout(Matrix matC) {
        nested_generation_inout(matC);
    }

    public static void main(String[] args) {
        Matrix matA;
        Matrix matC;

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
