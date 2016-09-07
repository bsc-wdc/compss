package taskSubmission;

public class FaultToleranceImpl {

    public static void task(int iter) {
        System.out.println("BEGIN OF TASK. Iterations = " + iter);
        for (int i = 0; i < iter; ++i) {
            System.out.println(String.valueOf(i));
        }
        System.out.println("END OF TASK");
    }

}
