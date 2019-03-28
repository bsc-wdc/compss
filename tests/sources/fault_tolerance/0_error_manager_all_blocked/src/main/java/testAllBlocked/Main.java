package testAllBlocked;

// No available workers to execute the application
// (using impossible constraints - see MainItf.java)
public class Main {

    private static int NUM_TASKS = 2;


    public static void main(String[] args) {
        Dummy[] dummies = new Dummy[NUM_TASKS];
        for (int i = 0; i < NUM_TASKS; ++i) {
            dummies[i] = normalTask(i, new Dummy());
        }

        for (int i = 0; i < NUM_TASKS; ++i) {
            System.out.println("Finished task " + (i + 1) + " (" + dummies[i] + ")");
        }
    }

    public static Dummy normalTask(int x, Dummy din) {
        Dummy d = null;
        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            // No need to catch such exception
        }
        return d;
    }

}
