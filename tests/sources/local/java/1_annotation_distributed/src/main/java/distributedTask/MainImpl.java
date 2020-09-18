package distributedTask;

public class MainImpl {

    private static final int WAIT_TIME = 2_000;


    public static void normalTask(String msg) {
        task(msg);
    }

    public static void distributedTask(String msg) {
        task(msg);
    }

    private static void task(String msg) {
        System.out.println(msg);

        try {
            Thread.sleep(WAIT_TIME);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
