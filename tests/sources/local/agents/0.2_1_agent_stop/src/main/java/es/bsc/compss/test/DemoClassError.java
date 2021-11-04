package es.bsc.compss.test;

public class DemoClassError {

    public static void demoFunction(String repeatsStr) throws Exception {
        throw new Exception("fake exception when executing to test agent stop on error");
    }

    public static void main(String[] args) {
        System.out.println("Executing main function");
        int repeats = Integer.parseInt(args[0]);
        method(repeats);
    }

    private static void method(int repeats) {
        for (int i = 0; i < repeats; i++) {
            System.out.println("Iteration " + i);
            addDelay();
        }
    }

    public static void addDelay() {
        System.out.println("Start time:" + System.currentTimeMillis());
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }
        System.out.println("End time:" + System.currentTimeMillis());
    }
}
