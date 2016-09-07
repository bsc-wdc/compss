package fileTransfer;

public class FaultToleranceImpl {

    public static void taskLocalhost() {
        System.out.println("This task should be scheduled to Localhost");
    }

    public static void taskHostDown() {
        System.out.println("This task should be scheduled to HostDown. Can't --> Localhost");
    }

}
