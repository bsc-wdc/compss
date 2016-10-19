package noTask;

public class Main {

    public static void main(String[] args) {
        // Check and get parameters
        if (args.length != 0) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("    Usage: noTask.Main");
            System.exit(-1);
        }

        // ------------------------------------------------------------------------
        // Write a hello message from a function that it is NOT a task
        String msg = "Hello World";
        printMessage(msg);
    }

    public static void printMessage(String msg) {
        // Print out message
        System.out.println(msg);
    }

}
