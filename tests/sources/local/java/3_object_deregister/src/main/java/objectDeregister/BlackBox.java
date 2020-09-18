package objectDeregister;

public class BlackBox {

    public static void method(Dummy d) {
        // Method that should not be instrumented to detect
        // dependencies between tasks and main code.
    }
}