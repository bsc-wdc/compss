package testReschedulingFail;

//N workers available, task fails in all of them
public class Main {

    public static void main(String[] args) {
        // Create a Dummy object to transfer to the task
        Dummy dummyObj = new Dummy();

        // Launch an error task
        Dummy d2 = errorTask(0, dummyObj);

        // Synchronize
        System.out.println("Finished task 0 (" + d2 + ")");
    }

    @SuppressWarnings("null")
    public static Dummy errorTask(int x, Dummy din) {
        Dummy d = null;

        // Execute a null access
        d.foo();

        return d;
    }

}
