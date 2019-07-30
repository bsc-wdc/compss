package objectDeregister;

public class ObjectDeregisterImpl {

    public static void task1(int n, Dummy d1) { // Writes
        d1.setDummyNumber(n);
    }

    public static void task2(int n, Dummy d2) { // Writes
        d2.setDummyNumber(n);
    }

    public static void task3(Dummy d3) { // Reads
        d3.getDummyNumber();
    }

    public static void task4() throws Exception { // Null
        System.gc();
        Thread.sleep(100);
        int k = ClassInstanceTest.countInstances(Dummy.class);
        if (k > 0) {
            throw new Exception(
                "[ERROR] At the end in the WORKER" + String.valueOf(k) + " instances of the Dummy object were found");
        }
    }

}
