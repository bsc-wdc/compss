package objectDeregister;

import es.bsc.compss.api.COMPSs;


public class ObjectDeregister {

    public static void main(String[] args) throws Exception {

        /*
         * This test provides a Dummy object used to provoke a situation where useless objects remain in memory until
         * the end of execution, this is creating a loop using the same object all over.
         */

        int k;
        final int ITERATIONS = 10;
        // Dummy inDelDummy = null;
        for (int i = 0; i < ITERATIONS; ++i) {
            Dummy d = new Dummy(i);
            Dummy inDelDummy = new Dummy(i); // This object should be removed automatically
            ObjectDeregisterImpl.task1(i, d);
            ObjectDeregisterImpl.task2(i + 1, d, inDelDummy);
            ObjectDeregisterImpl.task3(d);
            // inDelDummy = null;
            // Allows garbage collector to delete the object from memory
            COMPSs.deregisterObject((Object) d);
        }

        COMPSs.barrier();
        Thread.sleep(2000);
        ObjectDeregisterImpl.task5();
        COMPSs.barrier();
        System.gc();
        Thread.sleep(2000);
        System.gc();
        System.out.println("GC performed");
        Thread.sleep(4000);

        k = ClassInstanceTest.countInstances(Dummy.class);
        if (k > 0) {
            System.out.println("[ERROR] At the end in the MASTER 1, " + String.valueOf(k)
                + " instances of the Dummy object were found");
            System.exit(-1);
        }

        /*
         * task1 & task2 write into the object so a copy of the object will be created, the task3 will just read the
         * object task2 used
         */

        /*
         * This second part of the test checks that accesses from the main are properly handled.
         */
        // IN
        Dummy dIn = new Dummy(1);
        ObjectDeregisterImpl.task3(dIn);
        BlackBox.method(dIn);
        COMPSs.deregisterObject((Object) dIn);
        dIn = null;

        COMPSs.barrier();
        System.gc();
        Thread.sleep(2000);
        System.gc();
        Thread.sleep(4000);
        k = ClassInstanceTest.countInstances(Dummy.class);
        if (k > 0) {
            System.out.println("[ERROR] At the end in the MASTER 2, " + String.valueOf(k)
                + " instances of the Dummy object were found");
            System.exit(-1);
        }

        // OUT
        Dummy dOut = ObjectDeregisterImpl.task4(1);
        BlackBox.method(dOut);
        COMPSs.deregisterObject((Object) dOut);
        dOut = null;
        COMPSs.barrier();
        System.gc();
        Thread.sleep(2000);
        System.gc();
        Thread.sleep(4000);
        k = ClassInstanceTest.countInstances(Dummy.class);
        if (k > 0) {
            System.out.println("[ERROR] At the end in the MASTER 3, " + String.valueOf(k)
                + " instances of the Dummy object were found");
            System.exit(-1);
        }

        // INOUT
        Dummy dInout = new Dummy(2);
        ObjectDeregisterImpl.task1(2, dInout);
        BlackBox.method(dInout);
        COMPSs.deregisterObject((Object) dInout);
        dInout = null;
        COMPSs.barrier();
        System.gc();
        Thread.sleep(2000);
        System.gc();
        Thread.sleep(4000);
        k = ClassInstanceTest.countInstances(Dummy.class);
        if (k > 0) {
            System.out.println("[ERROR] At the end in the MASTER 4, " + String.valueOf(k)
                + " instances of the Dummy object were found");
            System.exit(-1);
        }
    }

}
