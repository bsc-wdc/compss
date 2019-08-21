package es.bsc.compss.cbm3.objects;

import java.lang.management.ManagementFactory;
import java.util.Random;


public class Cbm3Impl {

    /**
     * Dummy in task.
     * 
     * @param sleepTime Sleep time.
     * @param objinLeft Object in left.
     * @param objinRight Object in right.
     * @return Resulting object.
     */
    public static DummyPayload runTaskIn(int sleepTime, DummyPayload objinLeft, DummyPayload objinRight) {
        computeSleep(sleepTime);
        objinRight.regen(objinRight.getSize());
        return objinLeft;
    }

    /**
     * Dummy inout task.
     * 
     * @param sleepTime Sleep time.
     * @param objinoutLeft Object inout left.
     * @param objinRight Object in right.
     */
    public static void runTaskInOut(int sleepTime, DummyPayload objinoutLeft, DummyPayload objinRight) {
        computeSleep(sleepTime);
        objinoutLeft.regen(objinoutLeft.getSize());
    }

    private static void computeSleep(int time) {
        long t = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId());
        while ((ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId()) - t)
            / 1000000 < time) {
            double x = new Random().nextDouble();
            for (int i = 0; i < 1000; ++i) {
                x = Math.atan(Math.sqrt(Math.pow(x, 10)));
            }
        }
    }
}
