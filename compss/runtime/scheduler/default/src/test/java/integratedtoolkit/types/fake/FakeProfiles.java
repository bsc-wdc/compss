package integratedtoolkit.types.fake;

import integratedtoolkit.scheduler.defaultscheduler.DefaultResourceScheduler;
import integratedtoolkit.types.Profile;

public class FakeProfiles {

    public static final Profile P2;
    public static final Profile P3;
    public static final Profile P5;

    static {
            FakeResourceDescription frd = new FakeResourceDescription(4);
            FakeWorker fw = new FakeWorker("worker1", frd);
            DefaultResourceScheduler drs = new DefaultResourceScheduler(fw);

            P2 = drs.generateProfileForAllocatable();
            P3 = drs.generateProfileForAllocatable();
            P5 = drs.generateProfileForAllocatable();

            P2.start();
            P5.start();
            long start = System.currentTimeMillis();
            waitUntil(start, 2_0);

            P2.end();
            P3.start();

            waitUntil(start, 5_0);
            P3.end();
            P5.end();
        
    }

    private static final void waitUntil(long start, long end) {
        long difference = System.currentTimeMillis() - start;
        while (difference < end) {
            try {
                Thread.sleep(end - difference);
            } catch (InterruptedException e) {
            }
            difference = System.currentTimeMillis() - start;
        }
    }
}
