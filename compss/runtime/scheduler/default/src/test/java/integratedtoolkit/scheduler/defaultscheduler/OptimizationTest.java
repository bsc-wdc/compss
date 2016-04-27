package integratedtoolkit.scheduler.defaultscheduler;

import integratedtoolkit.scheduler.defaultscheduler.utils.Verifiers;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.types.Gap;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.fake.FakeAllocatableAction;
import integratedtoolkit.types.fake.FakeImplementation;
import integratedtoolkit.types.fake.FakeProfiles;
import integratedtoolkit.types.fake.FakeResourceDescription;
import integratedtoolkit.types.fake.FakeWorker;
import integratedtoolkit.util.CoreManager;

import java.util.LinkedList;

import org.junit.BeforeClass;
import org.junit.Test;


public class OptimizationTest {

    private static DefaultScheduler ds;
    private static DefaultResourceScheduler drs;
    private static DefaultResourceScheduler secondDRS;

    private static long CORE0;
    private static long CORE1;
    private static long CORE2;
    private static long CORE3;

    public OptimizationTest() {

    }

    @BeforeClass
    public static void setUpClass() {
        CoreManager.clear();
        CoreManager.resizeStructures(4);

        ds = new DefaultScheduler();

        Implementation<?> impl00 = new FakeImplementation(0, 0, new FakeResourceDescription(2));
        CoreManager.registerImplementations(0, new Implementation[]{impl00});
        Implementation<?> impl10 = new FakeImplementation(1, 0, new FakeResourceDescription(3));
        CoreManager.registerImplementations(1, new Implementation[]{impl10});
        Implementation<?> impl20 = new FakeImplementation(2, 0, new FakeResourceDescription(1));
        CoreManager.registerImplementations(2, new Implementation[]{impl20});
        Implementation<?> impl30 = new FakeImplementation(3, 0, new FakeResourceDescription(4));
        CoreManager.registerImplementations(3, new Implementation[]{impl30});

        FakeResourceDescription frd = new FakeResourceDescription(4);
        FakeWorker fw = new FakeWorker("worker1", frd);
        drs = new DefaultResourceScheduler(fw);

        FakeResourceDescription frd2 = new FakeResourceDescription(4);
        FakeWorker fw2 = new FakeWorker("worker2", frd2);
        secondDRS = new DefaultResourceScheduler(fw2);

        drs.profiledExecution(impl00, FakeProfiles.P5);
        drs.profiledExecution(impl10, FakeProfiles.P5);
        drs.profiledExecution(impl20, FakeProfiles.P3);
        drs.profiledExecution(impl30, FakeProfiles.P5);

        CORE0 = drs.getProfile(impl00).getAverageExecutionTime();
        CORE1 = drs.getProfile(impl10).getAverageExecutionTime();
        CORE2 = drs.getProfile(impl20).getAverageExecutionTime();
        CORE3 = drs.getProfile(impl30).getAverageExecutionTime();

        //debugConfiguration();
    }

    @Test
    public void testNoDataDependencies() throws BlockedActionException, UnassignedActionException, InvalidSchedulingException, InterruptedException {
        //Build graph
        /*
         * 1 --> 3 --> 5 -->6 --> 8 -->9  ----->11 -->12 --> 13
         * 2 --> 4 ┘     └->7 ┘     └->10 ---|     └-----┘
         *                                   |           |
         *                                 14┘         15┘
         */
        drs.clear();

        FakeAllocatableAction action1 = new FakeAllocatableAction(1, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action2 = new FakeAllocatableAction(2, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action3 = new FakeAllocatableAction(3, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action4 = new FakeAllocatableAction(4, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action5 = new FakeAllocatableAction(5, CoreManager.getCoreImplementations(1));
        FakeAllocatableAction action6 = new FakeAllocatableAction(6, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action7 = new FakeAllocatableAction(7, CoreManager.getCoreImplementations(2));
        FakeAllocatableAction action8 = new FakeAllocatableAction(8, CoreManager.getCoreImplementations(3));
        FakeAllocatableAction action9 = new FakeAllocatableAction(9, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action10 = new FakeAllocatableAction(10, CoreManager.getCoreImplementations(2));
        FakeAllocatableAction action11 = new FakeAllocatableAction(11, CoreManager.getCoreImplementations(3));
        FakeAllocatableAction action12 = new FakeAllocatableAction(12, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action13 = new FakeAllocatableAction(13, CoreManager.getCoreImplementations(1));

        FakeAllocatableAction action14 = new FakeAllocatableAction(14, CoreManager.getCoreImplementations(0));
        action14.selectExecution(secondDRS, action14.getImplementations()[0]);
        DefaultSchedulingInformation dsi14 = (DefaultSchedulingInformation) action14.getSchedulingInfo();
        dsi14.setExpectedEnd(10_000);

        FakeAllocatableAction action15 = new FakeAllocatableAction(15, CoreManager.getCoreImplementations(0));
        action15.selectExecution(secondDRS, action15.getImplementations()[0]);
        DefaultSchedulingInformation dsi15 = (DefaultSchedulingInformation) action15.getSchedulingInfo();
        dsi15.setExpectedEnd(12_000);

        action1.selectExecution(drs, action1.getImplementations()[0]);
        action1.tryToLaunch();

        action2.selectExecution(drs, action2.getImplementations()[0]);
        action2.tryToLaunch();

        action3.selectExecution(drs, action3.getImplementations()[0]);
        drs.addSchedulingDependency(action1, action3);

        action4.selectExecution(drs, action4.getImplementations()[0]);
        drs.addSchedulingDependency(action2, action4);

        action5.selectExecution(drs, action5.getImplementations()[0]);
        drs.addSchedulingDependency(action3, action5);
        drs.addSchedulingDependency(action4, action5);

        action6.selectExecution(drs, action6.getImplementations()[0]);
        drs.addSchedulingDependency(action5, action6);

        action7.selectExecution(drs, action7.getImplementations()[0]);
        drs.addSchedulingDependency(action5, action7);

        action8.selectExecution(drs, action8.getImplementations()[0]);
        drs.addSchedulingDependency(action6, action8);
        drs.addSchedulingDependency(action7, action8);

        action9.selectExecution(drs, action9.getImplementations()[0]);
        drs.addSchedulingDependency(action8, action9);

        action10.selectExecution(drs, action10.getImplementations()[0]);
        drs.addSchedulingDependency(action8, action10);

        action11.selectExecution(drs, action11.getImplementations()[0]);
        drs.addSchedulingDependency(action9, action11);
        drs.addSchedulingDependency(action10, action11);
        action11.addDataPredecessor(action14);

        action12.selectExecution(drs, action12.getImplementations()[0]);
        drs.addSchedulingDependency(action11, action12);

        action13.selectExecution(drs, action13.getImplementations()[0]);
        drs.addSchedulingDependency(action11, action13);
        drs.addSchedulingDependency(action12, action13);
        action13.addDataPredecessor(action15);

        Thread.sleep(10);
        //debugActions(action1, action2, action3, action4, action5, action6, action7, action8, action9, action10, action11, action12, action13 );
        LinkedList<Gap> gaps = new LinkedList<Gap>();
        drs.seekGaps(System.currentTimeMillis(), gaps);
        FakeAllocatableAction[] actions = new FakeAllocatableAction[]{action1, action2, action3, action4, action5, action6, action7, action8, action9, action10, action11, action12};
        long[][] times = {
            new long[]{0, CORE0}, //1
            new long[]{0, CORE0}, //2
            new long[]{CORE0, 2 * CORE0}, //3
            new long[]{CORE0, 2 * CORE0}, //4
            new long[]{2 * CORE0, 2 * CORE0 + CORE1}, //5
            new long[]{2 * CORE0 + CORE1, 3 * CORE0 + CORE1}, //6
            new long[]{2 * CORE0 + CORE1, 2 * CORE0 + CORE1 + CORE2}, //7
            new long[]{3 * CORE0 + CORE1, 3 * CORE0 + CORE1 + CORE3}, //8
            new long[]{3 * CORE0 + CORE1 + CORE3, 4 * CORE0 + CORE1 + CORE3}, //9
            new long[]{3 * CORE0 + CORE1 + CORE3, 3 * CORE0 + CORE1 + CORE2 + CORE3}, //10
            new long[]{10_000, 10_000 + CORE3}, //11
            new long[]{10_000 + CORE3, 10_000 + CORE3 + CORE0}, //12
            new long[]{12_000, 12_000 + CORE1}, //13
        };
        Verifiers.verifyUpdate(actions, times);

        Gap[] expectedGaps = {
            new Gap(2 * CORE0, 3 * CORE0 + CORE1, action3, new FakeResourceDescription(1), 0),
            new Gap(2 * CORE0 + CORE1 + CORE2, 3 * CORE0 + CORE1, action7, new FakeResourceDescription(1), 0),
            new Gap(3 * CORE0 + CORE1 + CORE3, 10_000, action8, new FakeResourceDescription(1), 0),
            new Gap(3 * CORE0 + CORE1 + CORE2 + CORE3, 10_000, action10, new FakeResourceDescription(1), 0),
            new Gap(4 * CORE0 + CORE1 + CORE3, 10_000, action9, new FakeResourceDescription(2), 0),
            new Gap(10_000 + CORE3 + CORE0, 12_000, action12, new FakeResourceDescription(2), 0),
            new Gap(10_000 + CORE3, 12_000, action11, new FakeResourceDescription(1), 0),
            new Gap(10_000 + CORE3, Long.MAX_VALUE, action11, new FakeResourceDescription(1), 0),
            new Gap(12_000 + CORE1, Long.MAX_VALUE, action13, new FakeResourceDescription(3), 0),};
        Verifiers.verifyGaps(gaps, expectedGaps);
    }
}
