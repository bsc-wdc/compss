package integratedtoolkit.scheduler.fullGraphScheduler;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import integratedtoolkit.scheduler.fullGraphScheduler.utils.Verifiers;
import integratedtoolkit.scheduler.types.fake.FakeAllocatableAction;
import integratedtoolkit.scheduler.types.fake.FakeImplementation;
import integratedtoolkit.scheduler.types.fake.FakeProfile;
import integratedtoolkit.scheduler.types.fake.FakeResourceDescription;
import integratedtoolkit.scheduler.types.fake.FakeWorker;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.util.CoreManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class InitialSchedulingTest {

    private static FullGraphResourceScheduler drs;

    private static long CORE0;
    private static long CORE1;
    private static long CORE2;


    public InitialSchedulingTest() {

    }

    @BeforeClass
    public static void setUpClass() {
        CoreManager.clear();
        CoreManager.resizeStructures(3);

        Implementation<?> impl00 = new FakeImplementation(0, 0, new FakeResourceDescription(2));
        CoreManager.registerImplementations(0, new Implementation[] { impl00 }, new String[] { "fakeSignature00" });
        Implementation<?> impl10 = new FakeImplementation(1, 0, new FakeResourceDescription(3));
        CoreManager.registerImplementations(1, new Implementation[] { impl10 }, new String[] { "fakeSignature10" });
        Implementation<?> impl20 = new FakeImplementation(2, 0, new FakeResourceDescription(1));
        CoreManager.registerImplementations(2, new Implementation[] { impl20 }, new String[] { "fakeSignature20" });

        int maxSlots = 4;
        FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
        FakeWorker fw = new FakeWorker("worker1", frd, maxSlots);
        drs = new FullGraphResourceScheduler(fw);

        drs.profiledExecution(impl00, new FakeProfile(50));
        drs.profiledExecution(impl10, new FakeProfile(50));
        drs.profiledExecution(impl20, new FakeProfile(30));

        CORE0 = drs.getProfile(impl00).getAverageExecutionTime();
        CORE1 = drs.getProfile(impl10).getAverageExecutionTime();
        CORE2 = drs.getProfile(impl20).getAverageExecutionTime();

        // debugConfiguration();
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {
    }

    @Test
    public void testInitialScheduling() throws BlockedActionException, UnassignedActionException {
        drs.clear();
        FakeAllocatableAction action1 = new FakeAllocatableAction(1, 0, CoreManager.getCoreImplementations(0));
        action1.selectExecution(drs, action1.getImplementations()[0]);
        drs.initialSchedule(action1);
        Verifiers.verifyInitialPlan(action1, CORE0);

        FakeAllocatableAction action2 = new FakeAllocatableAction(2, 0, CoreManager.getCoreImplementations(0));
        action2.selectExecution(drs, action2.getImplementations()[0]);
        drs.initialSchedule(action2);
        Verifiers.verifyInitialPlan(action2, CORE0);

        FakeAllocatableAction action3 = new FakeAllocatableAction(3, 0, CoreManager.getCoreImplementations(1));
        action3.selectExecution(drs, action3.getImplementations()[0]);
        drs.initialSchedule(action3);
        Verifiers.verifyInitialPlan(action3, CORE1, action1, action2);

        FakeAllocatableAction action4 = new FakeAllocatableAction(4, 0, CoreManager.getCoreImplementations(0));
        action4.selectExecution(drs, action4.getImplementations()[0]);
        drs.initialSchedule(action4);
        Verifiers.verifyInitialPlan(action4, CORE0, action2, action3);

        FakeAllocatableAction action5 = new FakeAllocatableAction(5, 0, CoreManager.getCoreImplementations(0));
        action5.selectExecution(drs, action5.getImplementations()[0]);
        drs.initialSchedule(action5);
        Verifiers.verifyInitialPlan(action5, CORE0, action3);

        FakeAllocatableAction action6 = new FakeAllocatableAction(6, 0, CoreManager.getCoreImplementations(1));
        action6.selectExecution(drs, action6.getImplementations()[0]);
        drs.initialSchedule(action6);
        Verifiers.verifyInitialPlan(action6, CORE1, action4, action5);

        FakeAllocatableAction action7 = new FakeAllocatableAction(7, 0, CoreManager.getCoreImplementations(2));
        action7.selectExecution(drs, action7.getImplementations()[0]);
        drs.initialSchedule(action7);
        Verifiers.verifyInitialPlan(action7, CORE2, action5);

        FakeAllocatableAction action8 = new FakeAllocatableAction(8, 0, CoreManager.getCoreImplementations(2));
        action8.selectExecution(drs, action8.getImplementations()[0]);
        drs.initialSchedule(action8);
        Verifiers.verifyInitialPlan(action8, CORE2, action7);

        FakeAllocatableAction action9 = new FakeAllocatableAction(9, 0, CoreManager.getCoreImplementations(1));
        action9.selectExecution(drs, action9.getImplementations()[0]);
        drs.initialSchedule(action9);
        Verifiers.verifyInitialPlan(action9, CORE1, action6);

        FakeAllocatableAction action10 = new FakeAllocatableAction(10, 0, CoreManager.getCoreImplementations(0));
        action10.selectExecution(drs, action10.getImplementations()[0]);
        drs.initialSchedule(action10);
        Verifiers.verifyInitialPlan(action10, CORE0, action8, action9);

        FakeAllocatableAction action11 = new FakeAllocatableAction(11, 0, CoreManager.getCoreImplementations(0));
        action11.selectExecution(drs, action11.getImplementations()[0]);
        drs.initialSchedule(action11);
        Verifiers.verifyInitialPlan(action11, CORE0, action9);

    }

}
