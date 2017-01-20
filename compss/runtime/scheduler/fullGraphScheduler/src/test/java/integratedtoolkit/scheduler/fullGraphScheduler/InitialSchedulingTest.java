package integratedtoolkit.scheduler.fullGraphScheduler;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.fullGraphScheduler.utils.Verifiers;
import integratedtoolkit.scheduler.types.fake.FakeActionOrchestrator;
import integratedtoolkit.scheduler.types.fake.FakeAllocatableAction;
import integratedtoolkit.scheduler.types.fake.FakeImplementation;
import integratedtoolkit.scheduler.types.fake.FakeProfile;
import integratedtoolkit.scheduler.types.fake.FakeResourceDescription;
import integratedtoolkit.scheduler.types.fake.FakeResourceScheduler;
import integratedtoolkit.scheduler.types.fake.FakeWorker;
import integratedtoolkit.util.CoreManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class InitialSchedulingTest {

    private static FullGraphScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> ds;
    private static FakeActionOrchestrator fao;
    private static FakeResourceScheduler drs;

    private static long CORE0;
    private static long CORE1;
    private static long CORE2;


    public InitialSchedulingTest() {
        ds = new FullGraphScheduler<FakeProfile, FakeResourceDescription, FakeImplementation>();
        fao = new FakeActionOrchestrator(ds);
        ds.setOrchestrator(fao);
    }

    @BeforeClass
    public static void setUpClass() {
        CoreManager.clear();
        CoreManager.resizeStructures(3);

        FakeImplementation impl00 = new FakeImplementation(0, 0, new FakeResourceDescription(2));
        CoreManager.registerImplementations(0, new FakeImplementation[] { impl00 }, new String[] { "fakeSignature00" });
        FakeImplementation impl10 = new FakeImplementation(1, 0, new FakeResourceDescription(3));
        CoreManager.registerImplementations(1, new FakeImplementation[] { impl10 }, new String[] { "fakeSignature10" });
        FakeImplementation impl20 = new FakeImplementation(2, 0, new FakeResourceDescription(1));
        CoreManager.registerImplementations(2, new FakeImplementation[] { impl20 }, new String[] { "fakeSignature20" });

        int maxSlots = 4;
        FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
        FakeWorker fw = new FakeWorker("worker1", frd, maxSlots);
        drs = new FakeResourceScheduler(fw, fao, 0);

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

    @SuppressWarnings("unchecked")
    @Test
    public void testInitialScheduling() throws BlockedActionException, UnassignedActionException {
        drs.clear();
        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, (FakeImplementation[]) CoreManager.getCoreImplementations(0));
        action1.selectExecution(drs, (FakeImplementation) action1.getImplementations()[0]);
        drs.scheduleAction(action1);
        Verifiers.verifyInitialPlan(action1, CORE0);

        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, (FakeImplementation[]) CoreManager.getCoreImplementations(0));
        action2.selectExecution(drs, (FakeImplementation) action2.getImplementations()[0]);
        drs.scheduleAction(action2);
        Verifiers.verifyInitialPlan(action2, CORE0);

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 0, (FakeImplementation[]) CoreManager.getCoreImplementations(1));
        action3.selectExecution(drs, (FakeImplementation) action3.getImplementations()[0]);
        drs.scheduleAction(action3);
        Verifiers.verifyInitialPlan(action3, CORE1, action1, action2);

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, (FakeImplementation[]) CoreManager.getCoreImplementations(0));
        action4.selectExecution(drs, (FakeImplementation) action4.getImplementations()[0]);
        drs.scheduleAction(action4);
        Verifiers.verifyInitialPlan(action4, CORE0, action2, action3);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 0, (FakeImplementation[]) CoreManager.getCoreImplementations(0));
        action5.selectExecution(drs, (FakeImplementation) action5.getImplementations()[0]);
        drs.scheduleAction(action5);
        Verifiers.verifyInitialPlan(action5, CORE0, action3);

        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 0, (FakeImplementation[]) CoreManager.getCoreImplementations(1));
        action6.selectExecution(drs, (FakeImplementation) action6.getImplementations()[0]);
        drs.scheduleAction(action6);
        Verifiers.verifyInitialPlan(action6, CORE1, action4, action5);

        FakeAllocatableAction action7 = new FakeAllocatableAction(fao, 7, 0, (FakeImplementation[]) CoreManager.getCoreImplementations(2));
        action7.selectExecution(drs, (FakeImplementation) action7.getImplementations()[0]);
        drs.scheduleAction(action7);
        Verifiers.verifyInitialPlan(action7, CORE2, action5);

        FakeAllocatableAction action8 = new FakeAllocatableAction(fao, 8, 0, (FakeImplementation[]) CoreManager.getCoreImplementations(2));
        action8.selectExecution(drs, (FakeImplementation) action8.getImplementations()[0]);
        drs.scheduleAction(action8);
        Verifiers.verifyInitialPlan(action8, CORE2, action7);

        FakeAllocatableAction action9 = new FakeAllocatableAction(fao, 9, 0, (FakeImplementation[]) CoreManager.getCoreImplementations(1));
        action9.selectExecution(drs, (FakeImplementation) action9.getImplementations()[0]);
        drs.scheduleAction(action9);
        Verifiers.verifyInitialPlan(action9, CORE1, action6);

        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0,
                (FakeImplementation[]) CoreManager.getCoreImplementations(0));
        action10.selectExecution(drs, (FakeImplementation) action10.getImplementations()[0]);
        drs.scheduleAction(action10);
        Verifiers.verifyInitialPlan(action10, CORE0, action8, action9);

        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0,
                (FakeImplementation[]) CoreManager.getCoreImplementations(0));
        action11.selectExecution(drs, (FakeImplementation) action11.getImplementations()[0]);
        drs.scheduleAction(action11);
        Verifiers.verifyInitialPlan(action11, CORE0, action9);
    }

}
