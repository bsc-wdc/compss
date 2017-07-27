package es.bsc.compss.scheduler.fullGraphScheduler;

import es.bsc.es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.es.bsc.compss.scheduler.fullGraphScheduler.utils.Verifiers;
import es.bsc.es.bsc.compss.scheduler.types.fake.FakeActionOrchestrator;
import es.bsc.es.bsc.compss.scheduler.types.fake.FakeAllocatableAction;
import es.bsc.es.bsc.compss.scheduler.types.fake.FakeImplementation;
import es.bsc.es.bsc.compss.scheduler.types.fake.FakeProfile;
import es.bsc.es.bsc.compss.scheduler.types.fake.FakeResourceDescription;
import es.bsc.es.bsc.compss.scheduler.types.fake.FakeResourceScheduler;
import es.bsc.es.bsc.compss.scheduler.types.fake.FakeWorker;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.util.CoreManager;

import java.util.LinkedList;
import java.util.List;

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
        CoreManager.registerNewCoreElement("fakeSignature00");
        CoreManager.registerNewCoreElement("fakeSignature10");
        CoreManager.registerNewCoreElement("fakeSignature20");

        FakeImplementation impl00 = new FakeImplementation(0, 0, new FakeResourceDescription(2));
        List<Implementation<?>> impls0 = new LinkedList<>();
        impls0.add(impl00);
        List<String> signatures0 = new LinkedList<>();
        signatures0.add("fakeSignature00");
        CoreManager.registerNewImplementations(0, impls0, signatures0);

        FakeImplementation impl10 = new FakeImplementation(1, 0, new FakeResourceDescription(3));
        List<Implementation<?>> impls1 = new LinkedList<>();
        impls1.add(impl10);
        List<String> signatures1 = new LinkedList<>();
        signatures1.add("fakeSignature10");
        CoreManager.registerNewImplementations(1, impls1, signatures1);

        FakeImplementation impl20 = new FakeImplementation(2, 0, new FakeResourceDescription(1));
        List<Implementation<?>> impls2 = new LinkedList<>();
        impls2.add(impl20);
        List<String> signatures2 = new LinkedList<>();
        signatures2.add("fakeSignature20");
        CoreManager.registerNewImplementations(2, impls2, signatures2);

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
        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, CoreManager.getCoreImplementations(0));
        action1.selectExecution(drs, (FakeImplementation) action1.getImplementations()[0]);
        drs.scheduleAction(action1);
        Verifiers.verifyInitialPlan(action1, CORE0);

        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, CoreManager.getCoreImplementations(0));
        action2.selectExecution(drs, (FakeImplementation) action2.getImplementations()[0]);
        drs.scheduleAction(action2);
        Verifiers.verifyInitialPlan(action2, CORE0);

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 0, CoreManager.getCoreImplementations(1));
        action3.selectExecution(drs, (FakeImplementation) action3.getImplementations()[0]);
        drs.scheduleAction(action3);
        Verifiers.verifyInitialPlan(action3, CORE1, action1, action2);

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, CoreManager.getCoreImplementations(0));
        action4.selectExecution(drs, (FakeImplementation) action4.getImplementations()[0]);
        drs.scheduleAction(action4);
        Verifiers.verifyInitialPlan(action4, CORE0, action2, action3);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 0, CoreManager.getCoreImplementations(0));
        action5.selectExecution(drs, (FakeImplementation) action5.getImplementations()[0]);
        drs.scheduleAction(action5);
        Verifiers.verifyInitialPlan(action5, CORE0, action3);

        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 0, CoreManager.getCoreImplementations(1));
        action6.selectExecution(drs, (FakeImplementation) action6.getImplementations()[0]);
        drs.scheduleAction(action6);
        Verifiers.verifyInitialPlan(action6, CORE1, action4, action5);

        FakeAllocatableAction action7 = new FakeAllocatableAction(fao, 7, 0, CoreManager.getCoreImplementations(2));
        action7.selectExecution(drs, (FakeImplementation) action7.getImplementations()[0]);
        drs.scheduleAction(action7);
        Verifiers.verifyInitialPlan(action7, CORE2, action5);

        FakeAllocatableAction action8 = new FakeAllocatableAction(fao, 8, 0, CoreManager.getCoreImplementations(2));
        action8.selectExecution(drs, (FakeImplementation) action8.getImplementations()[0]);
        drs.scheduleAction(action8);
        Verifiers.verifyInitialPlan(action8, CORE2, action7);

        FakeAllocatableAction action9 = new FakeAllocatableAction(fao, 9, 0, CoreManager.getCoreImplementations(1));
        action9.selectExecution(drs, (FakeImplementation) action9.getImplementations()[0]);
        drs.scheduleAction(action9);
        Verifiers.verifyInitialPlan(action9, CORE1, action6);

        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, CoreManager.getCoreImplementations(0));
        action10.selectExecution(drs, (FakeImplementation) action10.getImplementations()[0]);
        drs.scheduleAction(action10);
        Verifiers.verifyInitialPlan(action10, CORE0, action8, action9);

        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, CoreManager.getCoreImplementations(0));
        action11.selectExecution(drs, (FakeImplementation) action11.getImplementations()[0]);
        drs.scheduleAction(action11);
        Verifiers.verifyInitialPlan(action11, CORE0, action9);
    }

}
