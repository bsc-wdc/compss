package integratedtoolkit.scheduler.fullGraphScheduler;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphScheduler;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import integratedtoolkit.scheduler.fullGraphScheduler.ScheduleOptimizer;
import integratedtoolkit.scheduler.fullGraphScheduler.utils.Verifiers;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Gap;
import integratedtoolkit.scheduler.types.OptimizationWorker;
import integratedtoolkit.scheduler.types.PriorityActionSet;
import integratedtoolkit.scheduler.types.fake.FakeAllocatableAction;
import integratedtoolkit.scheduler.types.fake.FakeImplementation;
import integratedtoolkit.scheduler.types.fake.FakeProfile;
import integratedtoolkit.scheduler.types.fake.FakeResourceDescription;
import integratedtoolkit.scheduler.types.fake.FakeWorker;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ResourceScheduler;
import java.util.HashMap;

import java.util.LinkedList;
import java.util.PriorityQueue;

import org.junit.BeforeClass;
import org.junit.Test;


public class OptimizationTest {

    private static FullGraphScheduler ds;
    private static FullGraphResourceScheduler drs;
    private static FullGraphResourceScheduler secondDRS;


    public OptimizationTest() {

    }

    @BeforeClass
    public static void setUpClass() {
        CoreManager.clear();
        CoreManager.resizeStructures(7);

        ds = new FullGraphScheduler();

        Implementation<?> impl00 = new FakeImplementation(0, 0, new FakeResourceDescription(2));
        CoreManager.registerImplementations(0, new Implementation[] { impl00 }, new String[] { "fakeSignature00" });
        Implementation<?> impl10 = new FakeImplementation(1, 0, new FakeResourceDescription(3));
        CoreManager.registerImplementations(1, new Implementation[] { impl10 }, new String[] { "fakeSignature10" });
        Implementation<?> impl20 = new FakeImplementation(2, 0, new FakeResourceDescription(1));
        CoreManager.registerImplementations(2, new Implementation[] { impl20 }, new String[] { "fakeSignature20" });
        Implementation<?> impl30 = new FakeImplementation(3, 0, new FakeResourceDescription(4));
        CoreManager.registerImplementations(3, new Implementation[] { impl30 }, new String[] { "fakeSignature30" });
        Implementation<?> impl40 = new FakeImplementation(4, 0, new FakeResourceDescription(2));
        CoreManager.registerImplementations(4, new Implementation[] { impl40 }, new String[] { "fakeSignature40" });
        Implementation<?> impl50 = new FakeImplementation(5, 0, new FakeResourceDescription(1));
        CoreManager.registerImplementations(5, new Implementation[] { impl50 }, new String[] { "fakeSignature50" });
        Implementation<?> impl60 = new FakeImplementation(6, 0, new FakeResourceDescription(3));
        CoreManager.registerImplementations(6, new Implementation[] { impl60 }, new String[] { "fakeSignature60" });

        int maxSlots = 4;
        FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
        FakeWorker fw = new FakeWorker("worker1", frd, maxSlots);
        drs = new FullGraphResourceScheduler(fw);

        FakeResourceDescription frd2 = new FakeResourceDescription(maxSlots);
        FakeWorker fw2 = new FakeWorker("worker2", frd2, maxSlots);
        secondDRS = new FullGraphResourceScheduler(fw2);

        drs.profiledExecution(impl00, new FakeProfile(50));
        drs.profiledExecution(impl10, new FakeProfile(50));
        drs.profiledExecution(impl20, new FakeProfile(30));
        drs.profiledExecution(impl30, new FakeProfile(50));
        drs.profiledExecution(impl40, new FakeProfile(20));
        drs.profiledExecution(impl50, new FakeProfile(10));
        drs.profiledExecution(impl60, new FakeProfile(30));

        secondDRS.profiledExecution(impl00, new FakeProfile(50));
        secondDRS.profiledExecution(impl10, new FakeProfile(50));
        secondDRS.profiledExecution(impl20, new FakeProfile(30));
        // Faster than drs
        secondDRS.profiledExecution(impl30, new FakeProfile(30));
        secondDRS.profiledExecution(impl40, new FakeProfile(15));
        secondDRS.profiledExecution(impl50, new FakeProfile(10));
        secondDRS.profiledExecution(impl60, new FakeProfile(15));

    }


    private class FakeDefaultResourceScheduler extends FullGraphResourceScheduler {

        long fakeLastGapStart;


        public FakeDefaultResourceScheduler(Worker w, long fakeLastGapStart) {
            super(w);
            this.fakeLastGapStart = fakeLastGapStart;
        }

        @Override
        public long getLastGapExpectedStart() {
            return fakeLastGapStart;
        }

    }


    // @Test
    public void testDonorsAndReceivers() {

        long[] expectedEndTimes = new long[] { 35000, 20000, 15000, 50000, 40000, 1000 };
        OptimizationWorker[] optimizedWorkers = new OptimizationWorker[expectedEndTimes.length];
        for (int idx = 0; idx < expectedEndTimes.length; idx++) {
            int maxSlots = 1;
            FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
            FakeWorker fw = new FakeWorker("worker" + idx, frd, maxSlots);
            optimizedWorkers[idx] = new OptimizationWorker(new FakeDefaultResourceScheduler(fw, expectedEndTimes[idx]));
        }

        LinkedList<OptimizationWorker> receivers = new LinkedList<OptimizationWorker>();
        OptimizationWorker donor = ScheduleOptimizer.determineDonorAndReceivers(optimizedWorkers, receivers);

        LinkedList<OptimizationWorker> donors = new LinkedList<OptimizationWorker>();
        donors.offer(donor);
        LinkedList<String> donorsNames = new LinkedList<String>();
        donorsNames.add("worker3");

        LinkedList<String> receiversNames = new LinkedList<String>();
        receiversNames.add("worker5");
        receiversNames.add("worker2");
        receiversNames.add("worker1");
        receiversNames.add("worker0");
        receiversNames.add("worker4");

        Verifiers.verifyWorkersPriority(donors, donorsNames);
        Verifiers.verifyWorkersPriority(receivers, receiversNames);
    }

    // @Test
    public void globalOptimization() {
        ScheduleOptimizer so = new ScheduleOptimizer(ds);
        long updateId = System.currentTimeMillis();

        ResourceScheduler<?, ?>[] workers = new ResourceScheduler[2];
        drs.clear();
        secondDRS.clear();
        workers[0] = drs;

        FakeAllocatableAction action1 = new FakeAllocatableAction(1, 0, CoreManager.getCoreImplementations(4));
        action1.selectExecution(drs, action1.getImplementations()[0]);
        drs.initialSchedule(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(2, 0, CoreManager.getCoreImplementations(4));
        action2.selectExecution(drs, action2.getImplementations()[0]);
        drs.initialSchedule(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(3, 1, CoreManager.getCoreImplementations(4));
        action3.selectExecution(drs, action3.getImplementations()[0]);
        drs.initialSchedule(action3);

        FakeAllocatableAction action4 = new FakeAllocatableAction(4, 0, CoreManager.getCoreImplementations(5));
        action4.selectExecution(drs, action4.getImplementations()[0]);
        drs.initialSchedule(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(5, 1, CoreManager.getCoreImplementations(4));
        action5.selectExecution(drs, action5.getImplementations()[0]);
        drs.initialSchedule(action5);

        FakeAllocatableAction action6 = new FakeAllocatableAction(6, 1, CoreManager.getCoreImplementations(6));
        action6.selectExecution(drs, action6.getImplementations()[0]);
        drs.initialSchedule(action6);

        workers[1] = secondDRS;

        so.globalOptimization(updateId, workers);

    }

    // @Test
    public void testScan() {
        FakeAllocatableAction external10 = new FakeAllocatableAction(13, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).setExpectedEnd(10);
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).scheduled();
        external10.selectExecution(secondDRS, external10.getImplementations()[0]);

        FakeAllocatableAction external20 = new FakeAllocatableAction(14, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).setExpectedEnd(20);
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).scheduled();
        external20.selectExecution(secondDRS, external20.getImplementations()[0]);

        FakeAllocatableAction external90 = new FakeAllocatableAction(15, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).setExpectedEnd(90);
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).scheduled();
        external90.selectExecution(secondDRS, external90.getImplementations()[0]);

        drs.clear();
        secondDRS.clear();

        FakeAllocatableAction action1 = new FakeAllocatableAction(1, 0, CoreManager.getCoreImplementations(4));
        action1.selectExecution(drs, action1.getImplementations()[0]);
        drs.initialSchedule(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(2, 0, CoreManager.getCoreImplementations(4));
        action2.selectExecution(drs, action2.getImplementations()[0]);
        drs.initialSchedule(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(3, 1, CoreManager.getCoreImplementations(4));
        action3.addDataPredecessor(external90);
        action3.selectExecution(drs, action3.getImplementations()[0]);
        drs.initialSchedule(action3);

        FakeAllocatableAction action4 = new FakeAllocatableAction(4, 0, CoreManager.getCoreImplementations(5));
        action4.selectExecution(drs, action4.getImplementations()[0]);
        drs.initialSchedule(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(5, 1, CoreManager.getCoreImplementations(4));
        action5.selectExecution(drs, action5.getImplementations()[0]);
        drs.initialSchedule(action5);

        FakeAllocatableAction action6 = new FakeAllocatableAction(6, 1, CoreManager.getCoreImplementations(6));
        action6.selectExecution(drs, action6.getImplementations()[0]);
        drs.initialSchedule(action6);

        FakeAllocatableAction action7 = new FakeAllocatableAction(7, 0, CoreManager.getCoreImplementations(5));
        action7.addDataPredecessor(external10);
        action7.selectExecution(drs, action7.getImplementations()[0]);
        drs.initialSchedule(action7);

        FakeAllocatableAction action8 = new FakeAllocatableAction(8, 0, CoreManager.getCoreImplementations(5));
        action8.addDataPredecessor(external20);
        action8.selectExecution(drs, action8.getImplementations()[0]);
        drs.initialSchedule(action8);

        FakeAllocatableAction action9 = new FakeAllocatableAction(9, 0, CoreManager.getCoreImplementations(4));
        action9.addDataPredecessor(external90);
        action9.selectExecution(drs, action9.getImplementations()[0]);
        drs.initialSchedule(action9);

        FakeAllocatableAction action10 = new FakeAllocatableAction(10, 0, CoreManager.getCoreImplementations(4));
        action10.addDataPredecessor(action5);
        action10.selectExecution(drs, action10.getImplementations()[0]);
        drs.initialSchedule(action10);

        FakeAllocatableAction action11 = new FakeAllocatableAction(11, 0, CoreManager.getCoreImplementations(4));
        action11.addDataPredecessor(action6);
        action11.selectExecution(drs, action11.getImplementations()[0]);
        drs.initialSchedule(action11);

        FakeAllocatableAction action12 = new FakeAllocatableAction(12, 0, CoreManager.getCoreImplementations(4));
        action12.addDataPredecessor(action5);
        action12.addDataPredecessor(action6);
        action12.selectExecution(drs, action12.getImplementations()[0]);
        drs.initialSchedule(action12);

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction> readyActions = new PriorityQueue<AllocatableAction>(1, drs.getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet selectableActions = new PriorityActionSet(ScheduleOptimizer.getSelectionComparator());
        LinkedList<AllocatableAction> running = drs.scanActions(readyActions, selectableActions);

        HashMap<AllocatableAction, Long> expectedReady = new HashMap();
        expectedReady.put(action7, 10l);
        expectedReady.put(action8, 20l);
        expectedReady.put(action9, 90l);
        expectedReady.put(action3, 90l);
        Verifiers.verifyReadyActions(new PriorityQueue(readyActions), expectedReady);

        AllocatableAction[] expectedSelectable = new AllocatableAction[] { action5, action6, action4 };
        Verifiers.verifyPriorityActions(new PriorityActionSet(selectableActions), expectedSelectable);
    }

    public void printAction(AllocatableAction action) {
        System.out.println(action + " Core Element " + action.getCoreId() + " Implementation "
                + action.getAssignedImplementation().getImplementationId() + " (" + action.getAssignedImplementation() + ")");
        FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) action.getSchedulingInfo();
        System.out.println("\t Optimization:" + dsi.isOnOptimization());
        System.out.println("\t StartTime:" + dsi.getExpectedStart());
        System.out.println("\t EndTime:" + dsi.getExpectedEnd());
        System.out.println("\t Locks:" + dsi.getLockCount());
        System.out.println("\t Predecessors:" + dsi.getPredecessors());
        System.out.println("\t Successors:" + dsi.getSuccessors());
        System.out.println("\t Optimization Successors:" + dsi.getOptimizingSuccessors());

    }

    // @Test
    public void testPendingActions() {
        LinkedList<AllocatableAction> pendingActions = new LinkedList();

        FakeAllocatableAction external10 = new FakeAllocatableAction(13, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).setExpectedEnd(10);
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).scheduled();
        external10.selectExecution(secondDRS, external10.getImplementations()[0]);

        FakeAllocatableAction external20 = new FakeAllocatableAction(14, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).setExpectedEnd(20);
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).scheduled();
        external20.selectExecution(secondDRS, external20.getImplementations()[0]);

        FakeAllocatableAction external90 = new FakeAllocatableAction(15, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).setExpectedEnd(90);
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).scheduled();
        external90.selectExecution(secondDRS, external90.getImplementations()[0]);

        drs.clear();
        secondDRS.clear();

        FakeAllocatableAction action1 = new FakeAllocatableAction(1, 0, CoreManager.getCoreImplementations(4));
        action1.selectExecution(drs, action1.getImplementations()[0]);
        drs.initialSchedule(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(2, 0, CoreManager.getCoreImplementations(4));
        action2.selectExecution(drs, action2.getImplementations()[0]);
        drs.initialSchedule(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(3, 1, CoreManager.getCoreImplementations(4));
        action3.addDataPredecessor(external90);
        action3.selectExecution(drs, action3.getImplementations()[0]);
        drs.initialSchedule(action3);

        FakeAllocatableAction action4 = new FakeAllocatableAction(4, 0, CoreManager.getCoreImplementations(5));
        action4.selectExecution(drs, action4.getImplementations()[0]);
        drs.initialSchedule(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(5, 1, CoreManager.getCoreImplementations(4));
        action5.selectExecution(drs, action5.getImplementations()[0]);
        drs.initialSchedule(action5);

        FakeAllocatableAction action6 = new FakeAllocatableAction(6, 1, CoreManager.getCoreImplementations(6));
        action6.selectExecution(drs, action6.getImplementations()[0]);
        drs.initialSchedule(action6);

        FakeAllocatableAction action7 = new FakeAllocatableAction(7, 0, CoreManager.getCoreImplementations(5));
        action7.addDataPredecessor(external10);
        action7.selectExecution(drs, action7.getImplementations()[0]);
        pendingActions.add(action7);

        FakeAllocatableAction action8 = new FakeAllocatableAction(8, 0, CoreManager.getCoreImplementations(5));
        action8.addDataPredecessor(external20);
        action8.selectExecution(drs, action8.getImplementations()[0]);
        pendingActions.add(action8);

        FakeAllocatableAction action9 = new FakeAllocatableAction(9, 0, CoreManager.getCoreImplementations(4));
        action9.addDataPredecessor(external90);
        action9.selectExecution(drs, action9.getImplementations()[0]);
        pendingActions.add(action9);

        FakeAllocatableAction action10 = new FakeAllocatableAction(10, 0, CoreManager.getCoreImplementations(4));
        action10.addDataPredecessor(action5);
        action10.selectExecution(drs, action10.getImplementations()[0]);
        pendingActions.add(action10);

        FakeAllocatableAction action11 = new FakeAllocatableAction(11, 0, CoreManager.getCoreImplementations(4));
        action11.addDataPredecessor(action6);
        action11.selectExecution(drs, action11.getImplementations()[0]);
        pendingActions.add(action11);

        FakeAllocatableAction action12 = new FakeAllocatableAction(12, 0, CoreManager.getCoreImplementations(4));
        action12.addDataPredecessor(action5);
        action12.addDataPredecessor(action6);
        action12.selectExecution(drs, action12.getImplementations()[0]);
        pendingActions.add(action12);

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction> readyActions = new PriorityQueue<AllocatableAction>(1, drs.getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet selectableActions = new PriorityActionSet(ScheduleOptimizer.getSelectionComparator());
        drs.scanActions(readyActions, selectableActions);
        drs.classifyPendingSchedulings(pendingActions, readyActions, selectableActions, new LinkedList<AllocatableAction>());

        HashMap<AllocatableAction, Long> expectedReady = new HashMap();
        expectedReady.put(action7, 10l);
        expectedReady.put(action8, 20l);
        expectedReady.put(action9, 90l);
        expectedReady.put(action3, 90l);
        Verifiers.verifyReadyActions(new PriorityQueue(readyActions), expectedReady);

        AllocatableAction[] expectedSelectable = new AllocatableAction[] { action5, action6, action4 };
        Verifiers.verifyPriorityActions(new PriorityActionSet(selectableActions), expectedSelectable);
    }

    @Test
    public void testLocalOptimization() {

        drs.clear();
        secondDRS.clear();

        FakeAllocatableAction external10 = new FakeAllocatableAction(13, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).setExpectedEnd(10);
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).scheduled();
        external10.selectExecution(secondDRS, external10.getImplementations()[0]);

        FakeAllocatableAction external20 = new FakeAllocatableAction(14, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).setExpectedEnd(20);
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).scheduled();
        external20.selectExecution(secondDRS, external20.getImplementations()[0]);

        FakeAllocatableAction external90 = new FakeAllocatableAction(15, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).setExpectedEnd(90);
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).scheduled();
        external90.selectExecution(secondDRS, external90.getImplementations()[0]);

        FakeAllocatableAction action1 = new FakeAllocatableAction(1, 0, CoreManager.getCoreImplementations(4));
        action1.selectExecution(drs, action1.getImplementations()[0]);
        drs.initialSchedule(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(2, 0, CoreManager.getCoreImplementations(4));
        action2.selectExecution(drs, action2.getImplementations()[0]);
        drs.initialSchedule(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(3, 1, CoreManager.getCoreImplementations(4));
        action3.addDataPredecessor(external90);
        action3.selectExecution(drs, action3.getImplementations()[0]);
        drs.initialSchedule(action3);

        FakeAllocatableAction action4 = new FakeAllocatableAction(4, 0, CoreManager.getCoreImplementations(5));
        action4.selectExecution(drs, action4.getImplementations()[0]);
        drs.initialSchedule(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(5, 1, CoreManager.getCoreImplementations(4));
        action5.selectExecution(drs, action5.getImplementations()[0]);
        drs.initialSchedule(action5);

        FakeAllocatableAction action6 = new FakeAllocatableAction(6, 1, CoreManager.getCoreImplementations(6));
        action6.selectExecution(drs, action6.getImplementations()[0]);
        drs.initialSchedule(action6);

        FakeAllocatableAction action7 = new FakeAllocatableAction(7, 0, CoreManager.getCoreImplementations(5));
        action7.addDataPredecessor(external10);
        action7.selectExecution(drs, action7.getImplementations()[0]);
        drs.initialSchedule(action7);

        FakeAllocatableAction action8 = new FakeAllocatableAction(8, 0, CoreManager.getCoreImplementations(5));
        action8.addDataPredecessor(external20);
        action8.selectExecution(drs, action8.getImplementations()[0]);
        drs.initialSchedule(action8);

        FakeAllocatableAction action9 = new FakeAllocatableAction(9, 0, CoreManager.getCoreImplementations(4));
        action9.addDataPredecessor(external90);
        action9.selectExecution(drs, action9.getImplementations()[0]);
        drs.initialSchedule(action9);

        FakeAllocatableAction action10 = new FakeAllocatableAction(10, 0, CoreManager.getCoreImplementations(4));
        action10.addDataPredecessor(action5);
        action10.selectExecution(drs, action10.getImplementations()[0]);
        drs.initialSchedule(action10);

        FakeAllocatableAction action11 = new FakeAllocatableAction(11, 0, CoreManager.getCoreImplementations(4));
        action11.addDataPredecessor(action6);
        action11.selectExecution(drs, action11.getImplementations()[0]);
        drs.initialSchedule(action11);

        FakeAllocatableAction action12 = new FakeAllocatableAction(12, 0, CoreManager.getCoreImplementations(4));
        action12.addDataPredecessor(action5);
        action12.addDataPredecessor(action6);
        action12.selectExecution(drs, action12.getImplementations()[0]);
        drs.initialSchedule(action12);

        // Simulate Scan results
        LinkedList<AllocatableAction> runningActions = new LinkedList();
        PriorityQueue<AllocatableAction> readyActions = new PriorityQueue<AllocatableAction>(1, drs.getReadyComparator());
        PriorityActionSet selectableActions = new PriorityActionSet(ScheduleOptimizer.getSelectionComparator());

        long updateId = System.currentTimeMillis();

        ((FullGraphSchedulingInformation) action1.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action1.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action1.getSchedulingInfo()).setExpectedStart(0);
        ((FullGraphSchedulingInformation) action1.getSchedulingInfo()).lock();
        runningActions.add(action1);

        ((FullGraphSchedulingInformation) action2.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action2.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action2.getSchedulingInfo()).setExpectedStart(0);
        ((FullGraphSchedulingInformation) action2.getSchedulingInfo()).lock();
        runningActions.add(action2);

        ((FullGraphSchedulingInformation) action3.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action3.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action3.getSchedulingInfo()).setExpectedStart(90);
        readyActions.offer(action3);

        ((FullGraphSchedulingInformation) action4.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action4.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action4.getSchedulingInfo()).setExpectedStart(0);
        selectableActions.offer(action4);

        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).setExpectedStart(0);
        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).optimizingSuccessor(action12);
        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).optimizingSuccessor(action10);
        selectableActions.offer(action5);

        ((FullGraphSchedulingInformation) action6.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action6.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action6.getSchedulingInfo()).setExpectedStart(0);
        ((FullGraphSchedulingInformation) action6.getSchedulingInfo()).optimizingSuccessor(action12);
        ((FullGraphSchedulingInformation) action6.getSchedulingInfo()).optimizingSuccessor(action11);
        selectableActions.offer(action6);

        ((FullGraphSchedulingInformation) action7.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action7.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action7.getSchedulingInfo()).setExpectedStart(10);
        readyActions.offer(action7);

        ((FullGraphSchedulingInformation) action8.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action8.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action8.getSchedulingInfo()).setExpectedStart(20);
        readyActions.offer(action8);

        ((FullGraphSchedulingInformation) action9.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action9.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action9.getSchedulingInfo()).setExpectedStart(90);
        readyActions.offer(action9);

        ((FullGraphSchedulingInformation) action10.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action10.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action10.getSchedulingInfo()).setExpectedStart(0);

        ((FullGraphSchedulingInformation) action11.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action11.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action11.getSchedulingInfo()).setExpectedStart(0);

        ((FullGraphSchedulingInformation) action12.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action12.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action12.getSchedulingInfo()).setExpectedStart(0);

        PriorityQueue<AllocatableAction> donationActions = new PriorityQueue(1, ScheduleOptimizer.getDonationComparator());

        drs.rescheduleTasks(updateId, readyActions, selectableActions, runningActions, donationActions);

    }

    // @Test
    public void testNoDataDependencies()
            throws BlockedActionException, UnassignedActionException, InvalidSchedulingException, InterruptedException {

        //Build graph
        /*
         * 1 --> 3 --> 5 -->6 --> 8 -->9  ----->11 -->12 --> 13
         * 2 --> 4 ┘     └->7 ┘     └->10 ---|     └-----┘
         *                                   |           |
         * -------------------------------------------------------
         *                                 14┘         15┘
         */

        drs.clear();

        FakeAllocatableAction action1 = new FakeAllocatableAction(1, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action2 = new FakeAllocatableAction(2, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action3 = new FakeAllocatableAction(3, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action4 = new FakeAllocatableAction(4, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action5 = new FakeAllocatableAction(5, 0, CoreManager.getCoreImplementations(1));
        FakeAllocatableAction action6 = new FakeAllocatableAction(6, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action7 = new FakeAllocatableAction(7, 0, CoreManager.getCoreImplementations(2));
        FakeAllocatableAction action8 = new FakeAllocatableAction(8, 0, CoreManager.getCoreImplementations(3));
        FakeAllocatableAction action9 = new FakeAllocatableAction(9, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action10 = new FakeAllocatableAction(10, 0, CoreManager.getCoreImplementations(2));
        FakeAllocatableAction action11 = new FakeAllocatableAction(11, 0, CoreManager.getCoreImplementations(3));
        FakeAllocatableAction action12 = new FakeAllocatableAction(12, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action13 = new FakeAllocatableAction(13, 0, CoreManager.getCoreImplementations(1));

        FakeAllocatableAction action14 = new FakeAllocatableAction(14, 0, CoreManager.getCoreImplementations(0));
        action14.selectExecution(secondDRS, action14.getImplementations()[0]);
        FullGraphSchedulingInformation dsi14 = (FullGraphSchedulingInformation) action14.getSchedulingInfo();
        dsi14.setExpectedEnd(10_000);

        FakeAllocatableAction action15 = new FakeAllocatableAction(15, 0, CoreManager.getCoreImplementations(0));
        action15.selectExecution(secondDRS, action15.getImplementations()[0]);
        FullGraphSchedulingInformation dsi15 = (FullGraphSchedulingInformation) action15.getSchedulingInfo();
        dsi15.setExpectedEnd(12_000);

        action1.selectExecution(drs, action1.getImplementations()[0]);
        action1.tryToLaunch();

        action2.selectExecution(drs, action2.getImplementations()[0]);
        action2.tryToLaunch();

        action3.selectExecution(drs, action3.getImplementations()[0]);
        addSchedulingDependency(action1, action3);

        action4.selectExecution(drs, action4.getImplementations()[0]);
        addSchedulingDependency(action2, action4);

        action5.selectExecution(drs, action5.getImplementations()[0]);
        action5.addDataPredecessor(action2);
        addSchedulingDependency(action3, action5);
        addSchedulingDependency(action4, action5);

        action6.selectExecution(drs, action6.getImplementations()[0]);
        action6.addDataPredecessor(action2);
        addSchedulingDependency(action5, action6);

        action7.selectExecution(drs, action7.getImplementations()[0]);
        action7.addDataPredecessor(action2);
        addSchedulingDependency(action5, action7);

        action8.selectExecution(drs, action8.getImplementations()[0]);
        action8.addDataPredecessor(action5);
        addSchedulingDependency(action6, action8);
        addSchedulingDependency(action7, action8);

        action9.selectExecution(drs, action9.getImplementations()[0]);
        addSchedulingDependency(action8, action9);
        action9.addDataPredecessor(action5);

        action10.selectExecution(drs, action10.getImplementations()[0]);
        addSchedulingDependency(action8, action10);

        action11.selectExecution(drs, action11.getImplementations()[0]);
        addSchedulingDependency(action9, action11);
        addSchedulingDependency(action10, action11);
        action11.addDataPredecessor(action14);

        action12.selectExecution(drs, action12.getImplementations()[0]);
        addSchedulingDependency(action11, action12);

        action13.selectExecution(drs, action13.getImplementations()[0]);
        addSchedulingDependency(action11, action13);
        addSchedulingDependency(action12, action13);
        action13.addDataPredecessor(action15);

        // debugActions(action1, action2, action3, action4, action5, action6, action7, action8, action9, action10,
        // action11, action12, action13 );
        LinkedList<Gap> gaps = new LinkedList<Gap>();
        LinkedList<AllocatableAction>[] actions = new LinkedList[CoreManager.getCoreCount()];
        for (int i = 0; i < actions.length; i++) {
            actions[i] = new LinkedList<AllocatableAction>();
        }

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction> readyActions = new PriorityQueue<AllocatableAction>(1, drs.getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet selectableActions = new PriorityActionSet(ScheduleOptimizer.getSelectionComparator());

        // Scan actions: Filters ready and selectable actions
        LinkedList<AllocatableAction> runningActions = drs.scanActions(readyActions, selectableActions);
        HashMap<AllocatableAction, Long> expectedReady = new HashMap();
        expectedReady.put(action11, 10_000l);
        expectedReady.put(action13, 12_000l);
        Verifiers.verifyReadyActions(new PriorityQueue(readyActions), expectedReady);
        AllocatableAction[] expectedSelectable = new AllocatableAction[] { action3, action4, action10, action12 };
        Verifiers.verifyPriorityActions(new PriorityActionSet(selectableActions), expectedSelectable);

        PriorityQueue<AllocatableAction> donationActions = new PriorityQueue(1, ScheduleOptimizer.getDonationComparator());
        drs.rescheduleTasks(System.currentTimeMillis(), readyActions, selectableActions, runningActions, donationActions);

        /*drs.seekGaps(System.currentTimeMillis(), gaps, actions);

        long[][][] times = {
        new long[][]{//CORE 0
        new long[]{0, CORE0}, //1
        new long[]{0, CORE0}, //2
        new long[]{CORE0, 2 * CORE0}, //3
        new long[]{CORE0, 2 * CORE0}, //4
        new long[]{2 * CORE0 + CORE1, 3 * CORE0 + CORE1}, //6
        new long[]{3 * CORE0 + CORE1 + CORE3, 4 * CORE0 + CORE1 + CORE3}, //9
        new long[]{10_000 + CORE3, 10_000 + CORE3 + CORE0}, //12
        },
        new long[][]{//CORE 1
        new long[]{2 * CORE0, 2 * CORE0 + CORE1}, //5
        new long[]{12_000, 12_000 + CORE1}, //13
        },
        new long[][]{//CORE 2
        new long[]{2 * CORE0 + CORE1, 2 * CORE0 + CORE1 + CORE2}, //7
        new long[]{3 * CORE0 + CORE1 + CORE3, 3 * CORE0 + CORE1 + CORE2 + CORE3}, //10
        },
        new long[][]{//CORE 3
        new long[]{3 * CORE0 + CORE1, 3 * CORE0 + CORE1 + CORE3}, //8
        new long[]{10_000, 10_000 + CORE3}, //11
        },};
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
        Verifiers.verifyGaps(gaps, expectedGaps);*/
    }

    private void addSchedulingDependency(FakeAllocatableAction pred, FakeAllocatableAction succ) {
        FullGraphSchedulingInformation predDSI = (FullGraphSchedulingInformation) pred.getSchedulingInfo();
        predDSI.lock();
        FullGraphSchedulingInformation succDSI = (FullGraphSchedulingInformation) succ.getSchedulingInfo();
        succDSI.lock();
        if (pred.isPending()) {
            predDSI.addSuccessor(succ);
            succDSI.addPredecessor(pred);
        }
    }

}
