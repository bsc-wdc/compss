package integratedtoolkit.scheduler.fullGraphScheduler;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphScheduler;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import integratedtoolkit.scheduler.fullGraphScheduler.ScheduleOptimizer;
import integratedtoolkit.scheduler.fullGraphScheduler.utils.Verifiers;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.OptimizationWorker;
import integratedtoolkit.scheduler.types.PriorityActionSet;
import integratedtoolkit.scheduler.types.fake.FakeActionOrchestrator;
import integratedtoolkit.scheduler.types.fake.FakeAllocatableAction;
import integratedtoolkit.scheduler.types.fake.FakeImplementation;
import integratedtoolkit.scheduler.types.fake.FakeProfile;
import integratedtoolkit.scheduler.types.fake.FakeResourceDescription;
import integratedtoolkit.scheduler.types.fake.FakeResourceScheduler;
import integratedtoolkit.scheduler.types.fake.FakeWorker;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.util.CoreManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import org.junit.BeforeClass;
import org.junit.Test;


public class OptimizationTest {

    private static FullGraphScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> ds;
    private static FakeActionOrchestrator fao;
    private static FullGraphResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> drs1;
    private static FullGraphResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> drs2;


    public OptimizationTest() {
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
        CoreManager.registerNewCoreElement("fakeSignature30");
        CoreManager.registerNewCoreElement("fakeSignature40");
        CoreManager.registerNewCoreElement("fakeSignature50");
        CoreManager.registerNewCoreElement("fakeSignature60");
        CoreManager.registerNewCoreElement("fakeSignature70");

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

        FakeImplementation impl30 = new FakeImplementation(3, 0, new FakeResourceDescription(4));
        List<Implementation<?>> impls3 = new LinkedList<>();
        impls3.add(impl30);
        List<String> signatures3 = new LinkedList<>();
        signatures3.add("fakeSignature30");
        CoreManager.registerNewImplementations(3, impls3, signatures3);

        FakeImplementation impl40 = new FakeImplementation(4, 0, new FakeResourceDescription(2));
        List<Implementation<?>> impls4 = new LinkedList<>();
        impls4.add(impl40);
        List<String> signatures4 = new LinkedList<>();
        signatures4.add("fakeSignature40");
        CoreManager.registerNewImplementations(4, impls4, signatures4);

        FakeImplementation impl50 = new FakeImplementation(5, 0, new FakeResourceDescription(1));
        List<Implementation<?>> impls5 = new LinkedList<>();
        impls5.add(impl50);
        List<String> signatures5 = new LinkedList<>();
        signatures5.add("fakeSignature50");
        CoreManager.registerNewImplementations(5, impls5, signatures5);

        FakeImplementation impl60 = new FakeImplementation(6, 0, new FakeResourceDescription(3));
        List<Implementation<?>> impls6 = new LinkedList<>();
        impls6.add(impl60);
        List<String> signatures6 = new LinkedList<>();
        signatures6.add("fakeSignature60");
        CoreManager.registerNewImplementations(6, impls6, signatures6);

        int maxSlots = 4;
        FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
        FakeWorker fw = new FakeWorker("worker1", frd, maxSlots);
        drs1 = new FullGraphResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation>(fw, fao);

        FakeResourceDescription frd2 = new FakeResourceDescription(maxSlots);
        FakeWorker fw2 = new FakeWorker("worker2", frd2, maxSlots);
        drs2 = new FullGraphResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation>(fw2, fao);

        drs1.profiledExecution(impl00, new FakeProfile(50));
        drs1.profiledExecution(impl10, new FakeProfile(50));
        drs1.profiledExecution(impl20, new FakeProfile(30));
        drs1.profiledExecution(impl30, new FakeProfile(50));
        drs1.profiledExecution(impl40, new FakeProfile(20));
        drs1.profiledExecution(impl50, new FakeProfile(10));
        drs1.profiledExecution(impl60, new FakeProfile(30));

        drs2.profiledExecution(impl00, new FakeProfile(50));
        drs2.profiledExecution(impl10, new FakeProfile(50));
        drs2.profiledExecution(impl20, new FakeProfile(30));
        // Faster than drs
        drs2.profiledExecution(impl30, new FakeProfile(30));
        drs2.profiledExecution(impl40, new FakeProfile(15));
        drs2.profiledExecution(impl50, new FakeProfile(10));
        drs2.profiledExecution(impl60, new FakeProfile(15));
    }

    // @Test
    @SuppressWarnings("unchecked")
    public void testDonorsAndReceivers() {
        ScheduleOptimizer<FakeProfile, FakeResourceDescription, FakeImplementation> so = new ScheduleOptimizer<>(ds);

        long[] expectedEndTimes = new long[] { 35000, 20000, 15000, 50000, 40000, 1000 };
        OptimizationWorker<FakeProfile, FakeResourceDescription, FakeImplementation>[] optimizedWorkers = new OptimizationWorker[expectedEndTimes.length];
        for (int idx = 0; idx < expectedEndTimes.length; idx++) {
            int maxSlots = 1;
            FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
            FakeWorker fw = new FakeWorker("worker" + idx, frd, maxSlots);
            FakeResourceScheduler frs = new FakeResourceScheduler(fw, fao, expectedEndTimes[idx]);
            optimizedWorkers[idx] = new OptimizationWorker<>(frs);
        }

        LinkedList<OptimizationWorker<FakeProfile, FakeResourceDescription, FakeImplementation>> receivers = new LinkedList<>();
        OptimizationWorker<FakeProfile, FakeResourceDescription, FakeImplementation> donor = so.determineDonorAndReceivers(optimizedWorkers,
                receivers);

        LinkedList<OptimizationWorker<FakeProfile, FakeResourceDescription, FakeImplementation>> donors = new LinkedList<>();
        donors.offer(donor);
        LinkedList<String> donorsNames = new LinkedList<>();
        donorsNames.add("worker3");

        LinkedList<String> receiversNames = new LinkedList<>();
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
        ScheduleOptimizer<FakeProfile, FakeResourceDescription, FakeImplementation> so = new ScheduleOptimizer<>(ds);

        long updateId = System.currentTimeMillis();

        Collection<ResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation>> workers = new ArrayList<>();
        drs1.clear();
        drs2.clear();
        workers.add(drs1);

        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, CoreManager.getCoreImplementations(4));
        action1.selectExecution(drs1, (FakeImplementation) action1.getImplementations()[0]);
        drs1.scheduleAction(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, CoreManager.getCoreImplementations(4));
        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        drs1.scheduleAction(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 1, CoreManager.getCoreImplementations(4));
        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        drs1.scheduleAction(action3);

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, CoreManager.getCoreImplementations(5));
        action4.selectExecution(drs1, (FakeImplementation) action4.getImplementations()[0]);
        drs1.scheduleAction(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 1, CoreManager.getCoreImplementations(4));
        action5.selectExecution(drs1, (FakeImplementation) action5.getImplementations()[0]);
        drs1.scheduleAction(action5);

        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 1, CoreManager.getCoreImplementations(6));
        action6.selectExecution(drs1, (FakeImplementation) action6.getImplementations()[0]);
        drs1.scheduleAction(action6);

        workers.add(drs2);

        so.globalOptimization(updateId, workers);
    }

    // @Test
    @SuppressWarnings("unchecked")
    public void testScan() {
        FakeAllocatableAction external10 = new FakeAllocatableAction(fao, 13, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external10.getSchedulingInfo())
                .setExpectedEnd(10);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external10.getSchedulingInfo())
                .scheduled();
        external10.selectExecution(drs2, (FakeImplementation) external10.getImplementations()[0]);

        FakeAllocatableAction external20 = new FakeAllocatableAction(fao, 14, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external20.getSchedulingInfo())
                .setExpectedEnd(20);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external20.getSchedulingInfo())
                .scheduled();
        external20.selectExecution(drs2, (FakeImplementation) external20.getImplementations()[0]);

        FakeAllocatableAction external90 = new FakeAllocatableAction(fao, 15, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external90.getSchedulingInfo())
                .setExpectedEnd(90);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external90.getSchedulingInfo())
                .scheduled();
        external90.selectExecution(drs2, (FakeImplementation) external90.getImplementations()[0]);

        drs1.clear();
        drs2.clear();

        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, CoreManager.getCoreImplementations(4));
        action1.selectExecution(drs1, (FakeImplementation) action1.getImplementations()[0]);
        drs1.scheduleAction(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, CoreManager.getCoreImplementations(4));
        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        drs1.scheduleAction(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 1, CoreManager.getCoreImplementations(4));
        action3.addDataPredecessor(external90);
        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        drs1.scheduleAction(action3);

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, CoreManager.getCoreImplementations(5));
        action4.selectExecution(drs1, (FakeImplementation) action4.getImplementations()[0]);
        drs1.scheduleAction(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 1, CoreManager.getCoreImplementations(4));
        action5.selectExecution(drs1, (FakeImplementation) action5.getImplementations()[0]);
        drs1.scheduleAction(action5);

        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 1, CoreManager.getCoreImplementations(6));
        action6.selectExecution(drs1, (FakeImplementation) action6.getImplementations()[0]);
        drs1.scheduleAction(action6);

        FakeAllocatableAction action7 = new FakeAllocatableAction(fao, 7, 0, CoreManager.getCoreImplementations(5));
        action7.addDataPredecessor(external10);
        action7.selectExecution(drs1, (FakeImplementation) action7.getImplementations()[0]);
        drs1.scheduleAction(action7);

        FakeAllocatableAction action8 = new FakeAllocatableAction(fao, 8, 0, CoreManager.getCoreImplementations(5));
        action8.addDataPredecessor(external20);
        action8.selectExecution(drs1, (FakeImplementation) action8.getImplementations()[0]);
        drs1.scheduleAction(action8);

        FakeAllocatableAction action9 = new FakeAllocatableAction(fao, 9, 0, CoreManager.getCoreImplementations(4));
        action9.addDataPredecessor(external90);
        action9.selectExecution(drs1, (FakeImplementation) action9.getImplementations()[0]);
        drs1.scheduleAction(action9);

        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, CoreManager.getCoreImplementations(4));
        action10.addDataPredecessor(action5);
        action10.selectExecution(drs1, (FakeImplementation) action10.getImplementations()[0]);
        drs1.scheduleAction(action10);

        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, CoreManager.getCoreImplementations(4));
        action11.addDataPredecessor(action6);
        action11.selectExecution(drs1, (FakeImplementation) action11.getImplementations()[0]);
        drs1.scheduleAction(action11);

        FakeAllocatableAction action12 = new FakeAllocatableAction(fao, 12, 0, CoreManager.getCoreImplementations(4));
        action12.addDataPredecessor(action5);
        action12.addDataPredecessor(action6);
        action12.selectExecution(drs1, (FakeImplementation) action12.getImplementations()[0]);
        drs1.scheduleAction(action12);

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>> readyActions = new PriorityQueue<>(1,
                FullGraphResourceScheduler.getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet<FakeProfile, FakeResourceDescription, FakeImplementation> selectableActions = new PriorityActionSet<>(
                FullGraphResourceScheduler.getScanComparator());

        drs1.scanActions(readyActions, selectableActions);

        HashMap<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>, Long> expectedReady = new HashMap<>();
        expectedReady.put(action7, 10l);
        expectedReady.put(action8, 20l);
        expectedReady.put(action9, 90l);
        expectedReady.put(action3, 90l);
        Verifiers.verifyReadyActions(new PriorityQueue<>(readyActions), expectedReady);

        AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>[] expectedSelectable = new AllocatableAction[] {
                action5, action6, action4 };
        Verifiers.verifyPriorityActions(new PriorityActionSet<>(selectableActions), expectedSelectable);
    }

    public void printAction(AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation> action) {
        System.out.println(action + " Core Element " + action.getCoreId() + " Implementation "
                + action.getAssignedImplementation().getImplementationId() + " (" + action.getAssignedImplementation() + ")");
        FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation> dsi = (FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action
                .getSchedulingInfo();
        System.out.println("\t Optimization:" + dsi.isOnOptimization());
        System.out.println("\t StartTime:" + dsi.getExpectedStart());
        System.out.println("\t EndTime:" + dsi.getExpectedEnd());
        System.out.println("\t Locks:" + dsi.getLockCount());
        System.out.println("\t Predecessors:" + dsi.getPredecessors());
        System.out.println("\t Successors:" + dsi.getSuccessors());
        System.out.println("\t Optimization Successors:" + dsi.getOptimizingSuccessors());

    }

    // @Test
    @SuppressWarnings("unchecked")
    public void testPendingActions() {
        LinkedList<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>> pendingActions = new LinkedList<>();

        FakeAllocatableAction external10 = new FakeAllocatableAction(fao, 13, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external10.getSchedulingInfo())
                .setExpectedEnd(10);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external10.getSchedulingInfo())
                .scheduled();
        external10.selectExecution(drs2, (FakeImplementation) external10.getImplementations()[0]);

        FakeAllocatableAction external20 = new FakeAllocatableAction(fao, 14, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external20.getSchedulingInfo())
                .setExpectedEnd(20);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external20.getSchedulingInfo())
                .scheduled();
        external20.selectExecution(drs2, (FakeImplementation) external20.getImplementations()[0]);

        FakeAllocatableAction external90 = new FakeAllocatableAction(fao, 15, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external90.getSchedulingInfo())
                .setExpectedEnd(90);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external90.getSchedulingInfo())
                .scheduled();
        external90.selectExecution(drs2, (FakeImplementation) external90.getImplementations()[0]);

        drs1.clear();
        drs2.clear();

        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, CoreManager.getCoreImplementations(4));
        action1.selectExecution(drs1, (FakeImplementation) action1.getImplementations()[0]);
        drs1.scheduleAction(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, CoreManager.getCoreImplementations(4));
        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        drs1.scheduleAction(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 1, CoreManager.getCoreImplementations(4));
        action3.addDataPredecessor(external90);
        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        drs1.scheduleAction(action3);

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, CoreManager.getCoreImplementations(5));
        action4.selectExecution(drs1, (FakeImplementation) action4.getImplementations()[0]);
        drs1.scheduleAction(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 1, CoreManager.getCoreImplementations(4));
        action5.selectExecution(drs1, (FakeImplementation) action5.getImplementations()[0]);
        drs1.scheduleAction(action5);

        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 1, CoreManager.getCoreImplementations(6));
        action6.selectExecution(drs1, (FakeImplementation) action6.getImplementations()[0]);
        drs1.scheduleAction(action6);

        FakeAllocatableAction action7 = new FakeAllocatableAction(fao, 7, 0, CoreManager.getCoreImplementations(5));
        action7.addDataPredecessor(external10);
        action7.selectExecution(drs1, (FakeImplementation) action7.getImplementations()[0]);
        pendingActions.add(action7);

        FakeAllocatableAction action8 = new FakeAllocatableAction(fao, 8, 0, CoreManager.getCoreImplementations(5));
        action8.addDataPredecessor(external20);
        action8.selectExecution(drs1, (FakeImplementation) action8.getImplementations()[0]);
        pendingActions.add(action8);

        FakeAllocatableAction action9 = new FakeAllocatableAction(fao, 9, 0, CoreManager.getCoreImplementations(4));
        action9.addDataPredecessor(external90);
        action9.selectExecution(drs1, (FakeImplementation) action9.getImplementations()[0]);
        pendingActions.add(action9);

        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, CoreManager.getCoreImplementations(4));
        action10.addDataPredecessor(action5);
        action10.selectExecution(drs1, (FakeImplementation) action10.getImplementations()[0]);
        pendingActions.add(action10);

        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, CoreManager.getCoreImplementations(4));
        action11.addDataPredecessor(action6);
        action11.selectExecution(drs1, (FakeImplementation) action11.getImplementations()[0]);
        pendingActions.add(action11);

        FakeAllocatableAction action12 = new FakeAllocatableAction(fao, 12, 0, CoreManager.getCoreImplementations(4));
        action12.addDataPredecessor(action5);
        action12.addDataPredecessor(action6);
        action12.selectExecution(drs1, (FakeImplementation) action12.getImplementations()[0]);
        pendingActions.add(action12);

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>> readyActions = new PriorityQueue<>(1,
                FullGraphResourceScheduler.getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet<FakeProfile, FakeResourceDescription, FakeImplementation> selectableActions = new PriorityActionSet<>(
                FullGraphResourceScheduler.getScanComparator());

        drs1.scanActions(readyActions, selectableActions);
        drs1.classifyPendingSchedulings(pendingActions, readyActions, selectableActions,
                new LinkedList<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>>());

        HashMap<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>, Long> expectedReady = new HashMap<>();
        expectedReady.put(action7, 10l);
        expectedReady.put(action8, 20l);
        expectedReady.put(action9, 90l);
        expectedReady.put(action3, 90l);
        Verifiers.verifyReadyActions(new PriorityQueue<>(readyActions), expectedReady);

        AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>[] expectedSelectable = new AllocatableAction[] {
                action5, action6, action4 };
        Verifiers.verifyPriorityActions(new PriorityActionSet<FakeProfile, FakeResourceDescription, FakeImplementation>(selectableActions),
                expectedSelectable);
    }

    @SuppressWarnings("static-access")
    @Test
    public void testLocalOptimization() {

        drs1.clear();
        drs2.clear();

        FakeAllocatableAction external10 = new FakeAllocatableAction(fao, 13, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external10.getSchedulingInfo())
                .setExpectedEnd(10);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external10.getSchedulingInfo())
                .scheduled();
        external10.selectExecution(drs2, (FakeImplementation) external10.getImplementations()[0]);

        FakeAllocatableAction external20 = new FakeAllocatableAction(fao, 14, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external20.getSchedulingInfo())
                .setExpectedEnd(20);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external20.getSchedulingInfo())
                .scheduled();
        external20.selectExecution(drs2, (FakeImplementation) external20.getImplementations()[0]);

        FakeAllocatableAction external90 = new FakeAllocatableAction(fao, 15, 0, CoreManager.getCoreImplementations(4));
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external90.getSchedulingInfo())
                .setExpectedEnd(90);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) external90.getSchedulingInfo())
                .scheduled();
        external90.selectExecution(drs2, (FakeImplementation) external90.getImplementations()[0]);

        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, CoreManager.getCoreImplementations(4));
        action1.selectExecution(drs1, (FakeImplementation) action1.getImplementations()[0]);
        drs1.scheduleAction(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, CoreManager.getCoreImplementations(4));
        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        drs1.scheduleAction(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 1, CoreManager.getCoreImplementations(4));
        action3.addDataPredecessor(external90);
        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        drs1.scheduleAction(action3);

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, CoreManager.getCoreImplementations(5));
        action4.selectExecution(drs1, (FakeImplementation) action4.getImplementations()[0]);
        drs1.scheduleAction(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 1, CoreManager.getCoreImplementations(4));
        action5.selectExecution(drs1, (FakeImplementation) action5.getImplementations()[0]);
        drs1.scheduleAction(action5);

        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 1, CoreManager.getCoreImplementations(6));
        action6.selectExecution(drs1, (FakeImplementation) action6.getImplementations()[0]);
        drs1.scheduleAction(action6);

        FakeAllocatableAction action7 = new FakeAllocatableAction(fao, 7, 0, CoreManager.getCoreImplementations(5));
        action7.addDataPredecessor(external10);
        action7.selectExecution(drs1, (FakeImplementation) action7.getImplementations()[0]);
        drs1.scheduleAction(action7);

        FakeAllocatableAction action8 = new FakeAllocatableAction(fao, 8, 0, CoreManager.getCoreImplementations(5));
        action8.addDataPredecessor(external20);
        action8.selectExecution(drs1, (FakeImplementation) action8.getImplementations()[0]);
        drs1.scheduleAction(action8);

        FakeAllocatableAction action9 = new FakeAllocatableAction(fao, 9, 0, CoreManager.getCoreImplementations(4));
        action9.addDataPredecessor(external90);
        action9.selectExecution(drs1, (FakeImplementation) action9.getImplementations()[0]);
        drs1.scheduleAction(action9);

        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, CoreManager.getCoreImplementations(4));
        action10.addDataPredecessor(action5);
        action10.selectExecution(drs1, (FakeImplementation) action10.getImplementations()[0]);
        drs1.scheduleAction(action10);

        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, CoreManager.getCoreImplementations(4));
        action11.addDataPredecessor(action6);
        action11.selectExecution(drs1, (FakeImplementation) action11.getImplementations()[0]);
        drs1.scheduleAction(action11);

        FakeAllocatableAction action12 = new FakeAllocatableAction(fao, 12, 0, CoreManager.getCoreImplementations(4));
        action12.addDataPredecessor(action5);
        action12.addDataPredecessor(action6);
        action12.selectExecution(drs1, (FakeImplementation) action12.getImplementations()[0]);
        drs1.scheduleAction(action12);

        // Simulate Scan results
        LinkedList<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>> runningActions = new LinkedList<>();
        PriorityQueue<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>> readyActions = new PriorityQueue<>(1,
                drs1.getReadyComparator());
        PriorityActionSet<FakeProfile, FakeResourceDescription, FakeImplementation> selectableActions = new PriorityActionSet<>(
                ScheduleOptimizer.getSelectionComparator());

        long updateId = System.currentTimeMillis();

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action1.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action1.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action1.getSchedulingInfo())
                .setExpectedStart(0);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action1.getSchedulingInfo()).lock();
        runningActions.add(action1);

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action2.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action2.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action2.getSchedulingInfo())
                .setExpectedStart(0);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action2.getSchedulingInfo()).lock();
        runningActions.add(action2);

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action3.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action3.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action3.getSchedulingInfo())
                .setExpectedStart(90);
        readyActions.offer(action3);

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action4.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action4.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action4.getSchedulingInfo())
                .setExpectedStart(0);
        selectableActions.offer(action4);

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action5.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action5.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action5.getSchedulingInfo())
                .setExpectedStart(0);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action5.getSchedulingInfo())
                .optimizingSuccessor(action12);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action5.getSchedulingInfo())
                .optimizingSuccessor(action10);
        selectableActions.offer(action5);

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action6.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action6.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action6.getSchedulingInfo())
                .setExpectedStart(0);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action6.getSchedulingInfo())
                .optimizingSuccessor(action12);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action6.getSchedulingInfo())
                .optimizingSuccessor(action11);
        selectableActions.offer(action6);

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action7.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action7.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action7.getSchedulingInfo())
                .setExpectedStart(10);
        readyActions.offer(action7);

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action8.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action8.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action8.getSchedulingInfo())
                .setExpectedStart(20);
        readyActions.offer(action8);

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action9.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action9.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action9.getSchedulingInfo())
                .setExpectedStart(90);
        readyActions.offer(action9);

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action10.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action10.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action10.getSchedulingInfo())
                .setExpectedStart(0);

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action11.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action11.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action11.getSchedulingInfo())
                .setExpectedStart(0);

        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action12.getSchedulingInfo())
                .setOnOptimization(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action12.getSchedulingInfo())
                .setToReschedule(true);
        ((FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action12.getSchedulingInfo())
                .setExpectedStart(0);

        PriorityQueue<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>> donationActions = new PriorityQueue<>(1,
                ScheduleOptimizer.getDonationComparator());

        drs1.rescheduleTasks(updateId, readyActions, selectableActions, runningActions, donationActions);
    }

    // @Test
    @SuppressWarnings("unchecked")
    public void testNoDataDependencies()
            throws BlockedActionException, UnassignedActionException, InvalidSchedulingException, InterruptedException {

        // Build graph
        /*
         * 1 --> 3 --> 5 -->6 --> 8 -->9 ----->11 -->12 --> 13 2 --> 4 ┘ └->7 ┘ └->10 ---| └-----┘ | |
         * ------------------------------------------------------- 14┘ 15┘
         */

        drs1.clear();

        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 0, CoreManager.getCoreImplementations(1));
        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action7 = new FakeAllocatableAction(fao, 7, 0, CoreManager.getCoreImplementations(2));
        FakeAllocatableAction action8 = new FakeAllocatableAction(fao, 8, 0, CoreManager.getCoreImplementations(3));
        FakeAllocatableAction action9 = new FakeAllocatableAction(fao, 9, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, CoreManager.getCoreImplementations(2));
        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, CoreManager.getCoreImplementations(3));
        FakeAllocatableAction action12 = new FakeAllocatableAction(fao, 12, 0, CoreManager.getCoreImplementations(0));
        FakeAllocatableAction action13 = new FakeAllocatableAction(fao, 13, 0, CoreManager.getCoreImplementations(1));

        FakeAllocatableAction action14 = new FakeAllocatableAction(fao, 14, 0, CoreManager.getCoreImplementations(0));
        action14.selectExecution(drs2, (FakeImplementation) action14.getImplementations()[0]);
        FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation> dsi14 = (FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action14
                .getSchedulingInfo();
        dsi14.setExpectedEnd(10_000);

        FakeAllocatableAction action15 = new FakeAllocatableAction(fao, 15, 0, CoreManager.getCoreImplementations(0));
        action15.selectExecution(drs2, (FakeImplementation) action15.getImplementations()[0]);
        FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation> dsi15 = (FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) action15
                .getSchedulingInfo();
        dsi15.setExpectedEnd(12_000);

        action1.selectExecution(drs1, (FakeImplementation) action1.getImplementations()[0]);
        action1.tryToLaunch();

        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        action2.tryToLaunch();

        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        addSchedulingDependency(action1, action3);

        action4.selectExecution(drs1, (FakeImplementation) action4.getImplementations()[0]);
        addSchedulingDependency(action2, action4);

        action5.selectExecution(drs1, (FakeImplementation) action5.getImplementations()[0]);
        action5.addDataPredecessor(action2);
        addSchedulingDependency(action3, action5);
        addSchedulingDependency(action4, action5);

        action6.selectExecution(drs1, (FakeImplementation) action6.getImplementations()[0]);
        action6.addDataPredecessor(action2);
        addSchedulingDependency(action5, action6);

        action7.selectExecution(drs1, (FakeImplementation) action7.getImplementations()[0]);
        action7.addDataPredecessor(action2);
        addSchedulingDependency(action5, action7);

        action8.selectExecution(drs1, (FakeImplementation) action8.getImplementations()[0]);
        action8.addDataPredecessor(action5);
        addSchedulingDependency(action6, action8);
        addSchedulingDependency(action7, action8);

        action9.selectExecution(drs1, (FakeImplementation) action9.getImplementations()[0]);
        addSchedulingDependency(action8, action9);
        action9.addDataPredecessor(action5);

        action10.selectExecution(drs1, (FakeImplementation) action10.getImplementations()[0]);
        addSchedulingDependency(action8, action10);

        action11.selectExecution(drs1, (FakeImplementation) action11.getImplementations()[0]);
        addSchedulingDependency(action9, action11);
        addSchedulingDependency(action10, action11);
        action11.addDataPredecessor(action14);

        action12.selectExecution(drs1, (FakeImplementation) action12.getImplementations()[0]);
        addSchedulingDependency(action11, action12);

        action13.selectExecution(drs1, (FakeImplementation) action13.getImplementations()[0]);
        addSchedulingDependency(action11, action13);
        addSchedulingDependency(action12, action13);
        action13.addDataPredecessor(action15);

        // debugActions(action1, action2, action3, action4, action5, action6, action7, action8, action9, action10,
        // action11, action12, action13 );
        LinkedList<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>>[] actions = new LinkedList[CoreManager
                .getCoreCount()];
        for (int i = 0; i < actions.length; i++) {
            actions[i] = new LinkedList<>();
        }

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>> readyActions = new PriorityQueue<>(1,
                FullGraphResourceScheduler.getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet<FakeProfile, FakeResourceDescription, FakeImplementation> selectableActions = new PriorityActionSet<>(
                FullGraphResourceScheduler.getScanComparator());

        LinkedList<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>> runningActions = drs1
                .scanActions(readyActions, selectableActions);

        HashMap<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>, Long> expectedReady = new HashMap<>();
        expectedReady.put(action11, 10_000l);
        expectedReady.put(action13, 12_000l);
        Verifiers.verifyReadyActions(new PriorityQueue<>(readyActions), expectedReady);
        AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>[] expectedSelectable = new AllocatableAction[] {
                action3, action4, action10, action12 };
        Verifiers.verifyPriorityActions(new PriorityActionSet<FakeProfile, FakeResourceDescription, FakeImplementation>(selectableActions),
                expectedSelectable);

        PriorityQueue<AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation>> donationActions = new PriorityQueue<>(1,
                ScheduleOptimizer.getDonationComparator());
        drs1.rescheduleTasks(System.currentTimeMillis(), readyActions, selectableActions, runningActions, donationActions);

        /*
         * drs.seekGaps(System.currentTimeMillis(), gaps, actions);
         * 
         * long[][][] times = { new long[][]{//CORE 0 new long[]{0, CORE0}, //1 new long[]{0, CORE0}, //2 new
         * long[]{CORE0, 2 * CORE0}, //3 new long[]{CORE0, 2 * CORE0}, //4 new long[]{2 * CORE0 + CORE1, 3 * CORE0 +
         * CORE1}, //6 new long[]{3 * CORE0 + CORE1 + CORE3, 4 * CORE0 + CORE1 + CORE3}, //9 new long[]{10_000 + CORE3,
         * 10_000 + CORE3 + CORE0}, //12 }, new long[][]{//CORE 1 new long[]{2 * CORE0, 2 * CORE0 + CORE1}, //5 new
         * long[]{12_000, 12_000 + CORE1}, //13 }, new long[][]{//CORE 2 new long[]{2 * CORE0 + CORE1, 2 * CORE0 + CORE1
         * + CORE2}, //7 new long[]{3 * CORE0 + CORE1 + CORE3, 3 * CORE0 + CORE1 + CORE2 + CORE3}, //10 }, new
         * long[][]{//CORE 3 new long[]{3 * CORE0 + CORE1, 3 * CORE0 + CORE1 + CORE3}, //8 new long[]{10_000, 10_000 +
         * CORE3}, //11 },}; Verifiers.verifyUpdate(actions, times);
         * 
         * Gap[] expectedGaps = { new Gap(2 * CORE0, 3 * CORE0 + CORE1, action3, new FakeResourceDescription(1), 0), new
         * Gap(2 * CORE0 + CORE1 + CORE2, 3 * CORE0 + CORE1, action7, new FakeResourceDescription(1), 0), new Gap(3 *
         * CORE0 + CORE1 + CORE3, 10_000, action8, new FakeResourceDescription(1), 0), new Gap(3 * CORE0 + CORE1 + CORE2
         * + CORE3, 10_000, action10, new FakeResourceDescription(1), 0), new Gap(4 * CORE0 + CORE1 + CORE3, 10_000,
         * action9, new FakeResourceDescription(2), 0), new Gap(10_000 + CORE3 + CORE0, 12_000, action12, new
         * FakeResourceDescription(2), 0), new Gap(10_000 + CORE3, 12_000, action11, new FakeResourceDescription(1), 0),
         * new Gap(10_000 + CORE3, Long.MAX_VALUE, action11, new FakeResourceDescription(1), 0), new Gap(12_000 + CORE1,
         * Long.MAX_VALUE, action13, new FakeResourceDescription(3), 0),}; Verifiers.verifyGaps(gaps, expectedGaps);
         */
    }

    private void addSchedulingDependency(FakeAllocatableAction pred, FakeAllocatableAction succ) {
        FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation> predDSI = (FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) pred
                .getSchedulingInfo();
        predDSI.lock();
        FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation> succDSI = (FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) succ
                .getSchedulingInfo();
        succDSI.lock();
        if (pred.isPending()) {
            predDSI.addSuccessor(succ);
            succDSI.addPredecessor(pred);
        }
    }

}
