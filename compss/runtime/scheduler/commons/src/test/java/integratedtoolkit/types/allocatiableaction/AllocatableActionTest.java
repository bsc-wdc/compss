package integratedtoolkit.types.allocatiableaction;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.fake.FakeActionOrchestrator;
import integratedtoolkit.types.fake.FakeAllocatableAction;
import integratedtoolkit.types.fake.FakeResourceScheduler;
import integratedtoolkit.types.fake.FakeSI;
import integratedtoolkit.types.fake.FakeWorker;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.components.Processor;

import java.util.HashSet;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class AllocatableActionTest {

    private final MethodResourceDescription description;

    private final TaskScheduler ts;
    private final FakeActionOrchestrator fao;
    private final FakeResourceScheduler rs;

    public AllocatableActionTest() {
        // Method resource description and its slots
        int maxSlots = 3;
        Processor p = new Processor();
        p.setComputingUnits(maxSlots);
        this.description = new MethodResourceDescription();
        this.description.addProcessor(p);

        // Task Scheduler
        this.ts = new TaskScheduler();

        // Task Dispatcher
        this.fao = new FakeActionOrchestrator(this.ts);
        this.ts.setOrchestrator(this.fao);

        // Resource Scheduler
        this.rs = new FakeResourceScheduler(new FakeWorker(description, maxSlots), null);
    }

    @BeforeClass
    public static void setUpClass() {
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

    /**
     * Test of execute method, of class AllocatableAction.
     */
    @Test
    public void testExecute() throws BlockedActionException, UnassignedActionException, InvalidSchedulingException {
        // Create one instance
        prepare(1);
        FakeAllocatableAction instance = new FakeAllocatableAction(fao, 0);
        // Run it
        instance.assignResource(rs);
        instance.tryToLaunch();
        // Check if it was executed
        checkExecutions(new int[]{1});
    }

    /**
     * Test of end method, of class AllocatableAction.
     */
    @Test
    public void testEndSuccessors() {
        testEndNoSuccessors();
        testEndDataSuccessors();
        testEndResourceSuccessors();
        testEndTwoDataSuccessors();
        testEndTwoResourceSuccessors();
        testEndDataAndResourceSuccessors();
        testEndResourceAndDataSuccessors();
        testEndTwoDataPredecessors();
        testEndTwoResourcesPredecessors();
        testEndDataAndResourcesPredecessors();
        testEndResourcesAndPredecessors();
        testComplexGraph();
    }

    private void testEndNoSuccessors() {
        System.out.println("testEndNoSuccessors");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[]{1, 0, 0, 0});
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void testEndDataSuccessors() {
        System.out.println("testEndDataSuccessors");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            instance1.addDataPredecessor(instance0);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[]{1, 1, 0, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndResourceSuccessors() {
        System.out.println("testEndResourceSuccessors");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            addResourceDependency(instance0, instance1);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[]{1, 1, 0, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndTwoDataSuccessors() {
        System.out.println("testEndTwoDataSuccessors");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            instance1.addDataPredecessor(instance0);
            instance2.addDataPredecessor(instance0);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[]{1, 1, 1, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndTwoResourceSuccessors() {
        System.out.println("testEndTwoDataSuccessors");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            addResourceDependency(instance0, instance1);
            addResourceDependency(instance0, instance2);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[]{1, 1, 1, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndDataAndResourceSuccessors() {
        System.out.println("testEndDataAndResourceSuccessors");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            instance1.addDataPredecessor(instance0);
            addResourceDependency(instance0, instance2);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[]{1, 1, 1, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndResourceAndDataSuccessors() {
        System.out.println("testEndDataAndResourceSuccessors");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            instance1.addDataPredecessor(instance0);
            addResourceDependency(instance0, instance2);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[]{1, 1, 1, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndTwoDataPredecessors() {
        System.out.println("testEndTwoPredecessors");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            instance2.addDataPredecessor(instance0);
            instance2.addDataPredecessor(instance1);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[]{1, 0, 0, 0});
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[]{1, 1, 1, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndTwoResourcesPredecessors() {
        System.out.println("testEndTwoResourcesPredecessors");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            addResourceDependency(instance0, instance2);
            addResourceDependency(instance1, instance2);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[]{1, 0, 0, 0});
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[]{1, 1, 1, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndDataAndResourcesPredecessors() {
        System.out.println("testEndTwoPredecessors");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            instance2.addDataPredecessor(instance0);
            addResourceDependency(instance1, instance2);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[]{1, 0, 0, 0});
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[]{1, 1, 1, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndResourcesAndPredecessors() {
        System.out.println("testEndTwoPredecessors");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            addResourceDependency(instance0, instance2);
            instance2.addDataPredecessor(instance1);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[]{1, 0, 0, 0});
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[]{1, 1, 1, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testComplexGraph() {
        System.out.println("testComplexGraph");
        /*
         * Data dependency Graph
         * 
         * 1 2 3 \ | / \|/ 4 /\ / \ 5 6 | 7
         */
        try {
            prepare(7);
            FakeAllocatableAction task1 = new FakeAllocatableAction(fao, 0);
            task1.assignResource(rs);
            FakeAllocatableAction task2 = new FakeAllocatableAction(fao, 1);
            task2.assignResource(rs);
            FakeAllocatableAction task3 = new FakeAllocatableAction(fao, 2);
            task3.assignResource(rs);
            FakeAllocatableAction task4 = new FakeAllocatableAction(fao, 3);
            task4.assignResource(rs);
            task4.addDataPredecessor(task1);
            task4.addDataPredecessor(task2);
            task4.addDataPredecessor(task3);
            FakeAllocatableAction task5 = new FakeAllocatableAction(fao, 4);
            task5.assignResource(rs);
            task5.addDataPredecessor(task4);
            FakeAllocatableAction task6 = new FakeAllocatableAction(fao, 5);
            task6.assignResource(rs);
            task6.addDataPredecessor(task4);
            FakeAllocatableAction task7 = new FakeAllocatableAction(fao, 6);
            task7.assignResource(rs);
            task7.addDataPredecessor(task4);

            /*
             * Scheduling Resource 1 executes 1, 3, 4 and 5 Resource 2 executes 2, 6 and 7
             */
            // Resource 1
            addResourceDependency(task1, task3);
            addResourceDependency(task3, task4);
            addResourceDependency(task4, task5);
            // Resource 2
            addResourceDependency(task2, task6);
            addResourceDependency(task6, task7);

            task1.tryToLaunch();
            task2.tryToLaunch();
            checkExecutions(new int[]{1, 1, 0, 0, 0, 0, 0});
            completed(task1);
            checkExecutions(new int[]{1, 1, 1, 0, 0, 0, 0});
            completed(task2);
            checkExecutions(new int[]{1, 1, 1, 0, 0, 0, 0});
            completed(task3);
            checkExecutions(new int[]{1, 1, 1, 1, 0, 0, 0});
            completed(task4);
            checkExecutions(new int[]{1, 1, 1, 1, 1, 1, 0});
            completed(task5);
            checkExecutions(new int[]{1, 1, 1, 1, 1, 1, 0});
            completed(task6);
            checkExecutions(new int[]{1, 1, 1, 1, 1, 1, 1});
            completed(task7);
            checkExecutions(new int[]{1, 1, 1, 1, 1, 1, 1});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    @Test
    public void testError() {
        testOneError();
        testOneFail();
        testBasicErrorDependencies();
        testBasicFailDependencies();
        testFailDependencies();
    }

    private void testOneError() {
        System.out.println("testOneError");
        try {
            prepare(2);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance0.assignResource(rs);
            instance1.assignResource(rs);
            instance0.tryToLaunch();
            error(instance0);
            checkExecutions(new int[]{1, 0});
            checkErrors(new int[]{1, 0});
            checkFailed(new int[]{0, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testOneFail() {
        System.out.println("testOneFail");
        try {
            prepare(2);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance0.assignResource(rs);
            instance1.assignResource(rs);
            instance0.tryToLaunch();
            error(instance0);
            checkExecutions(new int[]{1, 0});
            checkErrors(new int[]{1, 0});
            checkFailed(new int[]{0, 0});

            instance0.assignResource(rs);
            instance0.tryToLaunch();
            error(instance0);
            checkExecutions(new int[]{2, 0});
            checkErrors(new int[]{2, 0});
            checkFailed(new int[]{1, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testBasicErrorDependencies() {
        System.out.println("testBasicErrorDependencies");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            instance1.addDataPredecessor(instance0);
            instance2.addDataPredecessor(instance0);

            addResourceDependency(instance0, instance1);
            addResourceDependency(instance0, instance2);
            addResourceDependency(instance0, instance3);
            instance0.tryToLaunch();
            checkExecutions(new int[]{1, 0, 0, 0});
            checkErrors(new int[]{0, 0, 0, 0});
            checkFailed(new int[]{0, 0, 0, 0});
            error(instance0);
            checkExecutions(new int[]{1, 0, 0, 1});
            checkErrors(new int[]{1, 0, 0, 0});
            checkFailed(new int[]{0, 0, 0, 0});

            instance0.assignResource(rs);
            addResourceDependency(instance3, instance0);
            completed(instance3);
            checkExecutions(new int[]{2, 0, 0, 1});
            checkErrors(new int[]{1, 0, 0, 0});
            checkFailed(new int[]{0, 0, 0, 0});

            completed(instance0);
            checkExecutions(new int[]{2, 1, 1, 1});
            checkErrors(new int[]{1, 0, 0, 0});
            checkFailed(new int[]{0, 0, 0, 0});

        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testBasicFailDependencies() {
        System.out.println("testBasicFailDependencies");
        try {
            prepare(4);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            instance2.addDataPredecessor(instance0);

            addResourceDependency(instance0, instance1);
            addResourceDependency(instance1, instance2);
            addResourceDependency(instance2, instance3);
            instance0.tryToLaunch();
            checkExecutions(new int[]{1, 0, 0, 0});
            checkErrors(new int[]{0, 0, 0, 0});
            checkFailed(new int[]{0, 0, 0, 0});
            error(instance0);
            checkExecutions(new int[]{1, 1, 0, 0});
            checkErrors(new int[]{1, 0, 0, 0});
            checkFailed(new int[]{0, 0, 0, 0});

            instance0.assignResource(rs);
            instance0.tryToLaunch();
            checkExecutions(new int[]{2, 1, 0, 0});
            checkErrors(new int[]{1, 0, 0, 0});
            checkFailed(new int[]{0, 0, 0, 0});

            error(instance0);
            checkExecutions(new int[]{2, 1, 0, 0});
            checkErrors(new int[]{2, 0, 0, 0});
            checkFailed(new int[]{1, 0, 1, 0});
            completed(instance1);
            checkExecutions(new int[]{2, 1, 0, 1});
            checkErrors(new int[]{2, 0, 0, 0});
            checkFailed(new int[]{1, 0, 1, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testFailDependencies() {

        System.out.println("testFailDependencies");
        try {
            prepare(6);
            /*
             * Data dependencies: 1-->4 Graph execution 2 5 / \ / 1~~~4 \ / \ 3 6
             */
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            instance0.assignResource(rs);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance1.assignResource(rs);
            FakeAllocatableAction instance2 = new FakeAllocatableAction(fao, 2);
            instance2.assignResource(rs);
            FakeAllocatableAction instance3 = new FakeAllocatableAction(fao, 3);
            instance3.assignResource(rs);
            FakeAllocatableAction instance4 = new FakeAllocatableAction(fao, 4);
            instance4.assignResource(rs);
            FakeAllocatableAction instance5 = new FakeAllocatableAction(fao, 5);
            instance5.assignResource(rs);
            instance3.addDataPredecessor(instance0);
            addResourceDependency(instance0, instance1);
            addResourceDependency(instance0, instance2);
            addResourceDependency(instance1, instance3);
            addResourceDependency(instance2, instance3);
            addResourceDependency(instance3, instance4);
            addResourceDependency(instance3, instance5);
            instance0.tryToLaunch();
            checkExecutions(new int[]{1, 0, 0, 0, 0, 0});
            checkErrors(new int[]{0, 0, 0, 0, 0, 0});
            checkFailed(new int[]{0, 0, 0, 0, 0, 0});
            error(instance0);
            checkExecutions(new int[]{1, 1, 1, 0, 0, 0});
            checkErrors(new int[]{1, 0, 0, 0, 0, 0});
            checkFailed(new int[]{0, 0, 0, 0, 0, 0});
            instance0.assignResource(rs);
            instance0.tryToLaunch();
            checkExecutions(new int[]{2, 1, 1, 0, 0, 0});
            checkErrors(new int[]{1, 0, 0, 0, 0, 0});
            checkFailed(new int[]{0, 0, 0, 0, 0, 0});
            error(instance0);
            checkExecutions(new int[]{2, 1, 1, 0, 0, 0});
            checkErrors(new int[]{2, 0, 0, 0, 0, 0});
            checkFailed(new int[]{1, 0, 0, 1, 0, 0});
            completed(instance1);
            checkExecutions(new int[]{2, 1, 1, 0, 0, 0});
            checkErrors(new int[]{2, 0, 0, 0, 0, 0});
            checkFailed(new int[]{1, 0, 0, 1, 0, 0});
            completed(instance2);
            checkExecutions(new int[]{2, 1, 1, 0, 1, 1});
            checkErrors(new int[]{2, 0, 0, 0, 0, 0});
            checkFailed(new int[]{1, 0, 0, 1, 0, 0});
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    /*---------------------------------------------------------
     ----------------------------------------------------------
     --------------------- SCHEDULER ACTIONS ------------------
     ----------------------------------------------------------
     --------------------------------------------------------*/
    public void completed(FakeAllocatableAction action)
            throws BlockedActionException, UnassignedActionException, InvalidSchedulingException {

        FakeResourceScheduler resource = (FakeResourceScheduler) action.getAssignedResource();
        LinkedList<AllocatableAction> dataFree = action.completed();
        LinkedList<AllocatableAction> resourceFree = resource
                .unscheduleAction(action);
        HashSet<AllocatableAction> freeTasks = new HashSet<>();
        freeTasks.addAll(dataFree);
        freeTasks.addAll(resourceFree);

        for (AllocatableAction a : freeTasks) {
            FakeAllocatableAction fa = (FakeAllocatableAction) a;
            fa.tryToLaunch();
        }
    }

    public void error(FakeAllocatableAction action) throws BlockedActionException, UnassignedActionException, InvalidSchedulingException {
        FakeResourceScheduler resource = (FakeResourceScheduler) action.getAssignedResource();
        LinkedList<AllocatableAction> resourceFree;
        try {
            action.error();
            resourceFree = resource.unscheduleAction(action);
        } catch (FailedActionException fae) {
            resourceFree = new LinkedList<>();
            for (AllocatableAction failed : action.failed()) {
                resourceFree.addAll(resource.unscheduleAction(failed));
            }
        }
        for (AllocatableAction a : resourceFree) {
            FakeAllocatableAction fa = (FakeAllocatableAction) a;
            fa.tryToLaunch();
        }
    }

    /*---------------------------------------------------------
     ----------------------------------------------------------
     ----------------------- TEST CHECKERS --------------------
     ----------------------------------------------------------
     --------------------------------------------------------*/
    private void prepare(int size) {
        FakeAllocatableAction.resize(size);
    }

    private void checkExecutions(int[] pattern) {
        if (pattern.length != FakeAllocatableAction.getSize()) {
            fail("Unconsistent execution arrays. " + FakeAllocatableAction.getSize() + " results obtained and " + pattern.length
                    + " expected");
            return;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != FakeAllocatableAction.getExecution(i)) {
                fail("AllocatableAction " + i + " should be executed " + pattern[i] + " time and it was "
                        + FakeAllocatableAction.getExecution(i));
            }
        }
    }

    private void checkErrors(int[] pattern) {
        if (pattern.length != FakeAllocatableAction.getSize()) {
            fail("Unconsistent AllocatableActionImpl.error arrays. " + FakeAllocatableAction.getSize() + " results obtained and "
                    + pattern.length + " expected");
            return;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != FakeAllocatableAction.getError(i)) {
                fail("AllocatableAction " + i + " should had " + pattern[i] + " AllocatableActionImpl.error and it was "
                        + FakeAllocatableAction.getError(i));
            }
        }
    }

    private void checkFailed(int[] pattern) {
        if (pattern.length != FakeAllocatableAction.getSize()) {
            fail("Unconsistent AllocatableActionImpl.failed arrays. " + FakeAllocatableAction.getSize() + " results obtained and "
                    + pattern.length + " expected");
            return;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != FakeAllocatableAction.getFailed(i)) {
                fail("AllocatableAction " + i + " should had AllocatableActionImpl.failed " + pattern[i] + " time and it was "
                        + FakeAllocatableAction.getFailed(i));

            }
        }
    }

    public static void addResourceDependency(FakeAllocatableAction pred, FakeAllocatableAction succ) {
        FakeSI dsiPred = (FakeSI) pred.getSchedulingInfo();
        FakeSI dsiSucc = (FakeSI) succ.getSchedulingInfo();
        if (pred.isPending()) {
            dsiSucc.addPredecessor(pred);
            dsiPred.addSuccessor(succ);
        }
    }

}
