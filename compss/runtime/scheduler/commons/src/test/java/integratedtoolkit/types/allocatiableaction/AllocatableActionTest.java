package integratedtoolkit.types.allocatiableaction;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.fake.FakeWorker;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.resources.components.Processor;

import java.util.HashSet;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;


public class AllocatableActionTest<P extends Profile, T extends WorkerResourceDescription> {

    private final MethodResourceDescription description;
    private final ResourceScheduler<P, T> r;


    public AllocatableActionTest() {
        int maxSlots = 3;
        this.description = new MethodResourceDescription();

        // Slots
        Processor p = new Processor();
        p.setComputingUnits(maxSlots);
        this.description.addProcessor(p);

        // Resource Scheduler
        this.r = new DummyResourceScheduler<P, T>(new FakeWorker<T>(description, maxSlots));
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
        AllocatableAction<P, T> instance = new AllocatableActionImpl<>(0);
        // Run it
        instance.assignResources(r);
        instance.tryToLaunch();
        // Check if it was executed
        checkExecutions(new int[] { 1 });
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
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[] { 1, 0, 0, 0 });
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void testEndDataSuccessors() {
        System.out.println("testEndDataSuccessors");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            instance1.addDataPredecessor(instance0);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[] { 1, 1, 0, 0 });
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndResourceSuccessors() {
        System.out.println("testEndResourceSuccessors");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            addResourceDependency(instance0, instance1);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[] { 1, 1, 0, 0 });
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndTwoDataSuccessors() {
        System.out.println("testEndTwoDataSuccessors");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            instance1.addDataPredecessor(instance0);
            instance2.addDataPredecessor(instance0);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndTwoResourceSuccessors() {
        System.out.println("testEndTwoDataSuccessors");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            addResourceDependency(instance0, instance1);
            addResourceDependency(instance0, instance2);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndDataAndResourceSuccessors() {
        System.out.println("testEndDataAndResourceSuccessors");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            instance1.addDataPredecessor(instance0);
            addResourceDependency(instance0, instance2);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndResourceAndDataSuccessors() {
        System.out.println("testEndDataAndResourceSuccessors");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            instance1.addDataPredecessor(instance0);
            addResourceDependency(instance0, instance2);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndTwoDataPredecessors() {
        System.out.println("testEndTwoPredecessors");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            instance2.addDataPredecessor(instance0);
            instance2.addDataPredecessor(instance1);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[] { 1, 0, 0, 0 });
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndTwoResourcesPredecessors() {
        System.out.println("testEndTwoResourcesPredecessors");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            addResourceDependency(instance0, instance2);
            addResourceDependency(instance1, instance2);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[] { 1, 0, 0, 0 });
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndDataAndResourcesPredecessors() {
        System.out.println("testEndTwoPredecessors");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            instance2.addDataPredecessor(instance0);
            addResourceDependency(instance1, instance2);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[] { 1, 0, 0, 0 });
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testEndResourcesAndPredecessors() {
        System.out.println("testEndTwoPredecessors");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            addResourceDependency(instance0, instance2);
            instance2.addDataPredecessor(instance1);
            instance0.tryToLaunch();
            completed(instance0);
            checkExecutions(new int[] { 1, 0, 0, 0 });
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[] { 1, 1, 1, 0 });
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
         * 1 2 3
         * \ | / 
         *  \|/ 
         *   4 
         * /\ / \ 
         *  5 6 
         *   |
         *   7
         */
        try {
            prepare(7);
            AllocatableAction<P, T> task1 = new AllocatableActionImpl<>(0);
            task1.assignResources(r);
            AllocatableAction<P, T> task2 = new AllocatableActionImpl<>(1);
            task2.assignResources(r);
            AllocatableAction<P, T> task3 = new AllocatableActionImpl<>(2);
            task3.assignResources(r);
            AllocatableAction<P, T> task4 = new AllocatableActionImpl<>(3);
            task4.assignResources(r);
            task4.addDataPredecessor(task1);
            task4.addDataPredecessor(task2);
            task4.addDataPredecessor(task3);
            AllocatableAction<P, T> task5 = new AllocatableActionImpl<>(4);
            task5.assignResources(r);
            task5.addDataPredecessor(task4);
            AllocatableAction<P, T> task6 = new AllocatableActionImpl<>(5);
            task6.assignResources(r);
            task6.addDataPredecessor(task4);
            AllocatableAction<P, T> task7 = new AllocatableActionImpl<>(6);
            task7.assignResources(r);
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
            checkExecutions(new int[] { 1, 1, 0, 0, 0, 0, 0 });
            completed(task1);
            ;
            checkExecutions(new int[] { 1, 1, 1, 0, 0, 0, 0 });
            completed(task2);
            ;
            checkExecutions(new int[] { 1, 1, 1, 0, 0, 0, 0 });
            completed(task3);
            ;
            checkExecutions(new int[] { 1, 1, 1, 1, 0, 0, 0 });
            completed(task4);
            ;
            checkExecutions(new int[] { 1, 1, 1, 1, 1, 1, 0 });
            completed(task5);
            ;
            checkExecutions(new int[] { 1, 1, 1, 1, 1, 1, 0 });
            completed(task6);
            ;
            checkExecutions(new int[] { 1, 1, 1, 1, 1, 1, 1 });
            completed(task7);
            ;
            checkExecutions(new int[] { 1, 1, 1, 1, 1, 1, 1 });
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
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance0.assignResources(r);
            instance1.assignResources(r);
            instance0.tryToLaunch();
            error(instance0);
            checkExecutions(new int[] { 1, 0 });
            checkErrors(new int[] { 1, 0 });
            checkFailed(new int[] { 0, 0 });
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testOneFail() {
        System.out.println("testOneFail");
        try {
            prepare(2);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance0.assignResources(r);
            instance1.assignResources(r);
            instance0.tryToLaunch();
            error(instance0);
            checkExecutions(new int[] { 1, 0 });
            checkErrors(new int[] { 1, 0 });
            checkFailed(new int[] { 0, 0 });

            instance0.assignResources(r);
            instance0.tryToLaunch();
            error(instance0);
            checkExecutions(new int[] { 2, 0 });
            checkErrors(new int[] { 2, 0 });
            checkFailed(new int[] { 1, 0 });
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testBasicErrorDependencies() {
        System.out.println("testBasicErrorDependencies");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            instance1.addDataPredecessor(instance0);
            instance2.addDataPredecessor(instance0);

            addResourceDependency(instance0, instance1);
            addResourceDependency(instance0, instance2);
            addResourceDependency(instance0, instance3);
            instance0.tryToLaunch();
            checkExecutions(new int[] { 1, 0, 0, 0 });
            checkErrors(new int[] { 0, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0 });
            error(instance0);
            checkExecutions(new int[] { 1, 0, 0, 1 });
            checkErrors(new int[] { 1, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0 });

            instance0.assignResources(r);
            addResourceDependency(instance3, instance0);
            completed(instance3);
            checkExecutions(new int[] { 2, 0, 0, 1 });
            checkErrors(new int[] { 1, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0 });

            completed(instance0);
            checkExecutions(new int[] { 2, 1, 1, 1 });
            checkErrors(new int[] { 1, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0 });

        } catch (Throwable e) {
            e.printStackTrace(System.out);
            fail(e.getMessage());
        }
    }

    private void testBasicFailDependencies() {
        System.out.println("testBasicFailDependencies");
        try {
            prepare(4);
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            instance2.addDataPredecessor(instance0);

            addResourceDependency(instance0, instance1);
            addResourceDependency(instance1, instance2);
            addResourceDependency(instance2, instance3);
            instance0.tryToLaunch();
            checkExecutions(new int[] { 1, 0, 0, 0 });
            checkErrors(new int[] { 0, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0 });
            error(instance0);
            checkExecutions(new int[] { 1, 1, 0, 0 });
            checkErrors(new int[] { 1, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0 });

            instance0.assignResources(r);
            instance0.tryToLaunch();
            checkExecutions(new int[] { 2, 1, 0, 0 });
            checkErrors(new int[] { 1, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0 });

            error(instance0);
            checkExecutions(new int[] { 2, 1, 0, 0 });
            checkErrors(new int[] { 2, 0, 0, 0 });
            checkFailed(new int[] { 1, 0, 1, 0 });
            completed(instance1);
            checkExecutions(new int[] { 2, 1, 0, 1 });
            checkErrors(new int[] { 2, 0, 0, 0 });
            checkFailed(new int[] { 1, 0, 1, 0 });
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
             * Data dependencies: 1-->4 
             * Graph execution 
             *   2   5 
             *  / \ / 
             * 1~~~4 
             *  \ / \ 
             *   3   6
             */
            AllocatableAction<P, T> instance0 = new AllocatableActionImpl<>(0);
            instance0.assignResources(r);
            AllocatableAction<P, T> instance1 = new AllocatableActionImpl<>(1);
            instance1.assignResources(r);
            AllocatableAction<P, T> instance2 = new AllocatableActionImpl<>(2);
            instance2.assignResources(r);
            AllocatableAction<P, T> instance3 = new AllocatableActionImpl<>(3);
            instance3.assignResources(r);
            AllocatableAction<P, T> instance4 = new AllocatableActionImpl<>(4);
            instance4.assignResources(r);
            AllocatableAction<P, T> instance5 = new AllocatableActionImpl<>(5);
            instance5.assignResources(r);
            instance3.addDataPredecessor(instance0);
            addResourceDependency(instance0, instance1);
            addResourceDependency(instance0, instance2);
            addResourceDependency(instance1, instance3);
            addResourceDependency(instance2, instance3);
            addResourceDependency(instance3, instance4);
            addResourceDependency(instance3, instance5);
            instance0.tryToLaunch();
            checkExecutions(new int[] { 1, 0, 0, 0, 0, 0 });
            checkErrors(new int[] { 0, 0, 0, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0, 0, 0 });
            error(instance0);
            checkExecutions(new int[] { 1, 1, 1, 0, 0, 0 });
            checkErrors(new int[] { 1, 0, 0, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0, 0, 0 });
            instance0.assignResources(r);
            instance0.tryToLaunch();
            checkExecutions(new int[] { 2, 1, 1, 0, 0, 0 });
            checkErrors(new int[] { 1, 0, 0, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0, 0, 0 });
            error(instance0);
            checkExecutions(new int[] { 2, 1, 1, 0, 0, 0 });
            checkErrors(new int[] { 2, 0, 0, 0, 0, 0 });
            checkFailed(new int[] { 1, 0, 0, 1, 0, 0 });
            completed(instance1);
            checkExecutions(new int[] { 2, 1, 1, 0, 0, 0 });
            checkErrors(new int[] { 2, 0, 0, 0, 0, 0 });
            checkFailed(new int[] { 1, 0, 0, 1, 0, 0 });
            completed(instance2);
            checkExecutions(new int[] { 2, 1, 1, 0, 1, 1 });
            checkErrors(new int[] { 2, 0, 0, 0, 0, 0 });
            checkFailed(new int[] { 1, 0, 0, 1, 0, 0 });
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
    public void completed(AllocatableAction<P, T> action)
            throws BlockedActionException, UnassignedActionException, InvalidSchedulingException {
        ResourceScheduler<P, T> resource = action.getAssignedResource();
        LinkedList<AllocatableAction<P, T>> dataFree = action.completed();
        LinkedList<AllocatableAction<P, T>> resourceFree = resource.unscheduleAction(action);
        HashSet<AllocatableAction<P, T>> freeTasks = new HashSet<AllocatableAction<P, T>>();
        freeTasks.addAll(dataFree);
        freeTasks.addAll(resourceFree);
        for (AllocatableAction<P, T> a : freeTasks) {
            a.tryToLaunch();
        }
    }

    public void error(AllocatableAction<P, T> action) throws BlockedActionException, UnassignedActionException, InvalidSchedulingException {
        ResourceScheduler<P, T> resource = action.getAssignedResource();
        LinkedList<AllocatableAction<P, T>> resourceFree;
        try {
            action.error();
            resourceFree = resource.unscheduleAction(action);
        } catch (FailedActionException fae) {
            resourceFree = new LinkedList<AllocatableAction<P, T>>();
            for (AllocatableAction<P, T> failed : action.failed()) {
                resourceFree.addAll(resource.unscheduleAction(failed));
            }
        }
        for (AllocatableAction<P, T> a : resourceFree) {
            a.tryToLaunch();
        }

    }

    /*---------------------------------------------------------
     ----------------------------------------------------------
     ----------------------- TEST CHECKERS --------------------
     ----------------------------------------------------------
     --------------------------------------------------------*/

    private void prepare(int size) {
        AllocatableActionImpl.executions = new int[size];
        AllocatableActionImpl.error = new int[size];
        AllocatableActionImpl.failed = new int[size];
    }

    private void checkExecutions(int[] pattern) {
        if (pattern.length != AllocatableActionImpl.executions.length) {
            fail("Unconsistent execution arrays. " + AllocatableActionImpl.executions.length + " results obtained and " + pattern.length
                    + " expected");
            return;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != AllocatableActionImpl.executions[i]) {
                fail("AllocatableAction " + i + " should be executed " + pattern[i] + " time and it was "
                        + AllocatableActionImpl.executions[i]);
            }
        }
    }

    private void checkErrors(int[] pattern) {
        if (pattern.length != AllocatableActionImpl.error.length) {
            fail("Unconsistent AllocatableActionImpl.error arrays. " + AllocatableActionImpl.error.length + " results obtained and "
                    + pattern.length + " expected");
            return;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != AllocatableActionImpl.error[i]) {
                fail("AllocatableAction " + i + " should had " + pattern[i] + " AllocatableActionImpl.error and it was "
                        + AllocatableActionImpl.error[i]);
            }
        }
    }

    private void checkFailed(int[] pattern) {
        if (pattern.length != AllocatableActionImpl.failed.length) {
            fail("Unconsistent AllocatableActionImpl.failed arrays. " + AllocatableActionImpl.failed.length + " results obtained and "
                    + pattern.length + " expected");
            return;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != AllocatableActionImpl.failed[i]) {
                fail("AllocatableAction " + i + " should had AllocatableActionImpl.failed " + pattern[i] + " time and it was "
                        + AllocatableActionImpl.failed[i]);

            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void addResourceDependency(AllocatableAction<?, ?> pred, AllocatableAction<?, ?> succ) {
        ResourceDependencies<?,?> dsiPred = (ResourceDependencies<?,?>) pred.getSchedulingInfo();
        ResourceDependencies<?,?> dsiSucc = (ResourceDependencies<?,?>) succ.getSchedulingInfo();
        if (pred.isPending()) {
            ((ResourceDependencies) dsiSucc).addPredecessor(pred);
            ((ResourceDependencies) dsiPred).addSuccessor(succ);
        }
    }


    public static class ResourceDependencies<P extends Profile, T extends WorkerResourceDescription> extends SchedulingInformation<P, T> {

        // Allocatable actions that the action depends on due to resource availability
        private final LinkedList<AllocatableAction<P, T>> resourcePredecessors;

        // Allocatable actions depending on the allocatable action due to resource availability
        private final LinkedList<AllocatableAction<P, T>> resourceSuccessors;


        public ResourceDependencies() {
            resourcePredecessors = new LinkedList<AllocatableAction<P, T>>();
            resourceSuccessors = new LinkedList<AllocatableAction<P, T>>();
        }

        public void addPredecessor(AllocatableAction<P, T> predecessor) {
            resourcePredecessors.add(predecessor);
        }

        public boolean hasPredecessors() {
            return !resourcePredecessors.isEmpty();
        }

        @Override
        public final boolean isExecutable() {
            return resourcePredecessors.isEmpty();
        }

        public LinkedList<AllocatableAction<P, T>> getPredecessors() {
            return resourcePredecessors;
        }

        public void removePredecessor(AllocatableAction<P, T> successor) {
            resourcePredecessors.remove(successor);
        }

        public void clearPredecessors() {
            resourcePredecessors.clear();
        }

        public void addSuccessor(AllocatableAction<P, T> successor) {
            resourceSuccessors.add(successor);
        }

        public LinkedList<AllocatableAction<P, T>> getSuccessors() {
            return resourceSuccessors;
        }

        public synchronized void removeSuccessor(AllocatableAction<P, T> successor) {
            resourceSuccessors.remove(successor);
        }

        public void clearSuccessors() {
            resourceSuccessors.clear();
        }
    }

}
