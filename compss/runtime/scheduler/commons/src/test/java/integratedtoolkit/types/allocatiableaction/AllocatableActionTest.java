package integratedtoolkit.types.allocatiableaction;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.Score;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.fake.FakeWorker;
import integratedtoolkit.types.resources.Worker;

import java.util.HashSet;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class AllocatableActionTest {

    private final ResourceScheduler<?> r = new DummyResourceScheduler(new FakeWorker());
    private int[] executions;
    private int[] error;
    private int[] failed;

    public AllocatableActionTest() {

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
        //Create one instance 
        prepare(1);
        AllocatableAction instance = new AllocatableActionImpl(0);
        //Run it
        instance.assignResource(r);
        instance.tryToLaunch();
        //Check if it was executed
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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
        /* Data dependency Graph
         *
         * 1  2  3
         *  \ | /
         *   \|/
         *    4
         *   /\
         *  /  \
         * 5    6
         *      |
         *      7
         *
         */
        try {
            prepare(7);
            AllocatableAction task1 = new AllocatableActionImpl(0);
            task1.assignResource(r);
            AllocatableAction task2 = new AllocatableActionImpl(1);
            task2.assignResource(r);
            AllocatableAction task3 = new AllocatableActionImpl(2);
            task3.assignResource(r);
            AllocatableAction task4 = new AllocatableActionImpl(3);
            task4.assignResource(r);
            task4.addDataPredecessor(task1);
            task4.addDataPredecessor(task2);
            task4.addDataPredecessor(task3);
            AllocatableAction task5 = new AllocatableActionImpl(4);
            task5.assignResource(r);
            task5.addDataPredecessor(task4);
            AllocatableAction task6 = new AllocatableActionImpl(5);
            task6.assignResource(r);
            task6.addDataPredecessor(task4);
            AllocatableAction task7 = new AllocatableActionImpl(6);
            task7.assignResource(r);
            task7.addDataPredecessor(task4);

            /*Scheduling
             * Resource 1 executes 1, 3, 4 and 5
             * Resource 2 executes 2, 6 and 7
             */
            //Resource 1
            addResourceDependency(task1, task3);
            addResourceDependency(task3, task4);
            addResourceDependency(task4, task5);
            //Resource 2 
            addResourceDependency(task2, task6);
            addResourceDependency(task6, task7);

            task1.tryToLaunch();
            task2.tryToLaunch();
            checkExecutions(new int[]{1, 1, 0, 0, 0, 0, 0});
            completed(task1);;
            checkExecutions(new int[]{1, 1, 1, 0, 0, 0, 0});
            completed(task2);;
            checkExecutions(new int[]{1, 1, 1, 0, 0, 0, 0});
            completed(task3);;
            checkExecutions(new int[]{1, 1, 1, 1, 0, 0, 0});
            completed(task4);;
            checkExecutions(new int[]{1, 1, 1, 1, 1, 1, 0});
            completed(task5);;
            checkExecutions(new int[]{1, 1, 1, 1, 1, 1, 0});
            completed(task6);;
            checkExecutions(new int[]{1, 1, 1, 1, 1, 1, 1});
            completed(task7);;
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance0.assignResource(r);
            instance1.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance0.assignResource(r);
            instance1.assignResource(r);
            instance0.tryToLaunch();
            error(instance0);
            checkExecutions(new int[]{1, 0});
            checkErrors(new int[]{1, 0});
            checkFailed(new int[]{0, 0});

            instance0.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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

            instance0.assignResource(r);
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
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
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

            instance0.assignResource(r);
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
             * Data dependencies: 1-->4
             * Graph execution
             *      2       5
             *    /   \   /
             *  1 ~~~~~ 4   
             *    \    /  \
             *      3       6
             */
            AllocatableAction instance0 = new AllocatableActionImpl(0);
            instance0.assignResource(r);
            AllocatableAction instance1 = new AllocatableActionImpl(1);
            instance1.assignResource(r);
            AllocatableAction instance2 = new AllocatableActionImpl(2);
            instance2.assignResource(r);
            AllocatableAction instance3 = new AllocatableActionImpl(3);
            instance3.assignResource(r);
            AllocatableAction instance4 = new AllocatableActionImpl(4);
            instance4.assignResource(r);
            AllocatableAction instance5 = new AllocatableActionImpl(5);
            instance5.assignResource(r);
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
            instance0.assignResource(r);
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
    public void completed(AllocatableAction action) throws BlockedActionException, UnassignedActionException, InvalidSchedulingException {
        ResourceScheduler<?> resource = action.getAssignedResource();
        LinkedList<AllocatableAction> dataFree = action.completed();
        LinkedList<AllocatableAction> resourceFree = resource.unscheduleAction(action);
        HashSet<AllocatableAction> freeTasks = new HashSet<AllocatableAction>();
        freeTasks.addAll(dataFree);
        freeTasks.addAll(resourceFree);
        for (AllocatableAction a : freeTasks) {
            a.tryToLaunch();
        }
    }

    public void error(AllocatableAction action) throws BlockedActionException, UnassignedActionException, InvalidSchedulingException {
        ResourceScheduler<?> resource = action.getAssignedResource();
        LinkedList<AllocatableAction> resourceFree;
        try {
            action.error();
            resourceFree = resource.unscheduleAction(action);
        } catch (FailedActionException fae) {
            resourceFree = new LinkedList<AllocatableAction>();
            for (AllocatableAction failed : action.failed()) {
                resourceFree.addAll(resource.unscheduleAction(failed));
            }
        }
        for (AllocatableAction a : resourceFree) {
            a.tryToLaunch();
        }

    }
    /*---------------------------------------------------------
     ----------------------------------------------------------
     ----------------------- TEST CHECKERS --------------------
     ----------------------------------------------------------
     --------------------------------------------------------*/

    private void prepare(int size) {
        executions = new int[size];
        error = new int[size];
        failed = new int[size];
    }

    private void checkExecutions(int[] pattern) {
        if (pattern.length != executions.length) {
            fail("Unconsistent execution arrays. " + executions.length + " results obtained and " + pattern.length + " expected");
            return;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != executions[i]) {
                fail("AllocatableAction " + i + " should be executed " + pattern[i] + " time and it was " + executions[i]);
            }
        }
    }

    private void checkErrors(int[] pattern) {
        if (pattern.length != error.length) {
            fail("Unconsistent error arrays. " + error.length + " results obtained and " + pattern.length + " expected");
            return;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != error[i]) {
                fail("AllocatableAction " + i + " should had " + pattern[i] + " error and it was " + error[i]);
            }
        }
    }

    private void checkFailed(int[] pattern) {
        if (pattern.length != failed.length) {
            fail("Unconsistent failed arrays. " + failed.length + " results obtained and " + pattern.length + " expected");
            return;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != failed[i]) {
                fail("AllocatableAction " + i + " should had failed " + pattern[i] + " time and it was " + failed[i]);

            }
        }
    }

    public static void addResourceDependency(AllocatableAction pred, AllocatableAction succ) {
        ResourceDependencies dsiPred = (ResourceDependencies) pred.getSchedulingInfo();
        ResourceDependencies dsiSucc = (ResourceDependencies) succ.getSchedulingInfo();
        if (pred.isPending()) {
            dsiSucc.addPredecessor(pred);
            dsiPred.addSuccessor(succ);
        }
    }

    public class DummyResourceScheduler extends ResourceScheduler {

        public DummyResourceScheduler(Worker<?> w) {
            super(w);
        }

        @Override
        public LinkedList<AllocatableAction> unscheduleAction(AllocatableAction action) {

            LinkedList<AllocatableAction> freeTasks = new LinkedList<AllocatableAction>();
            ResourceDependencies actionDSI = (ResourceDependencies) action.getSchedulingInfo();

            //Remove action from predecessors
            for (AllocatableAction pred : actionDSI.getPredecessors()) {
                ResourceDependencies predDSI = (ResourceDependencies) pred.getSchedulingInfo();
                predDSI.removeSuccessor(action);
            }

            for (AllocatableAction successor : actionDSI.getSuccessors()) {
                ResourceDependencies successorDSI = (ResourceDependencies) successor.getSchedulingInfo();
                //Remove predecessor
                successorDSI.removePredecessor(action);

                //Link with action predecessors
                for (AllocatableAction predecessor : actionDSI.getPredecessors()) {
                    ResourceDependencies predecessorDSI = (ResourceDependencies) predecessor.getSchedulingInfo();
                    if (predecessor.isPending()) {
                        successorDSI.addPredecessor(predecessor);
                        predecessorDSI.addSuccessor(successor);
                    }
                }
                //Check executability
                if (successorDSI.isExecutable()) {
                    freeTasks.add(successor);
                }
            }
            actionDSI.clearPredecessors();
            actionDSI.clearSuccessors();

            return freeTasks;
        }
    }

    public class AllocatableActionImpl extends AllocatableAction {

        private int id;

        public AllocatableActionImpl(int id) {
            super(new ResourceDependencies());
            this.id = id;
        }

        @Override
        public void doAction() {
            executions[id]++;
        }

        @Override
        public void doCompleted() {

        }

        @Override
        public void doError() throws FailedActionException {
            error[id]++;
            if (error[id] == 2) {
                throw new FailedActionException();
            }
        }

        @Override
        public void doFailed() {
            failed[id]++;
        }

        public String toString() {
            return "AllocatableAction " + id;
        }

        @Override
        public LinkedList<Implementation<?>> getCompatibleImplementations(ResourceScheduler<?> r) {
            return null;
        }

        @Override
        public LinkedList<ResourceScheduler<?>> getCompatibleWorkers() {
            return null;
        }

        @Override
        public Implementation<?>[] getImplementations() {
            return new Implementation[0];
        }

        @Override
        public boolean isCompatible(Worker<?> r) {
            return true;
        }

        @Override
        protected boolean areEnoughResources() {
            return true;
        }

        @Override
        protected void reserveResources() {

        }

        @Override
        protected void releaseResources() {

        }

        @Override
        public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

        }

        @Override
        public void schedule(ResourceScheduler<?> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {

        }

        @Override
        public Score schedulingScore(TaskScheduler TS) {
            return null;
        }

        @Override
        public Score schedulingScore(ResourceScheduler<?> targetWorker, Score actionScore) {
            return null;
        }

        @Override
        public Integer getCoreId() {
            return null;
        }

    }

    public static class ResourceDependencies extends SchedulingInformation {

        //Allocatable actions that the action depends on due to resource availability
        private final LinkedList<AllocatableAction> resourcePredecessors;

        //Allocatable actions depending on the allocatable action due to resource availability
        private final LinkedList<AllocatableAction> resourceSuccessors;

        public ResourceDependencies() {
            resourcePredecessors = new LinkedList<AllocatableAction>();
            resourceSuccessors = new LinkedList<AllocatableAction>();
        }

        public void addPredecessor(AllocatableAction predecessor) {
            resourcePredecessors.add(predecessor);
        }

        public boolean hasPredecessors() {
            return !resourcePredecessors.isEmpty();
        }

        @Override
        public final boolean isExecutable() {
            return resourcePredecessors.isEmpty();
        }

        public LinkedList<AllocatableAction> getPredecessors() {
            return resourcePredecessors;
        }

        public void removePredecessor(AllocatableAction successor) {
            resourcePredecessors.remove(successor);
        }

        public void clearPredecessors() {
            resourcePredecessors.clear();
        }

        public void addSuccessor(AllocatableAction successor) {
            resourceSuccessors.add(successor);
        }

        public LinkedList<AllocatableAction> getSuccessors() {
            return resourceSuccessors;
        }

        public synchronized void removeSuccessor(AllocatableAction successor) {
            resourceSuccessors.remove(successor);
        }

        public void clearSuccessors() {
            resourceSuccessors.clear();
        }
    }

}
