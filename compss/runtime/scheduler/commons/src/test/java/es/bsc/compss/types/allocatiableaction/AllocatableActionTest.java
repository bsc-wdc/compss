package es.bsc.compss.types.allocatiableaction;

import es.bsc.compss.components.ResourceUser;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.InvalidSchedulingException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.fake.FakeActionOrchestrator;
import es.bsc.compss.types.fake.FakeAllocatableAction;
import es.bsc.compss.types.fake.FakeResourceScheduler;
import es.bsc.compss.types.fake.FakeSI;
import es.bsc.compss.types.fake.FakeWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.types.resources.updates.ResourceUpdate;
import es.bsc.compss.util.ResourceManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public class AllocatableActionTest {

    // Test Logger
    private static final Logger LOGGER = LogManager.getLogger("Console");

    private static MethodResourceDescription description;
    private static TaskScheduler ts;
    private static FakeActionOrchestrator fao;
    private static FakeResourceScheduler rs;

    private static ResourceUser rus = new ResourceUser() {

        @Override
        public <T extends WorkerResourceDescription> void updatedResource(Worker<T> r, ResourceUpdate<T> modification) {

        }
    };


    @BeforeClass
    public static void setUpClass() {
        ResourceManager.clear(rus);

        // Method resource description and its slots
        int maxSlots = 3;
        Processor p = new Processor();
        p.setComputingUnits(maxSlots);
        description = new MethodResourceDescription();
        description.addProcessor(p);

        // Task Scheduler
        ts = new TaskScheduler();

        // Task Dispatcher
        fao = new FakeActionOrchestrator(ts);
        ts.setOrchestrator(fao);

        // Resource Scheduler
        rs = new FakeResourceScheduler(new FakeWorker(description, maxSlots), null, null);
    }

    @AfterClass
    public static void tearDownClass() {
        ts.shutdown();
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
        LOGGER.info("testEndNoSuccessors");
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
            checkExecutions(new int[] { 1, 0, 0, 0 });
        } catch (Exception e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testEndDataSuccessors() {
        LOGGER.info("testEndDataSuccessors");
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
            checkExecutions(new int[] { 1, 1, 0, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testEndResourceSuccessors() {
        LOGGER.info("testEndResourceSuccessors");
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
            checkExecutions(new int[] { 1, 1, 0, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testEndTwoDataSuccessors() {
        LOGGER.info("testEndTwoDataSuccessors");
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
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testEndTwoResourceSuccessors() {
        LOGGER.info("testEndTwoDataSuccessors");
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
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testEndDataAndResourceSuccessors() {
        LOGGER.info("testEndDataAndResourceSuccessors");
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
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testEndResourceAndDataSuccessors() {
        LOGGER.info("testEndDataAndResourceSuccessors");
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
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testEndTwoDataPredecessors() {
        LOGGER.info("testEndTwoPredecessors");
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
            checkExecutions(new int[] { 1, 0, 0, 0 });
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testEndTwoResourcesPredecessors() {
        LOGGER.info("testEndTwoResourcesPredecessors");
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
            checkExecutions(new int[] { 1, 0, 0, 0 });
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testEndDataAndResourcesPredecessors() {
        LOGGER.info("testEndTwoPredecessors");
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
            checkExecutions(new int[] { 1, 0, 0, 0 });
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testEndResourcesAndPredecessors() {
        LOGGER.info("testEndTwoPredecessors");
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
            checkExecutions(new int[] { 1, 0, 0, 0 });
            instance1.tryToLaunch();
            completed(instance1);
            checkExecutions(new int[] { 1, 1, 1, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testComplexGraph() {
        LOGGER.info("testComplexGraph");
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
            checkExecutions(new int[] { 1, 1, 0, 0, 0, 0, 0 });
            completed(task1);
            checkExecutions(new int[] { 1, 1, 1, 0, 0, 0, 0 });
            completed(task2);
            checkExecutions(new int[] { 1, 1, 1, 0, 0, 0, 0 });
            completed(task3);
            checkExecutions(new int[] { 1, 1, 1, 1, 0, 0, 0 });
            completed(task4);
            checkExecutions(new int[] { 1, 1, 1, 1, 1, 1, 0 });
            completed(task5);
            checkExecutions(new int[] { 1, 1, 1, 1, 1, 1, 0 });
            completed(task6);
            checkExecutions(new int[] { 1, 1, 1, 1, 1, 1, 1 });
            completed(task7);
            checkExecutions(new int[] { 1, 1, 1, 1, 1, 1, 1 });
        } catch (Throwable e) {
            LOGGER.error(e);
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
        LOGGER.info("testOneError");
        try {
            prepare(2);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance0.assignResource(rs);
            instance1.assignResource(rs);
            instance0.tryToLaunch();
            error(instance0);
            checkExecutions(new int[] { 1, 0 });
            checkErrors(new int[] { 1, 0 });
            checkFailed(new int[] { 0, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testOneFail() {
        LOGGER.info("testOneFail");
        try {
            prepare(2);
            FakeAllocatableAction instance0 = new FakeAllocatableAction(fao, 0);
            FakeAllocatableAction instance1 = new FakeAllocatableAction(fao, 1);
            instance0.assignResource(rs);
            instance1.assignResource(rs);
            instance0.tryToLaunch();
            error(instance0);
            checkExecutions(new int[] { 1, 0 });
            checkErrors(new int[] { 1, 0 });
            checkFailed(new int[] { 0, 0 });

            instance0.assignResource(rs);
            instance0.tryToLaunch();
            error(instance0);
            checkExecutions(new int[] { 2, 0 });
            checkErrors(new int[] { 2, 0 });
            checkFailed(new int[] { 1, 0 });
        } catch (Throwable e) {
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testBasicErrorDependencies() {
        LOGGER.info("testBasicErrorDependencies");
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
            checkExecutions(new int[] { 1, 0, 0, 0 });
            checkErrors(new int[] { 0, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0 });
            error(instance0);
            checkExecutions(new int[] { 1, 0, 0, 1 });
            checkErrors(new int[] { 1, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0 });

            instance0.assignResource(rs);
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
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testBasicFailDependencies() {
        LOGGER.info("testBasicFailDependencies");
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
            checkExecutions(new int[] { 1, 0, 0, 0 });
            checkErrors(new int[] { 0, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0 });
            error(instance0);
            checkExecutions(new int[] { 1, 1, 0, 0 });
            checkErrors(new int[] { 1, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0 });

            instance0.assignResource(rs);
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
            LOGGER.error(e);
            fail(e.getMessage());
        }
    }

    private void testFailDependencies() {
        LOGGER.info("testFailDependencies");
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
            checkExecutions(new int[] { 1, 0, 0, 0, 0, 0 });
            checkErrors(new int[] { 0, 0, 0, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0, 0, 0 });
            error(instance0);
            checkExecutions(new int[] { 1, 1, 1, 0, 0, 0 });
            checkErrors(new int[] { 1, 0, 0, 0, 0, 0 });
            checkFailed(new int[] { 0, 0, 0, 0, 0, 0 });
            instance0.assignResource(rs);
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
            LOGGER.error(e);
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
        List<AllocatableAction> dataFree = action.completed();
        List<AllocatableAction> resourceFree = resource.unscheduleAction(action);
        Set<AllocatableAction> freeTasks = new HashSet<>();
        freeTasks.addAll(dataFree);
        freeTasks.addAll(resourceFree);

        for (AllocatableAction a : freeTasks) {
            FakeAllocatableAction fa = (FakeAllocatableAction) a;
            fa.tryToLaunch();
        }
    }

    public void error(FakeAllocatableAction action) throws BlockedActionException, UnassignedActionException, InvalidSchedulingException {
        FakeResourceScheduler resource = (FakeResourceScheduler) action.getAssignedResource();
        List<AllocatableAction> resourceFree;
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
