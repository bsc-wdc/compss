/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.components.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import es.bsc.compss.components.ResourceUser;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.fake.FakeWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.types.resources.updates.PerformedIncrease;
import es.bsc.compss.types.resources.updates.ResourceUpdate;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.worker.COMPSsException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkerStartupRetryTest {

    private static final Logger LOGGER = LogManager.getLogger("Console");

    private static MethodResourceDescription description;

    private TrackingTaskScheduler ts;

    private static final ResourceUser DUMMY_RESOURCE_USER = new ResourceUser() {

        @Override
        public <T extends WorkerResourceDescription> void updatedResource(Worker<T> r, ResourceUpdate<T> mod) {
        }

        @Override
        public <T extends WorkerResourceDescription> void restartedResource(Worker<T> r, ResourceUpdate<T> mod) {
        }
    };


    @BeforeClass
    public static void setUpClass() {
        Processor p = new Processor();
        p.setComputingUnits(2);
        description = new MethodResourceDescription();
        description.addProcessor(p);
    }

    @Before
    public void setUp() {
        ResourceManager.clear(DUMMY_RESOURCE_USER);
        AtomicReference<TaskScheduler> tsRef = new AtomicReference<>();
        ActionOrchestrator orchestrator = new ActionOrchestrator() {

            @Override
            public void actionRunning(AllocatableAction action) {
                tsRef.get().actionRunning(action);
            }

            @Override
            public void actionCompletion(AllocatableAction action) {
                tsRef.get().actionCompleted(action);
            }

            @Override
            public void actionError(AllocatableAction action) {
                tsRef.get().errorOnAction(action);
            }

            @Override
            public void actionException(AllocatableAction action, COMPSsException e) {
                tsRef.get().exceptionOnAction(action, e);
            }
        };
        ts = new TrackingTaskScheduler(orchestrator);
        tsRef.set(ts);
    }

    @After
    public void tearDown() {
        ts.shutdown(new TaskScheduler.ShutdownListener() {

            @Override
            public void onShutdown() {
            }
        });
    }

    @Test
    public void testFirstAttemptSucceeds() throws InterruptedException {
        LOGGER.info("testFirstAttemptSucceeds");
        FailingWorker worker = new FailingWorker(description, 2, 0);
        ts.updateWorker(worker, new PerformedIncrease<>(description));
        worker.awaitCompletion();
        assertFalse("Worker should not be removed after a successful first start", ts.wasRemoved(worker));
        assertEquals("start() should be called exactly once", 1, worker.getStartCallCount());
    }

    @Test
    public void testRetryAfterOneFailure() throws InterruptedException {
        LOGGER.info("testRetryAfterOneFailure");
        FailingWorker worker = new FailingWorker(description, 2, 1);
        ts.updateWorker(worker, new PerformedIncrease<>(description));
        worker.awaitCompletion();
        assertFalse("Worker should not be removed after recovering on first retry", ts.wasRemoved(worker));
        assertEquals("start() should be called twice (1 failure + 1 success)", 2, worker.getStartCallCount());
    }

    @Test
    public void testRetryAfterTwoFailures() throws InterruptedException {
        LOGGER.info("testRetryAfterTwoFailures");
        FailingWorker worker = new FailingWorker(description, 2, 2);
        ts.updateWorker(worker, new PerformedIncrease<>(description));
        worker.awaitCompletion();
        assertFalse("Worker should not be removed after recovering on third attempt", ts.wasRemoved(worker));
        assertEquals("start() should be called 3 times (2 failures + 1 success)", 3, worker.getStartCallCount());
    }

    @Test
    public void testPermanentRemovalAfterThreeFailures() throws InterruptedException {
        LOGGER.info("testPermanentRemovalAfterThreeFailures");
        FailingWorker worker = new FailingWorker(description, 2, 3);
        ts.updateWorker(worker, new PerformedIncrease<>(description));
        worker.awaitCompletion();
        assertTrue("Worker should be permanently removed after 3 consecutive startup failures", ts.wasRemoved(worker));
        assertEquals("start() should be called exactly 3 times", 3, worker.getStartCallCount());
    }

    // ------------------------------------------------------------------
    // Test infrastructure
    // ------------------------------------------------------------------


    /**
     * TaskScheduler subclass that records which workers have been permanently removed.
     */
    private static class TrackingTaskScheduler extends TaskScheduler {

        private final Set<Worker<?>> removedWorkers = new HashSet<>();


        TrackingTaskScheduler(ActionOrchestrator orchestrator) {
            super(orchestrator);
        }

        @Override
        protected <T extends WorkerResourceDescription> void workerRemoved(ResourceScheduler<T> resource) {
            super.workerRemoved(resource);
            removedWorkers.add(resource.getResource());
        }

        boolean wasRemoved(Worker<?> worker) {
            return removedWorkers.contains(worker);
        }
    }

    /**
     * Worker whose start() throws InitNodeException for the first {@code failCount} attempts, then succeeds. Provides
     * {@link #awaitCompletion()} to synchronise the test thread with the last startup thread.
     */
    private static class FailingWorker extends FakeWorker {

        private final int failCount;
        private final AtomicInteger callCount = new AtomicInteger(0);
        // The call index on which the latch fires: last expected call (success or 3rd failure)
        private final int finalCallIndex;
        private final CountDownLatch doneLatch = new CountDownLatch(1);
        private volatile Thread lastStartThread;


        FailingWorker(MethodResourceDescription desc, int limitOfTasks, int failCount) {
            super(desc, limitOfTasks);
            this.failCount = failCount;
            // failCount < 3 → latch fires on the success call (failCount + 1)
            // failCount >= 3 → latch fires on the 3rd failure (permanent removal)
            this.finalCallIndex = failCount < 3 ? failCount + 1 : 3;
        }

        @Override
        public void start() throws InitNodeException {
            lastStartThread = Thread.currentThread();
            int call = callCount.incrementAndGet();
            if (call == finalCallIndex) {
                doneLatch.countDown();
            }
            if (call <= failCount) {
                throw new InitNodeException("simulated failure on attempt " + call);
            }
        }

        /**
         * Blocks until the final startup attempt has been made and all downstream processing in that thread (error
         * notification, strike check, possible removal) has completed.
         */
        void awaitCompletion() throws InterruptedException {
            assertTrue("start() was never called the expected number of times within timeout",
                doneLatch.await(5, TimeUnit.SECONDS));
            Thread t = lastStartThread;
            if (t != null) {
                t.join(5_000);
            }
        }

        int getStartCallCount() {
            return callCount.get();
        }
    }
}
