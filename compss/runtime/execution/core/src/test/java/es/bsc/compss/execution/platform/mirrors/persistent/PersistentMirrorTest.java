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
package es.bsc.compss.execution.platform.mirrors.persistent;

import es.bsc.compss.execution.Invocation;
import es.bsc.compss.execution.InvocationContext;
import es.bsc.compss.execution.executor.Executor;
import es.bsc.compss.execution.executor.ExecutorContext;
import es.bsc.compss.execution.executor.InvocationRunner;
import es.bsc.compss.execution.executor.binding.ExternalExecutor;
import es.bsc.compss.execution.executor.binding.persistent.PersistentExecutor;
import es.bsc.compss.execution.invokers.Invoker;
import es.bsc.compss.execution.invokers.test.utils.FakeInvocationContext;
import es.bsc.compss.execution.platform.ExecutorRequest;
import es.bsc.compss.execution.platform.InvocationResources;
import es.bsc.compss.execution.platform.mirrors.ExecutorContextMirror;
import es.bsc.compss.execution.platform.mirrors.MirrorFactory;
import es.bsc.compss.types.resources.ResourceDescription;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the PersistentMirror registration wiring: registering an executor thread deploys its handle and
 * initializes the thread binding, and unregistering it invalidates the handle and finishes the binding. The native
 * binding initialization is replaced with a no-op so the real register/unregister paths run without the persistent
 * binding library.
 */
public class PersistentMirrorTest {

    /**
     * Creates a new mirror whose thread binding calls are counted no-ops.
     *
     * @return mirror under test
     */
    private static TestMirror newMirror() {
        return new TestMirror(new FakePlatform());
    }

    @Test
    public void registeredExecutorIsDeployedAndExposesItsCollaborators() {
        TestMirror mirror = newMirror();
        Executor executor =
            new Executor(new FakeInvocationContext.Builder().build(), new FakePlatform(), 0, "executor0");

        PersistentExecutor handle = mirror.registerExecutor(executor);

        Assert.assertTrue(handle.canRun());
        Assert.assertSame(mirror, handle.getMirror());
        Assert.assertSame(executor, handle.getOwningExecutor());
        Assert.assertEquals(1, mirror.startedBindings);
        Assert.assertEquals(0, mirror.finishedBindings);
    }

    @Test
    public void unregisteredExecutorIsInvalid() {
        TestMirror mirror = newMirror();
        Executor executor =
            new Executor(new FakeInvocationContext.Builder().build(), new FakePlatform(), 0, "executor0");
        PersistentExecutor handle = mirror.registerExecutor(executor);

        mirror.unregisterExecutor(executor);

        Assert.assertFalse(handle.canRun());
        Assert.assertEquals(1, mirror.startedBindings);
        Assert.assertEquals(1, mirror.finishedBindings);
    }

    @Test
    public void unregisteringUnknownExecutorIsNoOp() {
        TestMirror mirror = newMirror();
        Executor executor =
            new Executor(new FakeInvocationContext.Builder().build(), new FakePlatform(), 0, "executor0");

        mirror.unregisterExecutor(executor);

        Assert.assertEquals(0, mirror.startedBindings);
        Assert.assertEquals(0, mirror.finishedBindings);
    }

    @Test
    public void reRegistrationAfterUnregisterReturnsNewHandle() {
        TestMirror mirror = newMirror();
        Executor executor =
            new Executor(new FakeInvocationContext.Builder().build(), new FakePlatform(), 0, "executor0");
        PersistentExecutor firstHandle = mirror.registerExecutor(executor);
        mirror.unregisterExecutor(executor);

        PersistentExecutor secondHandle = mirror.registerExecutor(executor);

        Assert.assertNotSame(firstHandle, secondHandle);
        Assert.assertFalse(firstHandle.canRun());
        Assert.assertTrue(secondHandle.canRun());
        Assert.assertEquals(2, mirror.startedBindings);
        Assert.assertEquals(1, mirror.finishedBindings);
    }

    @Test
    public void concurrentUnregisterRequestsFinishTheBindingExactlyOnce() throws InterruptedException {
        TestMirror mirror = newMirror();
        Executor executor =
            new Executor(new FakeInvocationContext.Builder().build(), new FakePlatform(), 0, "executor0");
        PersistentExecutor handle = mirror.registerExecutor(executor);

        int requesters = 8;
        CyclicBarrier barrier = new CyclicBarrier(requesters);
        Thread[] threads = new Thread[requesters];
        for (int i = 0; i < requesters; i++) {
            threads[i] = new Thread(() -> {
                awaitBarrier(barrier);
                mirror.unregisterExecutor(executor);
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse("Unregister thread did not finish", thread.isAlive());
        }

        Assert.assertFalse(handle.canRun());
        Assert.assertEquals(1, mirror.startedBindings);
        Assert.assertEquals(1, mirror.finishedBindings);
    }

    @Test
    public void stopInvalidatesRemainingHandlesAndAllowsFreshRegistration() {
        TestMirror mirror = newMirror();
        Executor abnormalA =
            new Executor(new FakeInvocationContext.Builder().build(), new FakePlatform(), 0, "executor0");
        Executor abnormalB =
            new Executor(new FakeInvocationContext.Builder().build(), new FakePlatform(), 1, "executor1");
        PersistentExecutor handleA = mirror.registerExecutor(abnormalA);
        PersistentExecutor handleB = mirror.registerExecutor(abnormalB);

        mirror.stop();

        Assert.assertFalse(handleA.canRun());
        Assert.assertFalse(handleB.canRun());
        // Stopping cannot finish the bindings of other threads
        Assert.assertEquals(2, mirror.startedBindings);
        Assert.assertEquals(0, mirror.finishedBindings);

        PersistentExecutor freshHandle = mirror.registerExecutor(abnormalA);
        Assert.assertNotSame(handleA, freshHandle);
        Assert.assertTrue(freshHandle.canRun());
    }

    @Test
    public void stopAfterNormalUnregisterDoesNotFinishTheBindingAgain() {
        TestMirror mirror = newMirror();
        Executor executor =
            new Executor(new FakeInvocationContext.Builder().build(), new FakePlatform(), 0, "executor0");
        PersistentExecutor handle = mirror.registerExecutor(executor);
        mirror.unregisterExecutor(executor);

        mirror.stop();

        Assert.assertFalse(handle.canRun());
        Assert.assertEquals(1, mirror.startedBindings);
        Assert.assertEquals(1, mirror.finishedBindings);
    }

    @Test
    public void concurrentStopAndUnregisterFinishTheBindingAtMostOnce() throws InterruptedException {
        TestMirror mirror = newMirror();
        Executor executor =
            new Executor(new FakeInvocationContext.Builder().build(), new FakePlatform(), 0, "executor0");
        PersistentExecutor handle = mirror.registerExecutor(executor);

        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread stopper = new Thread(() -> {
            awaitBarrier(barrier);
            mirror.stop();
        });
        stopper.start();
        awaitBarrier(barrier);
        mirror.unregisterExecutor(executor);
        stopper.join(TimeUnit.SECONDS.toMillis(10));
        Assert.assertFalse("Stop thread did not finish", stopper.isAlive());

        Assert.assertFalse(handle.canRun());
        Assert.assertTrue(mirror.finishedBindings <= 1);
    }

    @Test
    public void cancelJobDoesNotAffectTheHandle() {
        TestMirror mirror = newMirror();
        Executor executor =
            new Executor(new FakeInvocationContext.Builder().build(), new FakePlatform(), 0, "executor0");
        PersistentExecutor handle = mirror.registerExecutor(executor);

        mirror.cancelJob(handle);

        Assert.assertTrue(handle.canRun());
        Assert.assertEquals(1, mirror.startedBindings);
        Assert.assertEquals(0, mirror.finishedBindings);
    }

    /**
     * Waits on the given barrier, restoring the interrupt flag if interrupted.
     *
     * @param barrier barrier separating concurrent mirror requests
     */
    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (BrokenBarrierException e) {
            // Nothing to do: the other party left the barrier, proceed to the request
        }
    }


    /**
     * PersistentMirror subclass replacing the native thread binding calls with counting no-ops.
     */
    private static final class TestMirror extends PersistentMirror {

        private int startedBindings;
        private int finishedBindings;


        TestMirror(ExecutorContext platform) {
            super(platform);
        }

        @Override
        void initThreadBinding() {
            this.startedBindings++;
        }

        @Override
        void finishThreadBinding() {
            this.finishedBindings++;
        }
    }

    /**
     * ExecutorContext fake returning a FakeInvocationContext and doing nothing else.
     */
    private static class FakePlatform implements ExecutorContext {

        @Override
        public InvocationContext getContext() {
            return new FakeInvocationContext.Builder().build();
        }

        @Override
        public ExecutorContextMirror<?> getMirror(Class<?> invoker) {
            return null;
        }

        @Override
        public void registerMirror(Class<?> invoker, ExecutorContextMirror<?> mirror) {
            // Nothing to do
        }

        @Override
        public <E extends ExternalExecutor, M extends ExecutorContextMirror<E>> E getMirroredExecutor(Executor executor,
            Class<M> mirrorClass, MirrorFactory<M> mirrorFactory) {
            // No lazy handle lookup runs through this fake
            throw new UnsupportedOperationException("Fake platform does not hand out handles");
        }

        @Override
        public int getSize() {
            return 1;
        }

        @Override
        public ExecutorRequest newThread() {
            return null;
        }

        @Override
        public ExecutorRequest getJob() {
            return null;
        }

        @Override
        public InvocationResources acquireResources(int jobId, ResourceDescription requirements,
            InvocationResources previousAllocation) {
            // Nothing to do
            return null;
        }

        @Override
        public void releaseResources(int jobId) {
            // Nothing to do
        }

        @Override
        public void registerRunningJob(Invocation invocation, Invoker invoker) {
            // Nothing to do
        }

        @Override
        public void blockedRunner(Invocation invocation, InvocationRunner runner,
            InvocationResources assignedResources) {
            // Nothing to do
        }

        @Override
        public void unblockedRunner(Invocation invocation, InvocationRunner runner,
            InvocationResources previousAllocation, Semaphore sem) {
            sem.release();
        }

        @Override
        public void unregisterRunningJob(int jobId) {
            // Nothing to do
        }

        @Override
        public void finishedExecutor(Executor executor) {
            // Nothing to do
        }
    }

}
