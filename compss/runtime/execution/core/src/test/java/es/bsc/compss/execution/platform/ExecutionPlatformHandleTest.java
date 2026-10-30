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
package es.bsc.compss.execution.platform;

import es.bsc.compss.execution.executor.Executor;
import es.bsc.compss.execution.executor.ExecutorContext;
import es.bsc.compss.execution.executor.binding.ExternalExecutor;
import es.bsc.compss.execution.invokers.test.utils.FakeInvocationContext;
import es.bsc.compss.execution.platform.mirrors.ExecutorContextMirror;
import es.bsc.compss.execution.platform.mirrors.MirrorFactory;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.types.execution.exceptions.InvalidMapException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the lazy handle lookup: the first lookup creates and registers the mirror through its factory and the
 * mirror caches the returned handle on the calling thread, further lookups reuse the cached handle while it can run,
 * stale handles are re-registered through the already registered mirror, concurrent first lookups create the mirror
 * exactly once, and stopping the platform clears the mirror registry so that the next lookup re-creates the mirror and
 * hands out a fresh handle. Each test uses its own platform, factory and executors, so no state leaks across tests.
 */
public class ExecutionPlatformHandleTest {

    @Test
    public void firstLookupCreatesRegistersAndCachesTheThreadHandle() {
        ExecutionPlatform platform = newPlatform();
        CountingFactory factory = new CountingFactory();
        Executor executor = newExecutor(platform, 0);

        FakeHandle handle = platform.getMirroredExecutor(executor, FakeMirror.class, factory);
        FakeHandle cachedHandle = platform.getMirroredExecutor(executor, FakeMirror.class, factory);

        Assert.assertTrue(handle.canRun());
        Assert.assertSame(handle, cachedHandle);
        Assert.assertSame(platform, factory.lastPlatform);
        Assert.assertEquals(1, factory.created);
        Assert.assertSame(factory.lastCreated, platform.getMirror(FakeMirror.class));
        Assert.assertEquals(1, factory.lastCreated.registrations.get());
    }

    @Test
    public void staleCachedHandleIsReRegisteredThroughTheSameMirror() {
        ExecutionPlatform platform = newPlatform();
        CountingFactory factory = new CountingFactory();
        Executor executor = newExecutor(platform, 0);
        FakeHandle firstHandle = platform.getMirroredExecutor(executor, FakeMirror.class, factory);
        firstHandle.terminated();

        FakeHandle secondHandle = platform.getMirroredExecutor(executor, FakeMirror.class, factory);

        Assert.assertFalse(firstHandle.canRun());
        Assert.assertTrue(secondHandle.canRun());
        Assert.assertNotSame(firstHandle, secondHandle);
        Assert.assertSame(firstHandle.getMirror(), secondHandle.getMirror());
        Assert.assertEquals(1, factory.created);
        Assert.assertEquals(2, factory.lastCreated.registrations.get());
    }

    @Test
    public void alreadyRegisteredMirrorIsReusedWithoutTheFactory() {
        ExecutionPlatform platform = newPlatform();
        CountingFactory factory = new CountingFactory();
        Executor executor = newExecutor(platform, 0);
        FakeMirror mirror = new FakeMirror();
        platform.registerMirror(FakeMirror.class, mirror);

        FakeHandle handle = platform.getMirroredExecutor(executor, FakeMirror.class, factory);

        Assert.assertTrue(handle.canRun());
        Assert.assertSame(mirror, handle.getMirror());
        Assert.assertEquals(0, factory.created);
        Assert.assertEquals(1, mirror.registrations.get());
    }

    @Test
    public void concurrentFirstLookupsCreateTheMirrorOnce() throws InterruptedException {
        ExecutionPlatform platform = newPlatform();
        CountingFactory factory = new CountingFactory();

        int lookups = 8;
        CyclicBarrier barrier = new CyclicBarrier(lookups);
        Thread[] threads = new Thread[lookups];
        for (int i = 0; i < lookups; i++) {
            final Executor executor = newExecutor(platform, i);
            threads[i] = new Thread(() -> {
                awaitBarrier(barrier);
                platform.getMirroredExecutor(executor, FakeMirror.class, factory);
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse("Lookup thread did not finish", thread.isAlive());
        }

        Assert.assertEquals(1, factory.created);
        Assert.assertSame(factory.lastCreated, platform.getMirror(FakeMirror.class));
        Assert.assertEquals(lookups, factory.lastCreated.registrations.get());
    }

    @Test
    public void stopClearsTheMirrorRegistryForFreshLookups() {
        ExecutionPlatform platform = newPlatform();
        platform.start();
        CountingFactory factory = new CountingFactory();
        Executor executor = newExecutor(platform, 0);
        FakeHandle staleHandle = platform.getMirroredExecutor(executor, FakeMirror.class, factory);

        platform.stop();

        FakeHandle freshHandle = platform.getMirroredExecutor(executor, FakeMirror.class, factory);
        Assert.assertNotSame(staleHandle, freshHandle);
        Assert.assertTrue(freshHandle.canRun());
        Assert.assertEquals(2, factory.created);
        Assert.assertSame(factory.lastCreated, platform.getMirror(FakeMirror.class));
    }

    /**
     * Creates an execution platform with no worker threads and disabled resource binders, so the tests only exercise
     * the lazy handle lookup.
     *
     * @return new idle execution platform
     */
    private static ExecutionPlatform newPlatform() {
        try {
            return new ExecutionPlatform("handle test platform", new FakeInvocationContext.Builder().build(),
                new ExecutionPlatformConfiguration(0, true), new ResourceManager(0, ThreadBinder.BINDER_DISABLED, 0,
                    ThreadBinder.BINDER_DISABLED, 0, ThreadBinder.BINDER_DISABLED));
        } catch (InvalidMapException e) {
            throw new IllegalStateException("Could not create the execution platform of the tests", e);
        }
    }

    /**
     * Creates an executor bound to the given platform.
     *
     * @param platform platform running the executor
     * @param id executor identifier
     * @return new executor bound to the platform
     */
    private static Executor newExecutor(ExecutorContext platform, int id) {
        return new Executor(new FakeInvocationContext.Builder().build(), platform, id, "executor" + id);
    }

    /**
     * Waits on the given barrier, restoring the interrupt flag if interrupted.
     *
     * @param barrier barrier separating concurrent lookups
     */
    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (BrokenBarrierException e) {
            // Nothing to do: the other parties left the barrier, proceed to the lookup
        }
    }


    /**
     * Executor handle of the tests: only carries the mirror that registered it.
     */
    private static final class FakeHandle extends ExternalExecutor {

        private final FakeMirror mirror;


        FakeHandle(FakeMirror mirror) {
            this.mirror = mirror;
        }

        @Override
        public ExecutorContextMirror<?> getMirror() {
            return this.mirror;
        }
    }

    /**
     * Mirror of the tests: counts the handle registrations and caches the current thread's handle the same way the
     * production mirrors do.
     */
    private static final class FakeMirror implements ExecutorContextMirror<FakeHandle> {

        private final AtomicInteger registrations = new AtomicInteger();

        // Handle of the current thread (lazily registered)
        private final ThreadLocal<FakeHandle> threadHandle = new ThreadLocal<>();


        @Override
        public FakeHandle getMirroredExecutor(Executor executor) {
            FakeHandle handle = this.threadHandle.get();
            if (handle == null || !handle.canRun()) {
                handle = registerExecutor(executor);
                this.threadHandle.set(handle);
            }
            return handle;
        }

        @Override
        public FakeHandle registerExecutor(Executor executor) {
            this.registrations.incrementAndGet();
            FakeHandle handle = new FakeHandle(this);
            handle.deployed();
            return handle;
        }

        @Override
        public void unregisterExecutor(Executor executor) {
            // Nothing to do
        }

        @Override
        public void stop() {
            // Nothing to do
        }

        @Override
        public void cancelJob(FakeHandle handle) {
            // Nothing to do
        }
    }

    /**
     * Mirror factory of the tests: counts the created mirrors and records the last platform and mirror received.
     */
    private static final class CountingFactory implements MirrorFactory<FakeMirror> {

        private int created;
        private ExecutorContext lastPlatform;
        private FakeMirror lastCreated;


        @Override
        public FakeMirror create(ExecutorContext platform) {
            this.created++;
            this.lastPlatform = platform;
            this.lastCreated = new FakeMirror();
            return this.lastCreated;
        }
    }
}
