/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.executors;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.execution.types.ExecutorContext;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.InvocationRunner;
import es.bsc.compss.executor.external.ExecutionPlatformMirror;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.test.utils.ExecutionFlowVerifier;
import es.bsc.compss.invokers.test.utils.FakeInvocation;
import es.bsc.compss.invokers.test.utils.FakeInvocationContext;
import es.bsc.compss.types.execution.ExecutorRequest;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationExecutionRequest;
import es.bsc.compss.types.execution.StopExecutorRequest;
import es.bsc.compss.types.execution.exceptions.InvalidMapException;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.util.RequestQueue;

import java.util.Collection;
import java.util.HashMap;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;

import org.junit.Assert;
import org.junit.Test;


public class ExecutorTest {

    private final ExecutionFlowVerifier expectedEvents = new ExecutionFlowVerifier();


    @Test
    public void emptyExecutor() throws InterruptedException, InvalidMapException {
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        Platform p = new Platform();
        ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        Executor ex = new Executor(context, p, 1, "compute1");
        Thread t = new Thread(ex);
        t.start();

        p.execute(new StopExecutorRequest());
        t.join();
    }

    @Test
    public void javaExecutor() throws InterruptedException, InvalidMapException {
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        Platform p = new Platform();
        ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        Executor ex = new Executor(context, p, 1, "compute1");
        Thread t = new Thread(ex);
        t.start();

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setLang(Lang.JAVA);
        invBr = invBr.setImpl(new AbstractMethodImplementation(0, 0,
            new ImplementationDescription<>(new MethodDefinition(this.getClass().getCanonicalName(), "javaTest"), "",
                false, new MethodResourceDescription(), null, null)));
        FakeInvocation invocation1 = invBr.build();
        ExecutorRequest exec = new InvocationExecutionRequest(invocation1, null);
        p.execute(exec);
        p.execute(new StopExecutorRequest());
        t.join();
        Assert.assertNotEquals("Unset start time", 0, invocation1.getProfileTimes()[0]);
        Assert.assertNotEquals("Unset end time", 0, invocation1.getProfileTimes()[1]);
    }

    public static void javaTest() {

    }


    public static class Platform implements ExecutorContext {

        private final HashMap<Class<?>, ExecutionPlatformMirror<?>> mirrors = new HashMap<>();
        private final RequestQueue<ExecutorRequest> queue = new RequestQueue<>();


        @Override
        public ExecutionPlatformMirror<?> getMirror(Class<?> invoker) {
            return this.mirrors.get(invoker);
        }

        @Override
        public void registerMirror(Class<?> invoker, ExecutionPlatformMirror<?> mirror) {
            this.mirrors.put(invoker, mirror);
        }

        @Override
        public ExecutorRequest newThread() {
            return this.getJob();
        }

        @Override
        public int getSize() {
            return 1;
        }

        @Override
        public ExecutorRequest getJob() {
            return this.queue.dequeue();
        }

        public void execute(ExecutorRequest job) {
            this.queue.enqueue(job);
        }

        @Override
        public InvocationResources acquireResources(int jobId, ResourceDescription requirements,
            InvocationResources previousAllocation) {
            // No need to do anything
            return null;
        }

        @Override
        public void releaseResources(int jobId) {
            // No need to do anything
        }

        @Override
        public Collection<ExecutionPlatformMirror<?>> getMirrors() {
            return this.mirrors.values();
        }

        @Override
        public void registerRunningJob(Invocation invocation, Invoker invoker, TimerTask timeout) {

        }

        @Override
        public void blockedRunner(Invocation inv, InvocationRunner runner, InvocationResources assignedResources) {

        }

        @Override
        public void unblockedRunner(Invocation inv, InvocationRunner runner, InvocationResources previousAllocation,
            Semaphore sem) {
            sem.release();
        }

        @Override
        public void unregisterRunningJob(int jobId) {

        }
    }
}
