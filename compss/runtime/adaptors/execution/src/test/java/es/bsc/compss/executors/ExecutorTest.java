/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.ExecutorContext;
import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.utils.ExecutionPlatformMirror;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.test.utils.ExecutionFlowVerifier;
import es.bsc.compss.invokers.test.utils.FakeInvocation;
import es.bsc.compss.invokers.test.utils.FakeInvocationContext;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.exceptions.InvalidMapException;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.util.RequestQueue;
import java.util.HashMap;
import org.junit.Test;


public class ExecutorTest {

    private final ExecutionFlowVerifier expectedEvents = new ExecutionFlowVerifier();


    @Test
    public void emptyExecutor() throws InterruptedException, InvalidMapException {
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        Platform p = new Platform();
        ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        Executor ex = new Executor(context, p, "compute1");
        Thread t = new Thread(ex);
        t.start();

        p.execute(null);
        t.join();
    }

    @Test
    public void JavaExecutor() throws InterruptedException, InvalidMapException {
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        Platform p = new Platform();
        ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        Executor ex = new Executor(context, p, "compute1");
        Thread t = new Thread(ex);
        t.start();

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setLang(Lang.JAVA);
        invBr = invBr
                .setImpl(new MethodImplementation(this.getClass().getCanonicalName(), "javaTest", 0, 0, new MethodResourceDescription()));
        Invocation invocation1 = invBr.build();
        Execution exec = new Execution(invocation1, null);
        p.execute(exec);
        p.execute(null);
        t.join();
    }

    public static void javaTest() {

    }


    public static class Platform implements ExecutorContext {

        private final HashMap<Class<?>, ExecutionPlatformMirror> mirrors = new HashMap<>();
        private final RequestQueue<Execution> queue = new RequestQueue<>();


        @Override
        public ExecutionPlatformMirror getMirror(Class<?> invoker) {
            return mirrors.get(invoker);
        }

        @Override
        public void registerMirror(Class<?> invoker, ExecutionPlatformMirror mirror) {
            mirrors.put(invoker, mirror);
        }

        @Override
        public int getSize() {
            return 1;
        }

        @Override
        public Execution getJob() {
            return queue.dequeue();
        }

        public void execute(Execution job) {
            queue.enqueue(job);
        }

        @Override
        public InvocationResources acquireComputingUnits(int jobId, ResourceDescription requirements) {
            // No need to do anything
            return null;
        }

        @Override
        public void releaseComputingUnits(int jobId) {
            // No need to do anything
        }

    }
}
