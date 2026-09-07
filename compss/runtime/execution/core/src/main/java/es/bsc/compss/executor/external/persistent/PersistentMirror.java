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
package es.bsc.compss.executor.external.persistent;

import es.bsc.compss.execution.types.ExecutorContext;
import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.external.ExecutorContextMirror;
import es.bsc.compss.executor.external.piped.PipePair;
import es.bsc.compss.invokers.external.persistent.PersistentInvoker;
import es.bsc.compss.types.execution.InvocationContext;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

public class PersistentMirror implements ExecutorContextMirror<PersistentExecutor> {

    private final InvocationContext context;

    private final Set<Executor> registeredThread;
    private static final ThreadLocal<PersistentExecutor> executor = new ThreadLocal<>();


    /**
     * Creates a new PythonMirror instance with the given context and size.
     *
     * @param platform Mirrored platform
     */
    public PersistentMirror(ExecutorContext platform) {
        this.context = platform.getContext();
        this.registeredThread = new HashSet<>();
    }

    /**
     * Returns the PersistentExecutor associated to an executor.
     *
     * @param e executor whose PersistentExecutor we want to obtain
     * @return PersistentExecutor corresponding to executor e
     */
    public static PersistentExecutor getPersistentExecutor(Executor e) {
        PersistentExecutor pExec = executor.get();
        if (pExec == null || !pExec.canRun()) {
            ExecutorContext platform = e.getPlatform();
            PersistentMirror mirror;
            synchronized (platform) {
                mirror = (PersistentMirror) platform.getMirror(PersistentMirror.class);
                if (mirror == null) {
                    mirror = new PersistentMirror(platform);
                    platform.registerMirror(PersistentMirror.class, mirror);
                }
            }
            pExec = mirror.registerExecutor(e);
            executor.set(pExec);
        }
        return pExec;
    }

    @Override
    public void stop() {

    }

    @Override
    public void cancelJob(PipePair pipe) {

    }

    @Override
    public void unregisterExecutor(Executor executor) {

        if (this.registeredThread.contains(executor)) {
            PrintStream out = context.getThreadOutStream();
            // WARNING: Do not remove this log, is used for runtime testing
            out.println("[PersistentMirror] Thread unregistration has been done.");
            PersistentInvoker.finishThread();

            synchronized (this.registeredThread) {
                this.registeredThread.remove(executor);
            }
        }

    }

    @Override
    public PersistentExecutor registerExecutor(Executor executor) {
        synchronized (this.registeredThread) {
            this.registeredThread.add(executor);
        }

        PrintStream out = context.getThreadOutStream();
        // WARNING: Do not remove this log, is used for runtime testing
        out.println("[PersistentMirror] Thread registration has been done.");
        PersistentInvoker.initThread();
        return new PersistentExecutor();
    }
}
