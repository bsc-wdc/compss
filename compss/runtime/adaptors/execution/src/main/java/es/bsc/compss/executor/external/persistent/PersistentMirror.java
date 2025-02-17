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
package es.bsc.compss.executor.external.persistent;

import es.bsc.compss.executor.external.ExecutionPlatformMirror;
import es.bsc.compss.executor.external.piped.PipePair;
import es.bsc.compss.invokers.external.persistent.PersistentInvoker;
import es.bsc.compss.types.execution.InvocationContext;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;


public class PersistentMirror implements ExecutionPlatformMirror<Void> {

    private InvocationContext context;

    Set<String> registeredThread;


    public PersistentMirror(InvocationContext context, int size) {
        this.context = context;
        this.registeredThread = new HashSet<String>();
    }

    @Override
    public void stop() {

    }

    @Override
    public void cancelJob(PipePair pipe) {

    }

    @Override
    public void unregisterExecutor(String id) {

        if (this.registeredThread.contains(id)) {
            PrintStream out = context.getThreadOutStream();
            // WARNING: Do not remove this log, is used for runtime testing
            out.println("[PersistentMirror] Thread unregistration has been done.");
            PersistentInvoker.finishThread();

            synchronized (this.registeredThread) {
                this.registeredThread.remove(id);
            }
        }

    }

    @Override
    public Void registerExecutor(int id, String name) {
        synchronized (this.registeredThread) {
            this.registeredThread.add(name);
        }

        PrintStream out = context.getThreadOutStream();
        // WARNING: Do not remove this log, is used for runtime testing
        out.println("[PersistentMirror] Thread registration has been done.");
        PersistentInvoker.initThread();
        return null;
    }
}
