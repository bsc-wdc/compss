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
package es.bsc.compss.executor.utils;

import es.bsc.compss.invokers.external.persistent.PersistentInvoker;
import es.bsc.compss.types.execution.InvocationContext;

import java.io.PrintStream;

public class PersistentMirror implements ExecutionPlatformMirror {

    InvocationContext context;

    public PersistentMirror(InvocationContext context, int size) {
        this.context = context;
    }

    @Override
    public void stop() {

    }

    @Override
    public void unregisterExecutor(String id) {
        PrintStream out = context.getThreadOutStream();
        out.println("[PersistentMirror] Thread unregistration has been done."); //WARNING: Do not remove this log, is used for runtime testing
        PersistentInvoker.finishThread();
    }

    public void registerExecutor(String id) {
        PrintStream out = context.getThreadOutStream();
        out.println("[PersistentMirror] Thread registration has been done."); //WARNING: Do not remove this log, is used for runtime testing
        PersistentInvoker.initThread();
    }
}
