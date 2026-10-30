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
package es.bsc.compss.nio.listeners;

import es.bsc.compss.exceptions.WorkflowException;
import es.bsc.compss.execution.Invocation;
import es.bsc.compss.execution.InvocationExecutionListener;
import es.bsc.compss.execution.InvocationParam;
import es.bsc.compss.nio.worker.NIOWorker;

public class TaskExecutionListener implements InvocationExecutionListener {

    private final NIOWorker nw;


    /**
     * Creates a listener for a task execution.
     * 
     * @param nw Associated NIOWorker.
     */
    public TaskExecutionListener(NIOWorker nw) {
        this.nw = nw;
    }

    @Override
    public void notifyEnd(Invocation invocation, boolean success, WorkflowException exception) {
        this.nw.sendTaskDone(invocation, success, exception);
    }

    @Override
    public void onResultAvailable(InvocationParam param) {
        // Do nothing. All results notified at task end.
    }

}
