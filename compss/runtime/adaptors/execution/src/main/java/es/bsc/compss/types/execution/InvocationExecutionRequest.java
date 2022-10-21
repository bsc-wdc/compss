/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.execution;

import es.bsc.compss.executor.Executor;
import es.bsc.compss.worker.COMPSsException;


public class InvocationExecutionRequest extends ExecutorRequest {

    private final Invocation invocation;
    private final ExecutionListener listener;


    /**
     * Constructs a new InvocationExecutionRequest.
     *
     * @param invocation invocation to run
     * @param listener element to notify changes in the invocation execution
     */
    public InvocationExecutionRequest(Invocation invocation, ExecutionListener listener) {
        this.invocation = invocation;
        if (listener == null) {
            this.listener = new IgnoreListener();
        } else {
            this.listener = listener;
        }
    }

    public boolean isIOExecution() {
        return invocation.getMethodImplementation().isIO();
    }

    public String getInvocationSignature() {
        return invocation.getMethodImplementation().getSignature();
    }

    @Override
    public boolean hasAgePriority() {
        return false;
    }

    @Override
    public void run(Executor executor) {
        try {
            executor.processInvocation(invocation, listener);
            this.listener.notifyEnd(this.invocation, true, null);
        } catch (COMPSsException ce) {
            this.listener.notifyEnd(this.invocation, false, ce);
        } catch (Exception e) {
            this.listener.notifyEnd(this.invocation, false, null);
        }
    }

    @Override
    public String toString() {
        return "Invocation for Job " + this.invocation.getJobId();
    }


    private static class IgnoreListener implements ExecutionListener {

        public IgnoreListener() {
        }

        @Override
        public void notifyEnd(Invocation invocation, boolean success, COMPSsException e) {
            // Ignore
        }
    }

}
