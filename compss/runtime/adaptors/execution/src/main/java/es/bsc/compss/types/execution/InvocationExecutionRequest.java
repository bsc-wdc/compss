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
package es.bsc.compss.types.execution;

import es.bsc.compss.executor.Executor;
import es.bsc.compss.worker.COMPSsException;


public class InvocationExecutionRequest extends ExecutorRequest {

    private final Invocation invocation;
    private final Listener listener;


    /**
     * Constructs a new InvocationExecutionRequest.
     *
     * @param invocation invocation to run
     * @param listener element to notify changes in the invocation execution
     */
    public InvocationExecutionRequest(Invocation invocation, Listener listener) {
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


    private static class IgnoreListener implements Listener {

        public IgnoreListener() {
        }

        @Override
        public void onResultAvailable(InvocationParam param) {
            // Ignore
        }

        @Override
        public void notifyEnd(Invocation invocation, boolean success, COMPSsException e) {
            // Ignore
        }
    }

    public static interface Listener {

        /**
         * Notifies that a result of the invocation is already available.
         * 
         * @param param produced result
         */
        public void onResultAvailable(InvocationParam param);

        /**
         * Notifies the end of the given invocation with the given status.
         *
         * @param invocation Task invocation.
         * @param success Whether the task was successful or not.
         * @param e COMPSsException for task groups.
         */
        public void notifyEnd(Invocation invocation, boolean success, COMPSsException e);

    }
}
