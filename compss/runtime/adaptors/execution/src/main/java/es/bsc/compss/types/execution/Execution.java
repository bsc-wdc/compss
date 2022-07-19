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

import es.bsc.compss.worker.COMPSsException;


public class Execution {

    private final Invocation invocation;
    private final ExecutionListener listener;


    public Execution(Invocation invocation, ExecutionListener listener) {
        this.invocation = invocation;
        this.listener = listener;
    }

    public Invocation getInvocation() {
        return this.invocation;
    }

    public boolean isStopRequest() {
        return this.invocation == null && this.listener == null;
    }

    /**
     * Execution end notification.
     * 
     * @param e COMPSsException to handle task groups.
     * @param success Flags to indicate if execution was successful.
     */
    public void notifyEnd(COMPSsException e, boolean success) {
        if (this.listener != null) {
            this.listener.notifyEnd(this.invocation, success, e);
        }
    }

}
