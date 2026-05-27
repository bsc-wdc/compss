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
package es.bsc.compss.loader.editing;

import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.api.WorkflowListener;
import es.bsc.compss.loader.workflow.JavaWorkflow;

public class WorkflowSupplier extends ThreadLocal<JavaWorkflow> {

    private final COMPSsRuntime runtime;
    private final String ceiName;
    private final WorkflowListener listener;


    /**
     * Constructs a new Workflow Supplier for each thread.
     * 
     * @param rt runtime that will support the workflows
     * @param ceiName Name of the interface used to parallelize the workflow
     * @param listener element monitoring changes in the workflow
     */
    public WorkflowSupplier(COMPSsRuntime rt, String ceiName, WorkflowListener listener) {
        this.runtime = rt;
        this.ceiName = ceiName;
        this.listener = listener;
    }

    @Override
    protected JavaWorkflow initialValue() {
        try {
            return new JavaWorkflow(runtime, ceiName, listener);
        } catch (Exception e) {
            System.err.println("Cannot register workflow");
            e.printStackTrace(System.err);
            return null;
        }
    }

}
