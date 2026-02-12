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
package es.bsc.compss.loader.total;

import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.api.Workflow;
import es.bsc.compss.loader.JavaWorkflow;


public class WorkflowSupplier extends ThreadLocal<JavaWorkflow> {

    private static COMPSsRuntime runtime = null;


    public static void setRuntime(COMPSsRuntime rt) {
        runtime = rt;
    }

    protected JavaWorkflow initialValue() {
        return registerWorkflow();
    }

    /**
     * Registers a new Java Workflow in the runtime.
     *
     * @return registered workflow.
     */
    public static JavaWorkflow registerWorkflow() {
        try {
            return new JavaWorkflow(runtime);
        } catch (Exception e) {
            System.err.println("Cannot register workflow");
            e.printStackTrace(System.err);
            return null;
        }
    }

}
