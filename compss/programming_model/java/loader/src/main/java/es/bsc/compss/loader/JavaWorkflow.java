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
package es.bsc.compss.loader;

import es.bsc.compss.api.ApplicationRunner;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.api.Workflow;
import es.bsc.compss.loader.ObjectRegistry;
import es.bsc.compss.worker.COMPSsException;


public class JavaWorkflow implements Workflow {

    private final Workflow workflow;
    private final ObjectRegistry oReg;


    public JavaWorkflow(COMPSsRuntime runtime) {
        this(runtime, null, null);
    }

    public JavaWorkflow(COMPSsRuntime runtime, String parallelismSource, ApplicationRunner runner) {
        this.workflow = runtime.registerWorkflow(parallelismSource, runner);
        this.oReg = new ObjectRegistry((LoaderAPI) runtime);
    }

    /**
     * Access to the additional state associated with this workflow.
     */
    public ObjectRegistry getObjectRegistry() {
        return this.oReg;
    }

    @Override
    public Long getId() {
        return workflow.getId();
    }

    @Override
    public void deregister() {
        workflow.deregister();
    }

    @Override
    public void openTaskGroup(String groupName, boolean implicitBarrier) {
        workflow.openTaskGroup(groupName, implicitBarrier);
    }

    @Override
    public void closeTaskGroup(String groupName) {
        workflow.closeTaskGroup(groupName);
    }

    @Override
    public void cancelTaskGroup(String groupName) throws COMPSsException {
        workflow.cancelTaskGroup(groupName);
    }

    @Override
    public void cancelApplicationTasks() {
        workflow.cancelApplicationTasks();
    }

    @Override
    public void noMoreTasks() {
        workflow.noMoreTasks();
    }

    @Override
    public void barrier() {
        workflow.barrier();
    }

    @Override
    public void barrier(boolean noMoreTasks) {
        workflow.barrier(noMoreTasks);
    }

    @Override
    public void barrierGroup(String groupName) throws COMPSsException {
        workflow.barrierGroup(groupName);
    }

    @Override
    public void snapshot() {
        workflow.snapshot();
    }

}
