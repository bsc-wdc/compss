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

    /**
     * Registers new access from the workflow to an object as a parameter.
     *
     * @param o Object parameter.
     * @return Final hashcode of the object.
     */
    public int accessAsTaskParam(Object o) {
        return this.oReg.newObjectParameter(workflow.getId(), o);
    }

    /**
     * Registers new write access from the main code of the workflow to an object.
     *
     * @param o Object parameter.
     */
    public void accessObject(Object o) {
        this.oReg.newObjectAccess(workflow.getId(), o, true);
    }

    /**
     * Registers new {@code isWriter} access from the main code of the workflow to an object.
     *
     * @param o Object parameter.
     * @param isWriter {@code true} if its a writer access, {@code false} otherwise.
     */
    public void accessObject(Object o, boolean isWriter) {
        this.oReg.newObjectAccess(workflow.getId(), o, isWriter);
    }

    /**
     * Returns the currently registered value for the object represented by {@code o}.
     *
     * @param o Object.
     * @return Internal object representing the given object {@code o}.
     */
    public Object getRegisteredObjectValue(Object o) {
        return this.oReg.getInternalObject(workflow.getId(), o);
    }

    /**
     * Deletes the given object {@code o}.
     *
     * @param o Object.
     * @return {@code true} if the object has been removed, {@code false} otherwise.
     */
    public boolean removeObject(Object o) {
        return this.oReg.delete(workflow.getId(), o);
    }

    /**
     * Retrieves the last updated value for an object.
     * 
     * @param o Object whose final values has to be retrieved.
     * @return final value of the object
     */
    public Object collectObjectFinalValue(Object o) {
        return this.oReg.newObjectAccess(workflow.getId(), o, false);
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
