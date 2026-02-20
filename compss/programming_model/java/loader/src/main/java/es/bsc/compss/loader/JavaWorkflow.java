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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.ApplicationRunner;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.api.Workflow;
import es.bsc.compss.loader.ObjectRegistry;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.worker.COMPSsException;


public class JavaWorkflow implements Workflow {

    private final Workflow workflow;
    private final ObjectRegistry oReg;


    public JavaWorkflow(COMPSsRuntime runtime) {
        this(runtime, null, null);
    }

    public JavaWorkflow(COMPSsRuntime runtime, String parallelismSource, ApplicationRunner runner) {
        this.workflow = runtime.registerWorkflow(parallelismSource, runner);
        this.oReg = new ObjectRegistry(this.workflow);
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
    public void registerData(DataType type, Object stub, String dataId) {
        this.workflow.registerData(type, stub, dataId);
    }

    @Override
    public boolean bindExistingVersionToData(Object o, String dataId) {
        return this.oReg.bindToDataIfExisting(o, dataId);
    }

    @Override
    public boolean bindExistingVersionToData(String fileName, String dataId) {
        return this.workflow.bindExistingVersionToData(fileName, dataId);
    }

    /**
     * Registers new access from the workflow to an object as a parameter.
     *
     * @param o Object parameter.
     */
    public void accessAsTaskParam(Object o) {
        this.oReg.newObjectParameter(o);
    }

    @Override
    public boolean isFileAccessed(String fileName) {
        return this.workflow.isFileAccessed(fileName);
    }

    /**
     * Registers new write access from the main code of the workflow to an object.
     *
     * @param o Object parameter.
     */
    public <T> T getObject(T o) {
        return this.oReg.newObjectAccess(o, true);
    }

    /**
     * Registers new {@code isWriter} access from the main code of the workflow to an object.
     *
     * @param o Object parameter.
     * @param isWriter {@code true} if its a writer access, {@code false} otherwise.
     */
    public <T> T getObject(T o, boolean isWriter) {
        return this.oReg.newObjectAccess(o, isWriter);
    }

    @Override
    public String getBindingObject(String bindingObjectName) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the currently registered value for the object represented by {@code o}.
     *
     * @param o Object.
     * @return Internal object representing the given object {@code o}.
     */
    public <T> T getRegisteredObjectValue(T o) {
        return this.oReg.getInternalObject(o);
    }

    /**
     * Deletes the given object {@code o}.
     *
     * @param o Object.
     * @return {@code true} if the object has been removed, {@code false} otherwise.
     */
    @Override
    public boolean removeObject(Object o) {
        return this.oReg.delete(o);
    }

    @Override
    public boolean deleteBindingObject(String bindingObjectName) {
        throw new UnsupportedOperationException();
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
    public int executeTask(String methodClass, String onFailure, int timeOut, String methodName, boolean isPrioritary,
        int numNodes, boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed,
        boolean hasTarget, Integer numReturns, int parameterCount, Object... parameters) {
        return workflow.executeTask(methodClass, onFailure, timeOut, methodName, isPrioritary, numNodes, isReduce,
            reduceChunkSize, isReplicated, isDistributed, hasTarget, numReturns, parameterCount, parameters);
    }

    @Override
    public int executeTask(String signature, String onFailure, int timeOut, boolean isPrioritary, int numNodes,
        boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed, boolean hasTarget,
        Integer numReturns, int parameterCount, Object... parameters) {
        return workflow.executeTask(signature, onFailure, timeOut, isPrioritary, numNodes, isReduce, reduceChunkSize,
            isReplicated, isDistributed, hasTarget, numReturns, parameterCount, parameters);
    }

    @Override
    public int executeTask(COMPSsConstants.Lang lang, String methodClass, String methodName, boolean isPrioritary,
        int numNodes, boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed,
        boolean hasTarget, int parameterCount, OnFailure onFailure, int timeOut, Object... parameters) {
        return workflow.executeTask(lang, methodClass, methodName, isPrioritary, numNodes, isReduce, reduceChunkSize,
            isReplicated, isDistributed, hasTarget, parameterCount, onFailure, timeOut, parameters);
    }

    @Override
    public int executeTask(String declareMethodFullyQualifiedName, boolean isPrioritary, int numNodes, boolean isReduce,
        int reduceChunkSize, boolean isReplicated, boolean isDistributed, boolean hasTarget, int parameterCount,
        OnFailure onFailure, int timeOut, Object... parameters) {
        return workflow.executeTask(declareMethodFullyQualifiedName, isPrioritary, numNodes, isReduce, reduceChunkSize,
            isReplicated, isDistributed, hasTarget, parameterCount, onFailure, timeOut, parameters);
    }

    @Override
    public int executeTask(COMPSsConstants.Lang lang, boolean hasSignature, String methodClass, String methodName,
        String signature, OnFailure onFailure, int timeOut, boolean isPrioritary, int numNodes, boolean isReduce,
        int reduceChunkSize, boolean isReplicated, boolean isDistributed, boolean hasTarget, Integer numReturns,
        int parameterCount, Object... parameters) {
        return workflow.executeTask(lang, hasSignature, methodClass, methodName, signature, onFailure, timeOut,
            isPrioritary, numNodes, isReduce, reduceChunkSize, isReplicated, isDistributed, hasTarget, numReturns,
            parameterCount, parameters);
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
