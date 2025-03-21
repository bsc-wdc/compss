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
package es.bsc.compss.types;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.util.ErrorManager;

import java.util.List;


public class TaskDescription<P extends Parameter> {

    private final TaskType type;
    private final Lang lang;
    private final String signature;
    private final CoreElement coreElement;

    private final boolean priority;
    private final boolean reduction;
    private final int numNodes;
    private final boolean mustReplicate;
    private final boolean mustDistribute;
    private final List<P> parameters;
    private final boolean hasTarget;
    private final int numReturns;
    private final OnFailure onFailure;
    private final long timeOut;

    private final String parallelismSource;


    /**
     * Task description constructor.
     *
     * @param type Type of task.
     * @param lang Method language.
     * @param signature Method signature.
     * @param coreElement Core Element to execute.
     * @param parallelismSource Identifier of the interface to use for detecting parallelism within the invocation.
     * @param isPrioritary Whether the method is prioritary or not.
     * @param numNodes Number of nodes required for the method execution.
     * @param isReduction Whether the method is reduce or not.
     * @param isReplicated Whether the method is replicated or not.
     * @param isDistributed Whether the method is distributed or not.
     * @param hasTarget Whether the method has a target parameter or not.
     * @param numReturns Number of return values.
     * @param timeOut Task timeout.
     * @param onFailure On failure mechanisms.
     * @param parameters Number of parameters.
     */
    public TaskDescription(TaskType type, Lang lang, String signature, CoreElement coreElement,
        String parallelismSource, boolean isPrioritary, int numNodes, boolean isReduction, boolean isReplicated,
        boolean isDistributed, boolean hasTarget, int numReturns, OnFailure onFailure, long timeOut,
        List<P> parameters) {

        this.type = type;
        this.lang = lang;
        this.signature = signature;
        this.coreElement = coreElement;
        this.parallelismSource = parallelismSource;

        this.priority = isPrioritary;
        this.numNodes = numNodes;
        this.reduction = isReduction;
        this.mustReplicate = isReplicated;
        this.mustDistribute = isDistributed;

        this.hasTarget = hasTarget;
        this.parameters = parameters;
        this.numReturns = numReturns;

        this.onFailure = onFailure;
        this.timeOut = timeOut;

        if (this.numNodes < Constants.SINGLE_NODE) {
            ErrorManager.error("Invalid number of nodes " + this.numNodes + " on executeTask " + this.signature);
        }
    }

    /**
     * Returns the Core Element to execute to run the task.
     *
     * @return Core Element to execute to run the task.
     */
    public CoreElement getCoreElement() {
        return this.coreElement;
    }

    /**
     * Returns the task language.
     *
     * @return The task language.
     */
    public Lang getLang() {
        return lang;
    }

    /**
     * Returns the method name.
     *
     * @return The method name.
     */
    public String getName() {
        String methodName = this.signature;

        int endIndex = methodName.indexOf('(');
        if (endIndex >= 0) {
            methodName = methodName.substring(0, endIndex);
        }

        return methodName;
    }

    /**
     * Returns a string identifying the interface to use for detecting parallelism within the invocation.
     *
     * @return interface id
     */
    public String getParallelismSource() {
        return this.parallelismSource;
    }

    /**
     * Returns whether the task has the priority flag enabled or not.
     *
     * @return {@code true} if the priority flag is enabled, {@code false} otherwise.
     */
    public boolean hasPriority() {
        return this.priority;
    }

    /**
     * Returns whether the task has the priority flag enabled or not.
     *
     * @return {@code true} if the priority flag is enabled, {@code false} otherwise.
     */
    public boolean isReduction() {
        return this.reduction;
    }

    /**
     * Returns the number of required nodes to execute the task.
     *
     * @return Number of nodes required by the task execution.
     */
    public int getNumNodes() {
        return this.numNodes;
    }

    /**
     * Returns if the task can be executed in a single node or not.
     *
     * @return {@code true} if the task can be executed in a single node, {@code false} otherwise.
     */
    public boolean isSingleNode() {
        return this.numNodes == Constants.SINGLE_NODE;
    }

    /**
     * Returns whether the replication flag is enabled or not.
     *
     * @return {@code true} if the replication flag is enabled, {@code false} otherwise.
     */
    public boolean isReplicated() {
        return this.mustReplicate;
    }

    /**
     * Returns whether the distributed flag is enabled or not.
     *
     * @return {@code true} if the distributed flag is enabled, {@code false} otherwise.
     */
    public boolean isDistributed() {
        return this.mustDistribute;
    }

    /**
     * Returns the task parameters.
     *
     * @return The task parameters.
     */
    public List<P> getParameters() {
        return this.parameters;
    }

    /**
     * Returns whether the task has a target object or not.
     *
     * @return {@code true} if the task has a target object, {@code false} otherwise.
     */
    public boolean hasTargetObject() {
        return this.hasTarget;
    }

    /**
     * Returns the number of return values of the task.
     *
     * @return The number of return values of the task.
     */
    public int getNumReturns() {
        return this.numReturns;
    }

    /**
     * Returns the task type.
     *
     * @return The task type.
     */
    public TaskType getType() {
        return this.type;
    }

    /**
     * Returns the on-failure mechanisms.
     *
     * @return The on-failure mechanisms.
     */
    public OnFailure getOnFailure() {
        return this.onFailure;
    }

    /**
     * Returns the set time out.
     *
     * @return The time out.
     */
    public long getTimeOut() {
        return this.timeOut;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("[Core id: ").append(this.coreElement.getCoreId()).append("]");

        buffer.append(", [Priority: ").append(this.priority).append("]");
        buffer.append(", [NumNodes: ").append(this.numNodes).append("]");
        buffer.append(", [MustReplicate: ").append(this.mustReplicate).append("]");
        buffer.append(", [MustDistribute: ").append(this.mustDistribute).append("]");

        buffer.append(", [").append(getName()).append("(");
        int numParams = this.parameters.size();
        if (this.hasTarget) {
            numParams--;
        }
        numParams -= numReturns;
        if (numParams > 0) {
            buffer.append(this.parameters.get(0).getType());
            for (int i = 1; i < numParams; i++) {
                buffer.append(", ").append(this.parameters.get(i).getType());
            }
        }
        buffer.append(")]");

        return buffer.toString();
    }
}
