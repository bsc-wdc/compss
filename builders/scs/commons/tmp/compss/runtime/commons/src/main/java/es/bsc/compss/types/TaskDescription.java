/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.implementations.ServiceImplementation;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.ErrorManager;

import java.io.Serializable;


public class TaskDescription implements Serializable {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private final TaskType type;
    private final String signature;
    private final Integer coreId;

    private final boolean priority;
    private final int numNodes;
    private final boolean mustReplicate;
    private final boolean mustDistribute;

    private final Parameter[] parameters;
    private final boolean hasTarget;
    private final int numReturns;

    /**
     * Task description creation for METHODS
     *
     * @param signature
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param hasTarget
     * @param numReturns
     * @param parameters
     */
    public TaskDescription(String signature, boolean isPrioritary, int numNodes, boolean isReplicated, boolean isDistributed,
            boolean hasTarget, int numReturns, Parameter[] parameters) {

        this.type = TaskType.METHOD;
        this.signature = signature;
        this.coreId = CoreManager.getCoreId(signature);

        this.priority = isPrioritary;
        this.numNodes = numNodes;
        this.mustReplicate = isReplicated;
        this.mustDistribute = isDistributed;

        this.hasTarget = hasTarget;
        this.parameters = parameters;
        this.numReturns = numReturns;

        if (this.numNodes < Constants.SINGLE_NODE) {
            ErrorManager.error("Invalid number of nodes " + this.numNodes + " on executeTask " + this.signature);
        }
    }

    /**
     * Task description creation for SERVICES
     *
     * @param namespace
     * @param service
     * @param port
     * @param operation
     * @param isPrioritary
     * @param hasTarget
     * @param numReturns
     * @param parameters
     */
    public TaskDescription(String namespace, String service, String port, String operation, boolean isPrioritary, boolean hasTarget, int numReturns, Parameter[] parameters) {

        this.type = TaskType.SERVICE;

        this.priority = isPrioritary;
        this.numNodes = Constants.SINGLE_NODE;
        this.mustReplicate = Boolean.parseBoolean(Constants.IS_NOT_REPLICATED_TASK);
        this.mustDistribute = Boolean.parseBoolean(Constants.IS_NOT_DISTRIBUTED_TASK);

        this.hasTarget = hasTarget;
        this.numReturns = numReturns;
        this.parameters = parameters;

        this.signature = ServiceImplementation.getSignature(namespace, service, port, operation, hasTarget, numReturns, parameters);
        this.coreId = CoreManager.getCoreId(this.signature);
    }

    /**
     * Returns the id of the task
     *
     * @return
     */
    public Integer getId() {
        return this.coreId;
    }

    /**
     * Returns the method name
     *
     * @return
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
     * Returns whether the task has the priority flag enabled or not
     *
     * @return
     */
    public boolean hasPriority() {
        return this.priority;
    }

    /**
     * Returns the number of required nodes to execute the task
     *
     * @return
     */
    public int getNumNodes() {
        return this.numNodes;
    }

    /**
     * Returns if the task can be executed in a single task or not
     *
     * @return
     */
    public boolean isSingleNode() {
        return this.numNodes == Constants.SINGLE_NODE;
    }

    /**
     * Returns whether the replication flag is enabled or not
     *
     * @return
     */
    public boolean isReplicated() {
        return this.mustReplicate;
    }

    /**
     * Returns whether the distributed flag is enabled or not
     *
     * @return
     */
    public boolean isDistributed() {
        return this.mustDistribute;
    }

    /**
     * Returns the task parameters
     *
     * @return
     */
    public Parameter[] getParameters() {
        return this.parameters;
    }

    /**
     * Returns whether the task has a target object or not
     *
     * @return
     */
    public boolean hasTargetObject() {
        return this.hasTarget;
    }

    /**
     *
     * /**
     * Returns the number of return values of the task
     *
     * @return
     */
    public int getNumReturns() {
        return this.numReturns;
    }

    /**
     * Returns the task type
     *
     * @return
     */
    public TaskType getType() {
        return this.type;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("[Core id: ").append(this.coreId).append("]");

        buffer.append(", [Priority: ").append(this.priority).append("]");
        buffer.append(", [NumNodes: ").append(this.numNodes).append("]");
        buffer.append(", [MustReplicate: ").append(this.mustReplicate).append("]");
        buffer.append(", [MustDistribute: ").append(this.mustDistribute).append("]");

        buffer.append(", [").append(getName()).append("(");
        int numParams = this.parameters.length;
        if (this.hasTarget) {
            numParams--;
        }
        numParams -= numReturns;
        if (numParams > 0) {
            buffer.append(this.parameters[0].getType());
            for (int i = 1; i < numParams; i++) {
                buffer.append(", ").append(this.parameters[i].getType());
            }
        }
        buffer.append(")]");

        return buffer.toString();
    }

}
