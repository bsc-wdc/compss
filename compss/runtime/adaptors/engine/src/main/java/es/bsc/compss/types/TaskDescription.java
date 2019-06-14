/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.util.ErrorManager;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import java.util.List;


public class TaskDescription implements Externalizable {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private TaskType type;
    private Lang lang;
    private String signature;
    private Integer coreId;

    private boolean priority;
    private int numNodes;
    private boolean mustReplicate;
    private boolean mustDistribute;
    private final List<Parameter> parameters;
    private final boolean hasTarget;
    private final int numReturns;
    private final int timeOut;

    /**
     * No-parameter constructor only used for deserialization.
     */
    public TaskDescription() {
        this.type = null;
        this.lang = null;
        this.signature = null;
        this.coreId = null;
        this.priority = false;
        this.numNodes = 0;
        this.mustReplicate = false;
        this.mustDistribute = false;
        this.parameters = null;
        this.hasTarget = false;
        this.numReturns = 0;
    }

    /**
     * Task description constructor.
     *
     * @param type Type of task.
     * @param lang Method language.
     * @param signature Method signature.
     * @param coreId Core Id.
     * @param isPrioritary Whether the method is prioritary or not.
     * @param numNodes Number of nodes required for the method execution.
     * @param isReplicated Whether the method is replicated or not.
     * @param isDistributed Whether the method is distributed or not.
     * @param hasTarget Whether the method has a target parameter or not.
     * @param numReturns Number of return values.
     * @param parameters Number of parameters.
     */
    public TaskDescription(TaskType type, Lang lang, String signature, int coreId, boolean isPrioritary, int numNodes,
            boolean isReplicated, boolean isDistributed, boolean hasTarget, int numReturns,
            int timeOut, List<Parameter> parameters) {

        this.type = type;
        this.lang = lang;
        this.signature = signature;
        this.coreId = coreId;

        this.priority = isPrioritary;
        this.numNodes = numNodes;
        this.mustReplicate = isReplicated;
        this.mustDistribute = isDistributed;

        this.hasTarget = hasTarget;
        this.parameters = parameters;
        this.numReturns = numReturns;
        this.timeOut = timeOut;

        if (this.numNodes < Constants.SINGLE_NODE) {
            ErrorManager.error("Invalid number of nodes " + this.numNodes + " on executeTask " + this.signature);
        }
    }

    /**
     * Returns the task id.
     *
     * @return The task Id.
     */
    public Integer getCoreId() {
        return this.coreId;
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
     * Returns whether the task has the priority flag enabled or not.
     *
     * @return {@code true} if the priority flag is enabled, {@code false} otherwise.
     */
    public boolean hasPriority() {
        return this.priority;
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
    public List<Parameter> getParameters() {
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
     * Returns the set time out.
     * 
     * @return The time out.
     */
    public int getTimeOut() {
        return timeOut;
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

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeInt(type.ordinal());
        oo.writeInt(lang.ordinal());
        oo.writeUTF(signature);
        oo.writeObject(coreId);

        oo.writeBoolean(priority);
        oo.writeInt(numNodes);
        oo.writeBoolean(mustReplicate);
        oo.writeBoolean(mustDistribute);

        oo.writeObject(parameters);
        oo.writeBoolean(hasTarget);
        oo.writeInt(numReturns);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        this.type = TaskType.values()[oi.readInt()];
        this.lang = Lang.values()[oi.readInt()];
        this.signature = oi.readUTF();
        this.coreId = (Integer) oi.readObject();

        this.priority = oi.readBoolean();
        this.numNodes = oi.readInt();
        this.mustReplicate = oi.readBoolean();
        this.mustDistribute = oi.readBoolean();

        this.parameters = (List<Parameter>) oi.readObject();
        this.hasTarget = oi.readBoolean();
        this.numReturns = oi.readInt();
    }

}
