package integratedtoolkit.types;

import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.implementations.ServiceImplementation;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.util.CoreManager;

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
    private final boolean hasReturn;


    public TaskDescription(String signature, boolean isPrioritary, int numNodes, boolean isReplicated, boolean isDistributed,
            boolean hasTarget, boolean hasReturn, Parameter[] parameters) {

        this.type = TaskType.METHOD;
        this.signature = signature;
        this.coreId = CoreManager.getCoreId(signature);

        this.priority = isPrioritary;
        this.numNodes = numNodes;
        this.mustReplicate = isReplicated;
        this.mustDistribute = isDistributed;

        this.hasTarget = hasTarget;
        this.parameters = parameters;
        this.hasReturn = hasReturn;
    }

    public TaskDescription(String namespace, String service, String port, String operation, boolean isPrioritary, boolean hasTarget,
            Parameter[] parameters) {

        this.type = TaskType.SERVICE;

        this.priority = isPrioritary;
        this.numNodes = Constants.SINGLE_NODE;
        this.mustReplicate = Boolean.parseBoolean(Constants.IS_NOT_REPLICATED_TASK);
        this.mustDistribute = Boolean.parseBoolean(Constants.IS_NOT_DISTRIBUTED_TASK);

        this.hasTarget = hasTarget;
        this.parameters = parameters;
        if (parameters.length == 0) {
            this.hasReturn = false;
        } else {
            Parameter lastParam = parameters[parameters.length - 1];
            DataType type = lastParam.getType();
            this.hasReturn = (lastParam.getDirection() == Direction.OUT
                    && (type == DataType.OBJECT_T || type == DataType.PSCO_T || type == DataType.EXTERNAL_PSCO_T));
        }

        this.signature = ServiceImplementation.getSignature(namespace, service, port, operation, hasTarget, hasReturn, parameters);
        this.coreId = CoreManager.getCoreId(signature);
    }

    public Integer getId() {
        return this.coreId;
    }

    public String getName() {
        String methodName = this.signature;

        int endIndex = methodName.indexOf('(');
        if (endIndex >= 0) {
            methodName = methodName.substring(0, endIndex);
        }

        return methodName;
    }

    public boolean hasPriority() {
        return this.priority;
    }

    public int getNumNodes() {
        return this.numNodes;
    }

    public boolean isSingleNode() {
        return this.numNodes == Constants.SINGLE_NODE;
    }

    public boolean isReplicated() {
        return this.mustReplicate;
    }

    public boolean isDistributed() {
        return this.mustDistribute;
    }

    public Parameter[] getParameters() {
        return this.parameters;
    }

    public boolean hasTargetObject() {
        return this.hasTarget;
    }

    public boolean hasReturnValue() {
        return this.hasReturn;
    }

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
        if (this.hasReturn) {
            numParams--;
        }
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
