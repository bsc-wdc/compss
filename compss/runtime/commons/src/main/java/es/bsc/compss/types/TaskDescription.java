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
    private final boolean hasReturn;


    /**
     * Task description creation for METHODS
     * 
     * @param signature
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param hasTarget
     * @param hasReturn
     * @param parameters
     */
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
     * @param parameters
     */
    public TaskDescription(String namespace, String service, String port, String operation, boolean isPrioritary, boolean hasTarget,
            boolean hasReturn, Parameter[] parameters) {

        this.type = TaskType.SERVICE;

        this.priority = isPrioritary;
        this.numNodes = Constants.SINGLE_NODE;
        this.mustReplicate = Boolean.parseBoolean(Constants.IS_NOT_REPLICATED_TASK);
        this.mustDistribute = Boolean.parseBoolean(Constants.IS_NOT_DISTRIBUTED_TASK);

        this.hasTarget = hasTarget;
        this.hasReturn = hasReturn;
        this.parameters = parameters;

        this.signature = ServiceImplementation.getSignature(namespace, service, port, operation, hasTarget, hasReturn, parameters);
        this.coreId = CoreManager.getCoreId(this.signature);
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
