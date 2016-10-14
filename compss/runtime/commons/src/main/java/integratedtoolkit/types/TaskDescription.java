package integratedtoolkit.types;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.util.CoreManager;

import java.io.Serializable;


public class TaskDescription implements Serializable {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private final Integer coreId;
    private final String methodName;
    private final Parameter[] parameters;
    private final boolean priority;
    private final boolean hasTarget;
    private final boolean hasReturn;
    private final TaskType type;


    public TaskDescription(String methodClass, String methodName, boolean priority, boolean hasTarget, Parameter[] parameters) {
        this.methodName = methodName;
        this.priority = priority;
        this.hasTarget = hasTarget;
        this.parameters = parameters;
        if (parameters.length == 0) {
            this.hasReturn = false;
        } else {
            Parameter lastParam = parameters[parameters.length - 1];
            DataType type = lastParam.getType();
            this.hasReturn = (lastParam.getDirection() == DataDirection.OUT && (type == DataType.OBJECT_T || type == DataType.PSCO_T));
        }
        this.coreId = CoreManager.getCoreId(methodClass, methodName, hasTarget, hasReturn, parameters);
        type = TaskType.METHOD;
    }

    public TaskDescription(String namespace, String service, String port, String operation, boolean priority, boolean hasTarget,
            Parameter[] parameters) {
        this.methodName = operation;
        this.priority = priority;
        this.hasTarget = hasTarget;
        this.parameters = parameters;
        if (parameters.length == 0) {
            this.hasReturn = false;
        } else {
            Parameter lastParam = parameters[parameters.length - 1];
            DataType type = lastParam.getType();
            this.hasReturn = (lastParam.getDirection() == DataDirection.OUT && (type == DataType.OBJECT_T || type == DataType.PSCO_T));
        }
        this.coreId = CoreManager.getCoreId(namespace, service, port, operation, hasTarget, hasReturn, parameters);
        this.type = TaskType.SERVICE;
    }

    public Integer getId() {
        return coreId;
    }

    public String getName() {
        return methodName;
    }

    public Parameter[] getParameters() {
        return parameters;
    }

    public boolean hasPriority() {
        return priority;
    }

    public boolean hasTargetObject() {
        return hasTarget;
    }

    public boolean hasReturnValue() {
        return hasReturn;
    }

    public TaskType getType() {
        return this.type;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("[Core id: ").append(getId()).append("]");
        buffer.append(", [Priority: ").append(hasPriority()).append("]");
        buffer.append(", [").append(getName()).append("(");
        int numParams = parameters.length;
        if (hasTargetObject()) {
            numParams--;
        }
        if (hasReturnValue()) {
            numParams--;
        }
        if (numParams > 0) {
            buffer.append(parameters[0].getType());
            for (int i = 1; i < numParams; i++) {
                buffer.append(", ").append(parameters[i].getType());
            }
        }
        buffer.append(")]");
        return buffer.toString();
    }

}
