package integratedtoolkit.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import integratedtoolkit.ITConstants;
import integratedtoolkit.ITConstants.Lang;

import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.annotations.parameter.DataType;


public abstract class AbstractMethodImplementation extends Implementation<MethodResourceDescription> implements Externalizable {

    private static final Lang LANG;


    public enum MethodType {
        METHOD, // For native methods
        MPI, // For MPI methods
        OMPSS, // For OmpSs methods
        OPENCL, // For OpenCL methods
        BINARY, // For binary methods
        DECAF // For decaf methods
    }


    static {
        // Compute language
        Lang l = Lang.JAVA;

        String langProperty = System.getProperty(ITConstants.IT_LANG);
        if (langProperty != null) {
            if (langProperty.equalsIgnoreCase("python")) {
                l = Lang.PYTHON;
            } else if (langProperty.equalsIgnoreCase("c")) {
                l = Lang.C;
            }
        }

        LANG = l;
    }


    public AbstractMethodImplementation() {
        // For externalizable
        super();
    }

    public AbstractMethodImplementation(Integer coreId, Integer implementationId, MethodResourceDescription annot) {
        super(coreId, implementationId, annot);
    }

    public static String getSignature(String declaringClass, String methodName, boolean hasTarget, boolean hasReturn,
            Parameter[] parameters) {

        StringBuilder buffer = new StringBuilder();
        buffer.append(methodName).append("(");

        switch (LANG) {
            case JAVA:
            case C:
                int numPars = parameters.length;
                if (hasTarget) {
                    numPars--;
                }
                if (hasReturn) {
                    numPars--;
                }
                if (numPars > 0) {
                    DataType type = parameters[0].getType();
                    type = (type == DataType.PSCO_T) ? DataType.OBJECT_T : type;
                    buffer.append(type);
                    for (int i = 1; i < numPars; i++) {
                        type = parameters[i].getType();
                        type = (type == DataType.PSCO_T) ? DataType.OBJECT_T : type;
                        buffer.append(",").append(type);
                    }
                }
                break;
            case PYTHON:
                // There is no function overloading in Python
                break;
        }

        buffer.append(")").append(declaringClass);
        return buffer.toString();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

    public abstract MethodType getMethodType();

    public abstract String getMethodDefinition();

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

}
