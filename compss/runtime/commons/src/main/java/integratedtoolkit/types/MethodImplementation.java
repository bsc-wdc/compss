package integratedtoolkit.types;

import integratedtoolkit.ITConstants;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.resources.MethodResourceDescription;


public class MethodImplementation extends Implementation<MethodResourceDescription> {

    private static ITConstants.Lang lang;

    static {
        lang = ITConstants.Lang.JAVA;
        String l = System.getProperty(ITConstants.IT_LANG);
        if (l != null) {
            if (("c").equalsIgnoreCase(l)) {
                lang = ITConstants.Lang.C;
            } else if (("python").equalsIgnoreCase(l)) {
                lang = ITConstants.Lang.PYTHON;
            }
        }
    }

    private String declaringClass;

    public MethodImplementation(String methodClass, Integer coreId, Integer implementationId, MethodResourceDescription annot) {
        super(coreId, implementationId, annot);
        this.declaringClass = methodClass;
    }

    public String getDeclaringClass() {
        return declaringClass;
    }

    public void setDeclaringClass(String declaringClass) {
        this.declaringClass = declaringClass;
    }

    public static String getSignature(String declaringClass, String methodName, boolean hasTarget, boolean hasReturn, Parameter[] parameters) {
        StringBuilder buffer = new StringBuilder();

        buffer.append(methodName).append("(");
        // there is no function overloading in Python
        if (lang != ITConstants.Lang.PYTHON) {
            int numPars = parameters.length;
            if (hasTarget) {
                numPars--;
            }
            if (hasReturn) {
                numPars--;
            }
            if (numPars > 0) {
            	DataType type = parameters[0].getType();
            	if (type == DataType.SCO_T) type = DataType.OBJECT_T;
            	if (type == DataType.PSCO_T) type = DataType.OBJECT_T;            	
                buffer.append(type);
                for (int i = 1; i < numPars; i++) {
                	type = parameters[i].getType();
                	if (type == DataType.SCO_T) type = DataType.OBJECT_T;
                	if (type == DataType.PSCO_T) type = DataType.OBJECT_T;
                    buffer.append(",").append(type);
                }
            }
        }
        buffer.append(")").append(declaringClass);
        return buffer.toString();
    }

    @Override
    public Type getType() {
        return Type.METHOD;
    }

    public String toString() {
        return super.toString() + " Method declared in class " + declaringClass + ": " + requirements.toString();
    }

}
