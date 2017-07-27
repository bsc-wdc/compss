package es.bsc.compss.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.compss.types.resources.MethodResourceDescription;

public class MethodImplementation extends AbstractMethodImplementation implements Externalizable {

    private String declaringClass;
    // In C implementations could have different method names
    private String alternativeMethod;

    public MethodImplementation() {
        // For externalizable
        super();
    }

    public MethodImplementation(String methodClass, String altMethodName, Integer coreId, Integer implementationId,
            MethodResourceDescription annot) {

        super(coreId, implementationId, annot);

        this.declaringClass = methodClass;
        this.alternativeMethod = altMethodName;
    }

    public String getDeclaringClass() {
        return declaringClass;
    }

    public void setDeclaringClass(String declaringClass) {
        this.declaringClass = declaringClass;
    }

    public String getAlternativeMethodName() {
        return alternativeMethod;
    }

    public void setAlternativeMethodName(String alternativeMethod) {
        this.alternativeMethod = alternativeMethod;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.METHOD;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[DECLARING CLASS=").append(declaringClass);
        sb.append(", METHOD NAME=").append(alternativeMethod);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " Method declared in class " + declaringClass + "." + alternativeMethod + ": " + requirements.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        declaringClass = (String) in.readObject();
        alternativeMethod = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(declaringClass);
        out.writeObject(alternativeMethod);
    }

}
