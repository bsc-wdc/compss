package integratedtoolkit.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import integratedtoolkit.types.resources.MethodResourceDescription;


public class BinaryImplementation extends AbstractMethodImplementation implements Externalizable {

    private String binary;
    
    
    public BinaryImplementation() {
        // For externalizable
        super();
    }

    public BinaryImplementation(String binary, Integer coreId, Integer implementationId, MethodResourceDescription annot) {
        super(coreId, implementationId, annot);

        this.binary = binary;
    }

    public String getBinary() {
        return binary;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.BINARY;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[BINARY=").append(binary);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " Binary Method with binary " + binary;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        binary = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(binary);
    }

}
