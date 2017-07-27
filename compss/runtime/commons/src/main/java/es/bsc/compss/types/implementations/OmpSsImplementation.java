package es.bsc.compss.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.compss.types.resources.MethodResourceDescription;


public class OmpSsImplementation extends AbstractMethodImplementation implements Externalizable {

    private String binary;
    private String workingDir;


    public OmpSsImplementation() {
        // For externalizable
        super();
    }

    public OmpSsImplementation(String binary, String workingDir, Integer coreId, Integer implementationId,
            MethodResourceDescription annot) {
        super(coreId, implementationId, annot);

        this.binary = binary;
        this.workingDir = workingDir;
    }

    public String getBinary() {
        return this.binary;
    }

    public String getWorkingDir() {
        return this.workingDir;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.OMPSS;
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
        return super.toString() + " OmpSs Method with binary " + binary;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        binary = (String) in.readObject();
        workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(binary);
        out.writeObject(workingDir);
    }

}
