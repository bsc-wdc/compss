package es.bsc.compss.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.compss.types.resources.MethodResourceDescription;


public class OpenCLImplementation extends AbstractMethodImplementation implements Externalizable {

    private String kernel;
    private String workingDir;


    public OpenCLImplementation() {
        // For externalizable
        super();
    }

    public OpenCLImplementation(String kernel, String workingDir, Integer coreId, Integer implementationId,
            MethodResourceDescription annot) {

        super(coreId, implementationId, annot);

        this.kernel = kernel;
        this.workingDir = workingDir;
    }

    public String getKernel() {
        return this.kernel;
    }

    public String getWorkingDir() {
        return this.workingDir;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.OPENCL;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[KERNEL=").append(kernel);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " OpenCL Method with kernel " + kernel;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        kernel = (String) in.readObject();
        workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(kernel);
        out.writeObject(workingDir);
    }

}
