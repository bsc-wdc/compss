package integratedtoolkit.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import integratedtoolkit.types.resources.MethodResourceDescription;


public class OpenCLImplementation extends AbstractMethodImplementation implements Externalizable {

    private String kernel;


    public OpenCLImplementation() {
        // For externalizable
        super();
    }
    
    public OpenCLImplementation(String kernel, Integer coreId, Integer implementationId, MethodResourceDescription annot) {
        super(coreId, implementationId, annot);

        this.kernel = kernel;
    }

    public String getKernel() {
        return kernel;
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
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(kernel);
    }

}
