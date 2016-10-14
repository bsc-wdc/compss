package integratedtoolkit.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import integratedtoolkit.types.resources.MethodResourceDescription;


public class MPIImplementation extends AbstractMethodImplementation implements Externalizable {

    private String mpiRunner;
    private String binary;

    
    public MPIImplementation() {
        // For externalizable
        super();
    }

    public MPIImplementation(String binary, String mpiRunner, Integer coreId, Integer implementationId, MethodResourceDescription annot) {
        super(coreId, implementationId, annot);

        this.mpiRunner = mpiRunner;
        this.binary = binary;
    }

    public String getBinary() {
        return binary;
    }

    public String getMpiRunner() {
        return mpiRunner;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.MPI;
    }
    
    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[MPI RUNNER=").append(mpiRunner);
        sb.append(", BINARY=").append(binary);
        sb.append("]");
        
        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " MPI Method with binary " + binary + " and MPIrunner " + mpiRunner;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        mpiRunner = (String) in.readObject();
        binary = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(mpiRunner);
        out.writeObject(binary);
    }

}
