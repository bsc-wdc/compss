package integratedtoolkit.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.resources.ServiceResourceDescription;


public class ServiceImplementation extends Implementation<ServiceResourceDescription> implements Externalizable {

    private String operation;


    public ServiceImplementation() {
        // For externalizable
        super();
    }
    
    public ServiceImplementation(Integer coreId, String namespace, String service, String port, String operation) {
        super(coreId, 0, null);
        
        this.requirements = new ServiceResourceDescription(service, namespace, port, 1);
        this.operation = operation;
    }

    public String getOperation() {
        return operation;
    }

    public static String getSignature(String namespace, String serviceName, String portName, String operation, boolean hasTarget,
            boolean hasReturn, Parameter[] parameters) {
        StringBuilder buffer = new StringBuilder();

        buffer.append(operation).append("(");
        int numPars = parameters.length;
        if (hasTarget) {
            numPars--;
        }
        if (hasReturn) {
            numPars--;
        }
        if (numPars > 0) {
            buffer.append(parameters[0].getType());
            for (int i = 1; i < numPars; i++) {
                buffer.append(",").append(parameters[i].getType());
            }
        }
        buffer.append(")").append(namespace).append(',').append(serviceName).append(',').append(portName);

        return buffer.toString();
    }
    
    @Override
    public TaskType getTaskType() {
        return TaskType.SERVICE;
    }

    @Override
    public String toString() {
        ServiceResourceDescription description = this.requirements;
        return super.toString() + " Service in namespace " + description.getNamespace() + " with name " + description.getPort()
                + " on port " + description.getPort() + "and operation " + operation;
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.operation = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.operation);
    }

}
