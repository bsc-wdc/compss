package es.bsc.compss.types.resources;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.Implementation.TaskType;

public class ServiceResourceDescription extends WorkerResourceDescription {

    private final String serviceName;
    private final String namespace;
    private final String port;

    private int connections;

    public ServiceResourceDescription(String serviceName, String namespace, String port, int connections) {
        this.serviceName = serviceName;
        this.namespace = namespace;
        this.port = port;
        this.connections = connections;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getPort() {
        return port;
    }

    @Override
    public boolean canHost(Implementation impl) {
        if (impl.getTaskType() == TaskType.SERVICE) {
            ServiceResourceDescription s = (ServiceResourceDescription) impl.getRequirements();
            return s.serviceName.compareTo(serviceName) == 0 && s.namespace.compareTo(namespace) == 0 && s.port.compareTo(port) == 0
                    && s.connections < this.connections;
        }
        return false;
    }

    @Override
    public boolean canHostDynamic(Implementation impl) {
        int conRequired = ((ServiceResourceDescription) impl.getRequirements()).connections;
        return conRequired <= connections;
    }

    @Override
    public void increase(ResourceDescription rd) {
        ServiceResourceDescription srd = (ServiceResourceDescription) rd;
        this.connections += srd.connections;
    }

    @Override
    public void increaseDynamic(ResourceDescription rd) {
        ServiceResourceDescription srd = (ServiceResourceDescription) rd;
        this.connections += srd.connections;
    }

    @Override
    public void reduce(ResourceDescription rd) {
        ServiceResourceDescription srd = (ServiceResourceDescription) rd;
        this.connections -= srd.connections;
    }

    @Override
    public ResourceDescription reduceDynamic(ResourceDescription rd) {
        ServiceResourceDescription srd = (ServiceResourceDescription) rd;
        this.connections -= srd.connections;
        return new ServiceResourceDescription("", "", "", srd.connections);
    }

    @Override
    public ResourceDescription getDynamicCommons(ResourceDescription constraints) {
        ServiceResourceDescription sConstraints = (ServiceResourceDescription) constraints;
        int conCommons = Math.min(sConstraints.connections, this.connections);
        return new ServiceResourceDescription("", "", "", conCommons);
    }

    @Override
    public boolean isDynamicUseless() {
        return connections == 0;
    }

    @Override
    public boolean isDynamicConsuming() {
        return connections > 0;
    }

    @Override
    public String toString() {
        return "[SERVICE " + "NAMESPACE=" + this.namespace + " " + "SERVICE_NAME=" + this.getServiceName() + " " + "PORT=" + this.port + " "
                + "CONNECTIONS=" + this.connections + "]";
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Nothing to serialize since it is never used
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // Nothing to serialize since it is never used
    }

    @Override
    public ServiceResourceDescription copy() {
        return new ServiceResourceDescription(serviceName, namespace, port, connections);
    }

    @Override
    public String getDynamicDescription() {
        return "Connections:" + this.connections;
    }
}
