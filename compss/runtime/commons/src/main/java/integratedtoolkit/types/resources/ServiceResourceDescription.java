package integratedtoolkit.types.resources;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Implementation.Type;
import org.w3c.dom.Node;

public class ServiceResourceDescription extends WorkerResourceDescription {

    private final String serviceName;
    private final String namespace;
    private final String port;

    public ServiceResourceDescription(String serviceName, String namespace, String port) {
        this.serviceName = serviceName;
        this.namespace = namespace;
        this.port = port;
    }

    public ServiceResourceDescription(Node n) {
        super(n);
        this.serviceName = "";
        this.namespace = "";
        this.port = "";
    }

    public String getPort() {
        return port;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void hosts(Implementation<?> impl) {
        //Do nothing
    }

    @Override
    public boolean canHost(Implementation<?> impl) {
        if (impl.getType() == Type.SERVICE) {
            ServiceResourceDescription s = (ServiceResourceDescription) impl.getRequirements();
            return s.getServiceName().compareTo(serviceName) == 0
                    && s.getNamespace().compareTo(namespace) == 0
                    && s.getPort().compareTo(port) == 0;
        }
        return false;
    }

    @Override
    public void increase(ResourceDescription rd) {

    }

    @Override
    public void increaseDynamic(ResourceDescription rd) {

    }

    @Override
    public void reduce(ResourceDescription rd) {

    }

    @Override
    public void reduceDynamic(ResourceDescription rd) {

    }

    @Override
    public ResourceDescription getDynamicCommons(ResourceDescription constraints) {
        return new ServiceResourceDescription("", "", "");
    }

    @Override
    public boolean isDynamicUseless() {
        return false;
    }

    public String toString() {
        return "[SERVICE "
                + "NAMESPACE=" + this.namespace + " "
                + "SERVICE_NAME=" + this.getServiceName() + " "
                + "PORT=" + this.port
                + "]";
    }

    @Override
    public ServiceResourceDescription copy() {
        return new ServiceResourceDescription(serviceName, namespace, port);
    }

}
