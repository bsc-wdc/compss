package integratedtoolkit.types.resources;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.resources.configuration.ServiceConfiguration;


public class ServiceWorker extends Worker<ServiceResourceDescription> {

    private String wsdl;

    public ServiceWorker(String wsdl, ServiceResourceDescription description, ServiceConfiguration config) throws Exception {
        super(wsdl, description, config);
        this.wsdl = wsdl;
        
        if (config.getLimitOfTasks() > 0) {
        	this.description.setMaxTaskSlots(config.getLimitOfTasks());
        }
    }

    public ServiceWorker(ServiceWorker sw) {
        super(sw);
        this.wsdl = sw.getWsdl();
    }

    public void setWsdl(String wsdl) {
        this.wsdl = wsdl;
    }

    public String getWsdl() {
        return wsdl;
    }

    public String getServiceName() {
        return description.getServiceName();
    }

    public String getNamespace() {
        return description.getNamespace();
    }

    public String getPort() {
        return description.getPort();
    }

    @Override
    public String getName() {
        return wsdl;
    }

    @Override
    public Integer fitCount(Implementation<?> impl) {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean hasAvailable(ServiceResourceDescription consumption) {
        return true;
    }

    @Override
    public boolean reserveResource(ServiceResourceDescription consumption) {
        return true;
    }

    @Override
    public void releaseResource(ServiceResourceDescription consumption) {
    }

    @Override
    public Type getType() {
        return Type.SERVICE;
    }

    @Override
    public String getMonitoringData(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("<CPU>").append("</CPU>").append("\n");
        sb.append(prefix).append("<Core>").append("</Core>").append("\n");
        sb.append(prefix).append("<Memory>").append("</Memory>").append("\n");
        sb.append(prefix).append("<Disk>").append("</Disk>").append("\n");
        sb.append(prefix).append("<Provider>").append("</Provider>").append("\n");
        sb.append(prefix).append("<Image>").append("</Image>").append("\n");
        return sb.toString();
    }

    @Override
    public int compareTo(Resource t) {
        if (t == null) {
            throw new NullPointerException();
        }
        switch (t.getType()) {
            case SERVICE:
                return getName().compareTo(t.getName());
            case WORKER:
                return -1;
            case MASTER:
                return -1;
            default:
                return getName().compareTo(t.getName());
        }
    }

    @Override
    public boolean canRun(Implementation<?> implementation) {
        switch (implementation.getType()) {
            case SERVICE:
                ServiceResourceDescription s = (ServiceResourceDescription) implementation.getRequirements();
                return (this.description.getNamespace().compareTo(s.getNamespace()) == 0
                        && this.description.getServiceName().compareTo(s.getServiceName()) == 0
                        && this.description.getPort().compareTo(s.getPort()) == 0);
            default:
                return false;
        }
    }

    @Override
    public String getResourceLinks(String prefix) {
        StringBuilder sb = new StringBuilder(super.getResourceLinks(prefix));
        sb.append(prefix).append("TYPE = SERVICE").append("\n");

        return sb.toString();
    }

    @Override
    public Worker<?> getSchedulingCopy() {
        return new ServiceWorker(this);
    }

}
