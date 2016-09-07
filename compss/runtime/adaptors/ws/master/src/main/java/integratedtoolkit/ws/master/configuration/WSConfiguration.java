package integratedtoolkit.ws.master.configuration;

import integratedtoolkit.types.resources.configuration.ServiceConfiguration;


public class WSConfiguration extends ServiceConfiguration {

    private String serviceName;
    private String namespace;
    private String port;

    private int priceUnitTime;
    private float pricePerUnitTime;


    public WSConfiguration(String adaptorName, String wsdl) {
        super(adaptorName, wsdl);
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public int getPriceUnitTime() {
        return priceUnitTime;
    }

    public void setPriceUnitTime(int priceUnitTime) {
        this.priceUnitTime = priceUnitTime;
    }

    public float getPricePerUnitTime() {
        return pricePerUnitTime;
    }

    public void setPricePerUnitTime(float pricePerUnitTime) {
        this.pricePerUnitTime = pricePerUnitTime;
    }

}
