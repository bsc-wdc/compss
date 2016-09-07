package integratedtoolkit.types.resources.configuration;

public class ServiceConfiguration extends Configuration {

    private final String wsdl;


    public ServiceConfiguration(String adaptorName, String wsdl) {
        super(adaptorName);
        this.wsdl = wsdl;
    }

    public String getWsdl() {
        return wsdl;
    }

}
