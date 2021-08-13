package es.bsc.compss.types.resources.configuration;

import java.util.List;


public class HTTPConfiguration extends Configuration {

    private String baseUrl;
    private List<String> services;


    public HTTPConfiguration(String adaptorName, String baseUrl) {
        super(adaptorName);
        this.baseUrl = baseUrl;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public List<String> getServices() {
        return services;
    }

    public void setServices(List<String> services) {
        this.services = services;
    }
}
