package integratedtoolkit.ws.master;

import integratedtoolkit.comm.CommAdaptor;
import integratedtoolkit.exceptions.ConstructConfigurationException;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.resources.configuration.Configuration;
import integratedtoolkit.types.resources.jaxb.PriceType;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.ws.master.configuration.WSConfiguration;

import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WSAdaptor implements CommAdaptor {

    // Logging
    public static final Logger logger = LogManager.getLogger(Loggers.COMM);
    public static final boolean debug = logger.isDebugEnabled();

    // Tracing
    protected static boolean tracing;


    @Override
    public void init() {
        try {
            WSJob.init();
        } catch (Exception e) {
            logger.error("Can not initialize WS Adaptor");
        }
    }

    @Override
    public Configuration constructConfiguration(Object project_properties, Object resources_properties) 
            throws ConstructConfigurationException {
        
        integratedtoolkit.types.project.jaxb.ServiceType s_project = 
                (integratedtoolkit.types.project.jaxb.ServiceType) project_properties;
        integratedtoolkit.types.resources.jaxb.ServiceType s_resources = 
                (integratedtoolkit.types.resources.jaxb.ServiceType) resources_properties;

        String wsdl = null;
        if (s_project != null) {
            wsdl = s_project.getWsdl();
        } else if (s_resources != null) {
            wsdl = s_resources.getWsdl();
        } else {
            // No wsdl (service unique key), throw exception
            throw new ConstructConfigurationException("Cannot configure service because no WSDL provided");
        }

        WSConfiguration config = new WSConfiguration(this.getClass().getName(), wsdl);
        if (s_project != null) {
            config.setLimitOfTasks(s_project.getLimitOfTasks());
        }

        if (s_resources != null) {
            config.setServiceName(s_resources.getName());
            config.setNamespace(s_resources.getNamespace());
            String servicePort = s_resources.getPort();
            if (servicePort != null && !servicePort.isEmpty()) {
                config.setPort(s_resources.getPort());
            }
            PriceType p = s_resources.getPrice();
            if (p != null) {
                config.setPricePerUnitTime(p.getPricePerUnit());
                config.setPriceUnitTime(p.getTimeUnit());
            }
        }

        return config;
    }

    @Override
    public COMPSsWorker initWorker(String workerName, Configuration config) {
        return new ServiceInstance(workerName, (WSConfiguration) config);
    }

    @Override
    public void stop() {
        WSJob.end();
    }

    @Override
    public LinkedList<DataOperation> getPending() {
        return null;
    }

    @Override
    public void stopSubmittedJobs() {

    }

    @Override
    public void completeMasterURI(MultiURI u) {
        // No need to do nothing
    }

}
