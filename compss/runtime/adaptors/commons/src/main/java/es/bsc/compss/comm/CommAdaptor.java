package es.bsc.compss.comm;

import java.util.List;

import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.uri.MultiURI;


/**
 * Abstract definition of a Communication Adaptor for the Runtime
 *
 */
public interface CommAdaptor {

    /**
     * Initializes the Communication Adaptor
     */
    public void init();

    /**
     * Creates a configuration instance for the specific adaptor
     * 
     * @param project_properties
     * @param resources_properties
     * @return
     * @throws Exception
     */
    public Configuration constructConfiguration(Object project_properties, Object resources_properties)
            throws ConstructConfigurationException;

    /**
     * Initializes a worker through an adaptor
     * 
     * @param workerName
     * @param config
     * @return
     */
    public COMPSsWorker initWorker(String workerName, Configuration config);

    /**
     * Stops the Communication Adaptor
     */
    public void stop();

    /**
     * Retrieves all the pending operations
     * 
     * @return
     */
    public List<DataOperation> getPending();

    /**
     * Returns the complete Master URI
     * 
     * @param u
     */
    public void completeMasterURI(MultiURI u);

    /**
     * Stops all the pending jobs inside the Communication Adaptor
     */
    public void stopSubmittedJobs();

}
