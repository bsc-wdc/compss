package integratedtoolkit.comm;

import java.util.List;

import integratedtoolkit.exceptions.ConstructConfigurationException;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.resources.configuration.Configuration;
import integratedtoolkit.types.uri.MultiURI;


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
