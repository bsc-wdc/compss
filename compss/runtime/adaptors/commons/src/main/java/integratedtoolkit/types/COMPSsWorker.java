package integratedtoolkit.types;

import integratedtoolkit.exceptions.AnnounceException;
import integratedtoolkit.types.resources.configuration.Configuration;


/**
 * Abstract definition of a COMPSs Worker
 *
 */
public abstract class COMPSsWorker extends COMPSsNode {

    /**
     * New worker with name @name and configuration @config
     * 
     * @param name
     * @param config
     */
    public COMPSsWorker(String name, Configuration config) {
        super();
    }

    /**
     * Returns the worker user
     * 
     * @return
     */
    public abstract String getUser();

    /**
     * Returns the worker classpath
     * 
     * @return
     */
    public abstract String getClasspath();

    /**
     * Returns the worker pythonpath
     * 
     * @return
     */
    public abstract String getPythonpath();

    /**
     * Updates the task count to @processorCoreCount
     * 
     * @param processorCoreCount
     */
    public abstract void updateTaskCount(int processorCoreCount);

    /**
     * Announces the worker destruction
     * 
     * @throws AnnounceException
     */
    public abstract void announceDestruction() throws AnnounceException;

    /**
     * Announces the worker creation
     * 
     * @throws AnnounceException
     */
    public abstract void announceCreation() throws AnnounceException;

}
