package integratedtoolkit.types;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.exceptions.UnstartedNodeException;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.Job.JobListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class COMPSsNode implements Comparable<COMPSsNode> {

    // Log and debug
    protected static final Logger logger = LogManager.getLogger(Loggers.COMM);
    public static final boolean debug = logger.isDebugEnabled();

    protected static final String DELETE_ERR = "Error deleting intermediate files";
    protected static final String URI_CREATION_ERR = "Error creating new URI";


    public COMPSsNode() {
    }

    public abstract String getName();

    public abstract void start() throws Exception;

    public abstract void setInternalURI(MultiURI u) throws UnstartedNodeException;

    public abstract Job<?> newJob(int taskId, TaskDescription taskparams, Implementation<?> impl, Resource res, JobListener listener);

    public abstract void sendData(LogicalData srcData, DataLocation loc, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener);

    public abstract void obtainData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener);

    public abstract void stop(ShutdownListener sl);

    public abstract SimpleURI getCompletePath(DataType type, String name);

    public abstract void deleteTemporary();

    public abstract void generatePackage();

    public abstract void generateWorkersDebugInfo();

    @Override
    public int compareTo(COMPSsNode host) {
        return getName().compareTo(host.getName());
    }

}
