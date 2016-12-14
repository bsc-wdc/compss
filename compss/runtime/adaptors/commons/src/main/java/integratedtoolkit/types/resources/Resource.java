package integratedtoolkit.types.resources;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.exceptions.InitNodeException;
import integratedtoolkit.exceptions.UnstartedNodeException;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.COMPSsNode;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.listener.SafeCopyListener;
import integratedtoolkit.types.data.listener.TracingCopyListener;
import integratedtoolkit.types.data.listener.WorkersDebugInformationListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.types.data.transferable.SafeCopyTransferable;
import integratedtoolkit.types.data.transferable.TracingCopyTransferable;
import integratedtoolkit.types.data.transferable.WorkersDebugInfoCopyTransferable;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobListener;
import integratedtoolkit.types.resources.configuration.Configuration;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.SharedDiskManager;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import integratedtoolkit.util.Tracer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class Resource implements Comparable<Resource> {

    public enum Type {
        MASTER, 
        WORKER, 
        SERVICE
    }


    // Log and debug
    protected static final Logger logger = LogManager.getLogger(Loggers.COMM);
    public static final boolean debug = logger.isDebugEnabled();

    // List of all created resources
    private static final LinkedList<Resource> availableResources = new LinkedList<Resource>();

    private final String name;
    private final COMPSsNode node;
    protected HashMap<String, String> sharedDisks;

    private final LinkedList<LogicalData> obsoletes = new LinkedList<>();
    private final HashSet<LogicalData> privateFiles = new HashSet<>();


    public Resource(String name, Configuration conf, HashMap<String, String> sharedDisks) {
        this.name = name;
        this.node = Comm.initWorker(name, conf);
        this.sharedDisks = sharedDisks;
        SharedDiskManager.addMachine(this);
        availableResources.add(this);
    }

    public Resource(COMPSsNode node, HashMap<String, String> sharedDisks) {
        this.name = node.getName();
        this.node = node;
        this.sharedDisks = sharedDisks;
        SharedDiskManager.addMachine(this);
        availableResources.add(this);
    }

    public Resource(Resource clone) {
        name = clone.name;
        node = clone.node;
        availableResources.add(this);
    }

    /**
     * Starts a resource execution
     * 
     * @throws Exception
     */
    public void start() throws InitNodeException {
        this.node.start();
        if (sharedDisks != null) {
            for (Entry<String, String> disk : sharedDisks.entrySet()) {
                SharedDiskManager.addSharedToMachine(disk.getKey(), disk.getValue(), this);
            }
        }
    }

    /**
     * Returns the Resource associated to the given name @name Null if any resource has been registered with the
     * name @name
     * 
     * @param name
     * @return
     */
    public static Resource getResource(String name) {
        for (Resource r : availableResources) {
            if (r.getName().equals(name)) {
                return r;
            }
        }
        logger.warn("Resource with name "+ name + " not found");
        return null;
    }

    /**
     * Returns all the LogicalData stored in the host
     * 
     * @param
     * @return
     */
    public HashSet<LogicalData> getAllDataFromHost() {
        HashSet<LogicalData> data = new HashSet<>();

        LinkedList<String> sharedDisks = SharedDiskManager.getAllSharedNames(this);
        for (String diskName : sharedDisks) {
            HashSet<LogicalData> sharedData = SharedDiskManager.getAllSharedFiles(diskName);
            if (sharedData != null) {
                data.addAll(sharedData);
            }
        }

        synchronized (privateFiles) {
            data.addAll(privateFiles);
        }

        return data;
    }

    /**
     * Adds a new LogicalData available in the host
     * 
     * @param ld
     */
    public void addLogicalData(LogicalData ld) {
        synchronized (privateFiles) {
            privateFiles.add(ld);
        }
    }

    /**
     * Marks a file as obsolete
     * 
     * @param obsolete
     */
    public final void addObsolete(LogicalData obsolete) {
        synchronized (obsoletes) {
            obsoletes.add(obsolete);
        }

        // Remove from private files
        synchronized (privateFiles) {
            this.privateFiles.remove(obsolete);
        }

        // Remove from shared disk files
        LinkedList<String> sharedDisks = SharedDiskManager.getAllSharedNames(this);
        for (String diskName : sharedDisks) {
            SharedDiskManager.removeLogicalData(diskName, obsolete);
        }
    }

    /**
     * Clears the list of obsolete files
     * 
     * @return
     */
    public final LinkedList<LogicalData> clearObsoletes() {
        synchronized (obsoletes) {
            LinkedList<LogicalData> obs = obsoletes;
            obsoletes.clear();
            return obs;
        }
    }

    public String getName() {
        return name;
    }

    public COMPSsNode getNode() {
        return node;
    }

    public void setInternalURI(MultiURI u) throws UnstartedNodeException {
        node.setInternalURI(u);
    }

    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation<?> impl, List<String> slaveWorkersNodeNames,
            JobListener listener) {
        
        return node.newJob(taskId, taskParams, impl, this, slaveWorkersNodeNames, listener);
    }
    
    public Job<?> newSlaveJob(int taskId, TaskDescription taskParams, Implementation<?> impl, JobListener listener) {
        return node.newSlaveJob(taskId, taskParams, impl, this, listener);
    }

    public void getData(String dataId, String tgtDataId, Transferable reason, EventListener listener) {
        LogicalData srcData = Comm.getData(dataId);
        LogicalData tgtData = null;
        if (tgtDataId != null) {
            tgtData = Comm.getData(tgtDataId);
        }
        getData(srcData, dataId, tgtData, reason, listener);
    }

    public void getData(LogicalData ld, LogicalData tgtData, Transferable reason, EventListener listener) {
        getData(ld, ld.getName(), tgtData, reason, listener);
    }

    public void getData(String dataId, String newName, String tgtDataId, Transferable reason, EventListener listener) {
        LogicalData srcData = Comm.getData(dataId);
        LogicalData tgtData = Comm.getData(tgtDataId);
        this.getData(srcData, newName, tgtData, reason, listener);
    }

    public void getData(String dataId, String newName, LogicalData tgtData, Transferable reason, EventListener listener) {
        LogicalData ld = Comm.getData(dataId);
        this.getData(ld, newName, tgtData, reason, listener);
    }

    public void getData(LogicalData ld, String newName, LogicalData tgtData, Transferable reason, EventListener listener) {
        SimpleURI workingPath = node.getCompletePath(reason.getType(), newName);
        DataLocation target = null;
        try {
            target = DataLocation.createLocation(this, workingPath);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + workingPath.toString(), e);
        }

        getData(ld, target, tgtData, reason, listener);
    }

    public void getData(String dataId, DataLocation target, Transferable reason, EventListener listener) {
        LogicalData srcData = Comm.getData(dataId);
        getData(srcData, target, srcData, reason, listener);
    }

    public void getData(String dataId, DataLocation target, String tgtDataId, Transferable reason, EventListener listener) {
        LogicalData srcData = Comm.getData(dataId);
        LogicalData tgtData = Comm.getData(tgtDataId);
        getData(srcData, target, tgtData, reason, listener);
    }

    public void getData(String dataId, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener) {
        LogicalData ld = Comm.getData(dataId);
        getData(ld, target, tgtData, reason, listener);
    }

    public void getData(LogicalData srcData, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener) {
        node.obtainData(srcData, null, target, tgtData, reason, listener);
    }

    public SimpleURI getCompleteRemotePath(DataType type, String name) {
        return node.getCompletePath(type, name);
    }

    public void retrieveData(boolean saveUniqueData) {
        if (debug) {
            logger.debug("Retrieving data resource " + this.getName());
        }
        Semaphore sem = new Semaphore(0);
        SafeCopyListener listener = new SafeCopyListener(sem);
        HashSet<LogicalData> lds = getAllDataFromHost();
        HashMap<String, String> disks = SharedDiskManager.terminate(this);
        COMPSsNode masterNode = Comm.getAppHost().getNode();
        for (LogicalData ld : lds) {
            ld.notifyToInProgressCopiesEnd(listener);
            DataLocation lastLoc = ld.removeHostAndCheckLocationToSave(this, disks);
            if (lastLoc != null && saveUniqueData) {
                listener.addOperation();

                DataLocation safeLoc = null;
                String safePath = Protocol.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + ld.getName();
                try {
                    SimpleURI uri = new SimpleURI(safePath);
                    safeLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + safePath, e);
                }

                masterNode.obtainData(ld, lastLoc, safeLoc, ld, new SafeCopyTransferable(), listener);
            }
        }

        if (debug) {
            logger.debug("Waiting for finishing saving copies for " + this.getName());
        }
        listener.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            logger.error("Error waiting for files in resource " + getName() + " to get saved");
        }

        if (debug) {
            logger.debug("Unique files saved for " + this.getName());
        }

        if (this.getType() != Type.SERVICE) {

            if (Tracer.isActivated()) {
                if (node.generatePackage()){
                	getTracingPackageToMaster();
                	if (debug) {
                		logger.debug("Tracing package obtained for " + this.getName());
                	}
                }
            }
            if (debug) {
                if (node.generateWorkersDebugInfo()){
                	getWorkersDebugInfo();
                	logger.debug("Workers Debug files obtained for " + this.getName());
                }
            }
        }
    }

    public void stop(ShutdownListener sl) {
        this.deleteIntermediate();
        sl.addOperation();
        node.stop(sl);
    }

    private void getTracingPackageToMaster() {
        COMPSsNode masterNode = Comm.getAppHost().getNode();
        Semaphore sem = new Semaphore(0);
        String fileName = getName() + "_compss_trace.tar.gz";
        SimpleURI fileOriginURI = node.getCompletePath(DataType.FILE_T, fileName);

        if (debug) {
            logger.debug("Copying tracing package from : " + fileOriginURI.getPath() + ",to : " + Comm.getAppHost().getAppLogDirPath() + "trace"
                    + File.separator + fileName);
        }

        TracingCopyListener tracingListener = new TracingCopyListener(sem);

        tracingListener.addOperation();

        // Source data location
        DataLocation source;
        try {
            source = DataLocation.createLocation(this, fileOriginURI);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + fileOriginURI.getPath(), e);
            return;
        }

        // Target data location
        DataLocation tgt;
        String targetPath = Protocol.FILE_URI.getSchema() + Comm.getAppHost().getAppLogDirPath() + "trace" + File.separator + fileName;
        try {
            SimpleURI uri = new SimpleURI(targetPath);
            tgt = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath);
            return;
        }

        // Ask for data
        masterNode.obtainData(new LogicalData("tracing" + this.getName()), source, tgt, new LogicalData("tracing" + this.getName()),
                new TracingCopyTransferable(), tracingListener);

        tracingListener.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            logger.error("Error waiting for tracing files in resource " + getName() + " to get saved");
        }
        if (debug) {
            logger.debug("Removing " + this.getName() + " tracing temporary files");
        }

        File f = null;
        try {
            f = new File(source.getPath());
            if (!f.delete()) {
                logger.error("Unable to remove tracing temporary files of node " + this.getName());
            }
        } catch (Exception e) {
            logger.error("Unable to remove tracing temporary files of node " + this.getName(), e);
        }
    }

    private void getWorkersDebugInfo() {
        if (debug) {
            logger.debug("Copying Workers Information");
        }

        COMPSsNode masterNode = Comm.getAppHost().getNode();
        Semaphore sem = new Semaphore(0);
        WorkersDebugInformationListener wdil = new WorkersDebugInformationListener(sem);

        // Get Worker output
        wdil.addOperation();
        String outFileName = "worker_" + getName() + ".out";
        SimpleURI outFileOrigin = node.getCompletePath(DataType.FILE_T, "log" + File.separator + "static_" + outFileName);
        String outFileTarget = Protocol.FILE_URI.getSchema() + Comm.getAppHost().getWorkersDirPath() + File.separator + outFileName;

        DataLocation outSource = null;
        try {
            outSource = DataLocation.createLocation(this, outFileOrigin);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + outFileOrigin.toString(), e);
        }

        DataLocation outTarget = null;
        try {
            SimpleURI uri = new SimpleURI(outFileTarget);
            outTarget = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + outFileTarget);
        }

        logger.debug("- Source: " + outFileOrigin);
        logger.debug("- Target: " + outFileTarget);
        masterNode.obtainData(new LogicalData("workerOut" + this.getName()), outSource, outTarget,
                new LogicalData("workerOut" + this.getName()), new WorkersDebugInfoCopyTransferable(), wdil);

        // Get Worker error
        wdil.addOperation();
        String errFileName = "worker_" + getName() + ".err";
        SimpleURI errFileOrigin = node.getCompletePath(DataType.FILE_T, "log" + File.separator + "static_" + errFileName);
        String errFileTarget = Protocol.FILE_URI.getSchema() + Comm.getAppHost().getWorkersDirPath() + File.separator + errFileName;

        DataLocation errSource = null;
        try {
            errSource = DataLocation.createLocation(this, errFileOrigin);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + errFileOrigin.toString(), e);
        }

        DataLocation errTarget = null;
        try {
            SimpleURI uri = new SimpleURI(errFileTarget);
            errTarget = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + errFileTarget);
        }

        logger.debug("- Source: " + errFileOrigin);
        logger.debug("- Target: " + errFileTarget);
        masterNode.obtainData(new LogicalData("workerErr" + this.getName()), errSource, errTarget,
                new LogicalData("workerErr" + this.getName()), new WorkersDebugInfoCopyTransferable(), wdil);

        // Wait transfers
        wdil.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            logger.error("Error waiting for worker debug files in resource " + getName() + " to get saved");
        }

        logger.debug("Worker files from resource " + getName() + "received");
    }

    public abstract Type getType();

    @Override
    public abstract int compareTo(Resource t);

    public void deleteIntermediate() {
        node.deleteTemporary();
    }

}
