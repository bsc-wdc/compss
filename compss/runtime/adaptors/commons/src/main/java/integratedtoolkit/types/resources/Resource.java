package integratedtoolkit.types.resources;

import integratedtoolkit.ITConstants;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.exceptions.UnstartedNodeException;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.COMPSsNode;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.types.data.operation.DataOperation.EventListener;
import integratedtoolkit.types.data.operation.SafeCopyListener;
import integratedtoolkit.types.data.operation.SafeCopyTransferable;
import integratedtoolkit.types.data.operation.TracingCopyListener;
import integratedtoolkit.types.data.operation.TracingCopyTransferable;
import integratedtoolkit.types.data.operation.WorkersDebugInfoCopyTransferable;
import integratedtoolkit.types.data.operation.WorkersDebugInformationListener;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.Job.JobListener;
import integratedtoolkit.types.resources.configuration.Configuration;
import integratedtoolkit.util.SharedDiskManager;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;


public abstract class Resource implements Comparable<Resource> {

    public enum Type {
        MASTER,
        WORKER,
        SERVICE
    }

    // Tracing
    protected static final boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;

    // Log and debug
    protected static final Logger logger = Logger.getLogger(Loggers.COMM);
    public static final boolean debug = logger.isDebugEnabled();

    private final String name;
    private final COMPSsNode node;
    protected HashMap<String, String> sharedDisks;

    private LinkedList<String> obsoletes = new LinkedList<String>();

    public Resource(String name, Configuration conf, HashMap<String, String> sharedDisks) {
        this.name = name;
        this.node = Comm.initWorker(name, conf);
        this.sharedDisks = sharedDisks;
        SharedDiskManager.addMachine(this);
    }

    public Resource(COMPSsNode node, HashMap<String, String> sharedDisks) {
        this.name = node.getName();
        this.node = node;
        this.sharedDisks = sharedDisks;
        SharedDiskManager.addMachine(this);
    }

    public Resource(Resource clone) {
        name = clone.name;
        node = clone.node;
    }

    public void start() throws Exception {
        this.node.start();
        if (sharedDisks != null){
        	for (Entry<String, String> disk : sharedDisks.entrySet()) {
        		SharedDiskManager.addSharedToMachine(disk.getKey(), disk.getValue(), this);
        	}
    	}
    }

    public final void addObsolete(String obsoleteFile) {
        synchronized (obsoletes) {
            obsoletes.add(obsoleteFile);
        }
    }

    public final void addObsoletes(LinkedList<String> obsoleteFiles) {
        synchronized (obsoletes) {
            obsoletes.addAll(obsoleteFiles);
        }
    }

    public final LinkedList<String> clearObsoletes() {
        synchronized (obsoletes) {
            LinkedList<String> obs = obsoletes;
            obsoletes = new LinkedList<String>();
            return obs;
        }
    }
    
    public String getName() {
        return name;
    }

    public COMPSsNode getNode() {
        return node;
    }

    public void setInternalURI(URI u) throws UnstartedNodeException {
        node.setInternalURI(u);
    }

    public Job<?> newJob(int taskId, TaskParams taskParams, Implementation<?> impl, JobListener listener) {
        return node.newJob(taskId, taskParams, impl, this, listener);
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
        String workingPath = node.getCompletePath(reason.getType(), newName);
        DataLocation target = DataLocation.getLocation(this, workingPath);
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

    public String getCompleteRemotePath(DataType type, String name) {
        return node.getCompletePath(type, name);
    }
    
    public void retrieveData(boolean saveUniqueData){
    	if (debug) {
            logger.debug("retriving data resource " + this.getName());
        }
        Semaphore sem = new Semaphore(0);
        SafeCopyListener listener = new SafeCopyListener(sem);
        HashSet<LogicalData> lds = LogicalData.getAllDataFromHost(this);
        HashMap<String, String> disks = SharedDiskManager.terminate(this);
        COMPSsNode masterNode = Comm.appHost.getNode();
        for (LogicalData ld : lds) {
            ld.notifyToInProgressCopiesEnd(listener);
            DataLocation lastLoc = ld.removeHostAndCheckLocationToSave(this, disks);
            if (lastLoc != null && saveUniqueData) {
                listener.addOperation();
                DataLocation safeLoc = DataLocation.getLocation(Comm.appHost, Comm.appHost.getTempDirPath() + ld.getName());
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
            if (tracing) {
                node.generatePackage();
                getTracingPackageToMaster();
                if (debug) {
                    logger.debug("Tracing package obtained for " + this.getName());
                }
            }
            if (debug) {
                node.generateWorkersDebugInfo();
                getWorkersDebugInfo();
                logger.debug("Workers Debug files obtained for " + this.getName());
            }
        }
    }
    
    public void stop(ShutdownListener sl) {

        this.deleteIntermediate();

        sl.addOperation();
        node.stop(sl);
    }

    private void getTracingPackageToMaster() {
        COMPSsNode masterNode = Comm.appHost.getNode();
        Semaphore sem = new Semaphore(0);
        String fileName = getName() + "_compss_trace.tar.gz";
        String fileOriginPath = node.getCompletePath(DataType.FILE_T, fileName);

        if (debug) {
            logger.debug("Copying tracing package from : " + fileOriginPath + ",to : " + Comm.appHost.getAppLogDirPath() + "trace" + File.separator + fileName);
        }

        TracingCopyListener tracingListener = new TracingCopyListener(sem);

        tracingListener.addOperation();
        DataLocation source = DataLocation.getLocation(this, fileOriginPath);
        DataLocation tgt = DataLocation.getLocation(Comm.appHost, Comm.appHost.getAppLogDirPath() + "trace" + File.separator + fileName);

        masterNode.obtainData(new LogicalData("tracing" + this.getName()), source, tgt, new LogicalData("tracing" + this.getName()), new TracingCopyTransferable(), tracingListener);

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
            f.delete();
        } catch (Exception e) {
            logger.error("Unable to remove tracing temporary files of node " + this.getName());
        }
    }

    private void getWorkersDebugInfo() {
        if (debug) {
            logger.debug("Copying Workers Information");
        }

        COMPSsNode masterNode = Comm.appHost.getNode();
        Semaphore sem = new Semaphore(0);
        WorkersDebugInformationListener wdil = new WorkersDebugInformationListener(sem);

        // Get Worker output
        wdil.addOperation();
        String outFileName = "worker_" + getName() + ".out";
        String outFileOrigin = node.getCompletePath(DataType.FILE_T, "log" + File.separator + "static_" + outFileName);
        String outFileTarget = Comm.appHost.getWorkersDirPath() + File.separator + outFileName;
        DataLocation outSource = DataLocation.getLocation(this, outFileOrigin);
        DataLocation outTarget = DataLocation.getLocation(Comm.appHost, outFileTarget);
        logger.debug("- Source: " + outFileOrigin);
        logger.debug("- Target: " + outFileTarget);
        masterNode.obtainData(new LogicalData("workerOut" + this.getName()), outSource, outTarget, new LogicalData("workerOut" + this.getName()), new WorkersDebugInfoCopyTransferable(), wdil);

        // Get Worker error
        wdil.addOperation();
        String errFileName = "worker_" + getName() + ".err";
        String errFileOrigin = node.getCompletePath(DataType.FILE_T, "log" + File.separator + "static_" + errFileName);
        String errFileTarget = Comm.appHost.getWorkersDirPath() + File.separator + errFileName;
        DataLocation errSource = DataLocation.getLocation(this, errFileOrigin);
        DataLocation errTarget = DataLocation.getLocation(Comm.appHost, errFileTarget);
        logger.debug("- Source: " + errFileOrigin);
        logger.debug("- Target: " + errFileTarget);
        masterNode.obtainData(new LogicalData("workerErr" + this.getName()), errSource, errTarget, new LogicalData("workerErr" + this.getName()), new WorkersDebugInfoCopyTransferable(), wdil);

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

    public abstract int compareTo(Resource t);

    public void deleteIntermediate() {
        node.deleteTemporary();
    }

}
