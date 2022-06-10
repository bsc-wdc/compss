/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.types.resources;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.listener.SafeCopyListener;
import es.bsc.compss.types.data.listener.TracingCopyListener;
import es.bsc.compss.types.data.listener.WorkersDebugInformationListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.LocationType;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.transferable.SafeCopyTransferable;
import es.bsc.compss.types.data.transferable.TracingCopyTransferable;
import es.bsc.compss.types.data.transferable.WorkersDebugInfoCopyTransferable;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.util.SharedDiskManager;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class ResourceImpl implements Comparable<Resource>, Resource, NodeMonitor {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    public static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final boolean CACHE_PROFILING_ENABLED =
        Boolean.parseBoolean(System.getProperty(COMPSsConstants.PYTHON_CACHE_PROFILER));
    protected final String name;
    private final COMPSsNode node;
    protected Map<String, String> sharedDisks;

    private final List<LogicalData> obsoletes = new LinkedList<>();
    private final Set<LogicalData> privateFiles = new HashSet<>();
    private boolean isLost = false;


    /**
     * Creates a new ResourceImplementation instance.
     *
     * @param name Resource name.
     * @param conf Resource configuration.
     * @param sharedDisks Mounted shared disks.
     */
    public ResourceImpl(String name, Configuration conf, Map<String, String> sharedDisks) {
        this.name = name;
        this.node = Comm.initWorker(conf, this);
        this.sharedDisks = sharedDisks;
        SharedDiskManager.addMachine(this);
        ResourcesPool.add(this);
    }

    /**
     * Creates a new ResourceImplementation instance.
     *
     * @param node COMPSs node.
     * @param sharedDisks Mounter shared disks.
     */
    public ResourceImpl(COMPSsNode node, Map<String, String> sharedDisks) {
        this.name = node.getName();
        this.node = node;
        node.setMonitor(this);
        this.sharedDisks = sharedDisks;
        SharedDiskManager.addMachine(this);
        ResourcesPool.add(this);
    }

    /**
     * Clones the given ResourceImpl.
     *
     * @param clone ResourceImpl to clone.
     */
    public ResourceImpl(ResourceImpl clone) {
        this.name = clone.name;
        this.node = clone.node;
        ResourcesPool.add(this);
    }

    @Override
    public void start() throws InitNodeException {
        this.node.start();
        if (this.sharedDisks != null) {
            for (Entry<String, String> disk : this.sharedDisks.entrySet()) {
                SharedDiskManager.addSharedToMachine(disk.getKey(), disk.getValue(), this);
            }
        }
    }

    @Override
    public Set<LogicalData> getAllDataFromHost() {
        Set<LogicalData> data = new HashSet<>();

        List<String> sharedDisks = SharedDiskManager.getAllSharedNames(this);
        for (String diskName : sharedDisks) {
            Set<LogicalData> sharedData = SharedDiskManager.getAllSharedFiles(diskName);
            if (sharedData != null) {
                synchronized (sharedData) {
                    data.addAll(sharedData);
                }
            }
        }

        synchronized (this.privateFiles) {
            data.addAll(this.privateFiles);
        }

        return data;
    }

    @Override
    public void addLogicalData(LogicalData ld) {
        synchronized (this.privateFiles) {
            this.privateFiles.add(ld);
        }
    }

    @Override
    public void removeLogicalData(LogicalData ld) {
        synchronized (this.privateFiles) {
            this.privateFiles.remove(ld);
        }
    }

    @Override
    public final void addObsolete(LogicalData obsolete) {
        if (getType() == ResourceType.WORKER) {
            synchronized (this.obsoletes) {
                this.obsoletes.add(obsolete);
            }
        }

        // Remove from private files
        removeLogicalData(obsolete);

        // Remove from shared disk files
        List<String> sharedDisks = SharedDiskManager.getAllSharedNames(this);
        for (String diskName : sharedDisks) {
            SharedDiskManager.removeLogicalData(diskName, obsolete);
        }

    }

    @Override
    public final List<MultiURI> pollObsoletes() {
        LogicalData[] obs = null;
        synchronized (this.obsoletes) {
            obs = this.obsoletes.toArray(new LogicalData[this.obsoletes.size()]);
            this.obsoletes.clear();
        }
        List<MultiURI> obsoleteRenamings = new LinkedList<>();
        for (LogicalData ld : obs) {
            for (MultiURI u : ld.getURIsInHost((Resource) this)) {
                if (u != null) {
                    obsoleteRenamings.add(u);
                }
            }
        }

        return obsoleteRenamings;
    }

    /**
     * Clears the list of obsolete files.
     */
    @Override
    public final void clearObsoletes() {
        synchronized (this.obsoletes) {
            this.obsoletes.clear();
        }
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public COMPSsNode getNode() {
        return this.node;
    }

    @Override
    public void setInternalURI(MultiURI u) throws UnstartedNodeException {
        this.node.setInternalURI(u);
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {

        return this.node.newJob(taskId, taskParams, impl, this, slaveWorkersNodeNames, listener, predecessors,
            numSuccessors);
    }

    @Override
    public void getData(LogicalData srcData, Transferable reason, EventListener listener) {
        getData(srcData, srcData.getName(), srcData, reason, listener);
    }

    @Override
    public void getData(LogicalData srcData, String newName, Transferable reason, EventListener listener) {
        getData(srcData, newName, srcData, reason, listener);
    }

    @Override
    public void getData(LogicalData srcData, DataLocation target, Transferable reason, EventListener listener) {
        getData(srcData, target, srcData, reason, listener);
    }

    @Override
    public void getData(LogicalData srcData, LogicalData tgtData, Transferable reason, EventListener listener) {
        getData(srcData, srcData.getName(), tgtData, reason, listener);
    }

    @Override
    public void getData(LogicalData ld, String newName, LogicalData tgtData, Transferable reason,
        EventListener listener) {

        if (reason.getType() == DataType.BINDING_OBJECT_T) {
            if (ld.getValue() == null) {
                LOGGER.warn("[Resource] Getting data: " + newName
                    + ", source logical data value is null. Trying with data target from reason ");
                BindingObject bo = BindingObject.generate((String) reason.getDataTarget());
                newName = newName + "#" + bo.getType() + "#" + bo.getElements();
            } else {
                BindingObject bo = BindingObject.generate((String) ld.getValue());
                newName = newName + "#" + bo.getType() + "#" + bo.getElements();
            }
        }
        // for HTTP tasks, the target location should be on the Master, see HTTPInstance.getTargetLocation
        DataLocation target = this.node.getTargetLocation(this, reason.getType(), newName);
        getData(ld, target, tgtData, reason, listener);
    }

    @Override
    public void getData(LogicalData srcData, DataLocation target, LogicalData tgtData, Transferable reason,
        EventListener listener) {

        this.node.obtainData(srcData, null, target, tgtData, reason, listener);
    }

    @Override
    public void enforceDataObtaning(Transferable t, EventListener listener) {
        this.node.enforceDataObtaining(t, listener);
    }

    @Override
    public SimpleURI getCompleteRemotePath(DataType type, String name) {
        return this.node.getCompletePath(type, name);
    }

    public String getOutputDataTargetPath(String tgtName, DependencyParameter param) {
        return this.node.getOutputDataTarget(tgtName, param);
    }

    @Override
    public void retrieveUniqueDataValues() {
        if (this.isLost) {
            return;
        }
        COMPSsNode masterNode = Comm.getAppHost().getNode();
        if (this.getNode().compareTo(masterNode) == 0) {
            if (DEBUG) {
                LOGGER.debug("The resource is part of the master process. No need to retrieve any data value.");
            }
            return;
        }
        if (DEBUG) {
            LOGGER.debug("Retrieving data resource " + this.getName());
        }
        Semaphore sem = new Semaphore(0);
        SafeCopyListener listener = new SafeCopyListener(sem);
        Set<LogicalData> lds = getAllDataFromHost();
        Map<String, String> disks = SharedDiskManager.terminate(this);
        for (LogicalData ld : lds) {
            if (ld.getCopiesInProgress().size() > 0) {
                ld.notifyToInProgressCopiesEnd(listener);
            }
            DataLocation lastLoc = ld.removeHostAndCheckLocationToSave(this, disks);
            if (lastLoc != null) {
                listener.addOperation();

                DataLocation safeLoc = null;
                String safePath = null;
                boolean isBindingData = false;
                if (lastLoc.getType().equals(LocationType.BINDING)) {
                    BindingObject bo = BindingObject.generate(lastLoc.getPath());
                    safePath = ProtocolType.BINDING_URI.getSchema() + Comm.getAppHost().getTempDirPath() + ld.getName()
                        + "#" + bo.getType() + "#" + bo.getElements();
                } else {
                    safePath = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + ld.getName();
                }
                try {
                    SimpleURI uri = new SimpleURI(safePath);
                    safeLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + safePath, e);
                }

                if (isBindingData) {
                    masterNode.obtainData(ld, lastLoc, safeLoc, ld, new SafeCopyTransferable(DataType.BINDING_OBJECT_T),
                        listener);
                } else {
                    masterNode.obtainData(ld, lastLoc, safeLoc, ld, new SafeCopyTransferable(), listener);
                }
            }
        }

        if (DEBUG) {
            LOGGER.debug("Waiting for finishing saving copies for " + this.getName());
        }
        if (listener.getOperations() > 0) {
            listener.enable();
            try {
                sem.acquire();
            } catch (InterruptedException ex) {
                LOGGER.error("Error waiting for files in resource " + getName() + " to get saved");
            }
        }
        if (DEBUG) {
            LOGGER.debug("Unique files saved for " + this.getName());
        }
    }

    @Override
    public void retrieveTracingAndDebugData() {
        if (this.isLost) {
            LOGGER.debug(" Will not retrieve Tracing and Debug Data because the node: " + this.getName() + " is lost.");
            return;
        }
        if (Tracer.isActivated()) {
            if (this.node.generatePackage()) {
                getTracingPackageToMaster();
                if (DEBUG) {
                    LOGGER.debug("Tracing package obtained for " + this.getName());
                }
            }
        }

        if (DEBUG) {
            if (this.node.generateWorkersDebugInfo()) {
                getWorkersDebugInfo();
                LOGGER.debug("Workers Debug files obtained for " + this.getName());
                getBindingWorkersDebugInfo();
                LOGGER.debug("Binding Workers Debug files obtained for " + this.getName());
            }
        }

        if (CACHE_PROFILING_ENABLED) {
            getCacheProfilerDebugInfo();
            LOGGER.debug("Cache Profiler Debug files obtained for " + this.getName());
        }
    }

    @Override
    public void deleteIntermediate() {
        this.node.deleteTemporary();
    }

    @Override
    public void disableExecution() {
        if (this.isLost) {
            LOGGER.debug(" Skipping ExecutionManager shutdown because the node: " + this.getName() + " is lost.");
            return;
        }
        if (DEBUG) {
            LOGGER.debug("Shutting down Execution Manager on Resource " + this.getName());
        }

        Semaphore sem = new Semaphore(0);
        ExecutorShutdownListener executorShutdownListener = new ExecutorShutdownListener(sem);

        executorShutdownListener.addOperation();
        this.node.shutdownExecutionManager(executorShutdownListener);
        executorShutdownListener.enable();
        if (DEBUG) {
            LOGGER.debug("Waiting for shutting down the execution manager of " + this.getName());
        }
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            LOGGER.error("Error waiting for execution manager in resource " + getName() + " to stop");
        }
        if (DEBUG) {
            LOGGER.debug("Execution manager of " + this.getName() + " stopped");
        }
    }

    @Override
    public void stop(ShutdownListener sl) {
        if (this.isLost) {
            LOGGER.debug(" Skipping StopWorker because the node: " + this.getName() + " is lost.");
            sl.addOperation();
            sl.notifyEnd();
            return;
        }
        this.deleteIntermediate();
        sl.addOperation();
        this.node.stop(sl);
    }

    /**
     * Retrieves the tracing package to the master.
     */
    private void getTracingPackageToMaster() {
        Semaphore sem = new Semaphore(0);
        String fileName = getName() + "_compss_trace.tar.gz";
        SimpleURI fileOriginURI = this.node.getCompletePath(DataType.FILE_T, fileName);

        if (DEBUG) {
            LOGGER.debug("Copying tracing package from : " + fileOriginURI.getPath() + ",to : "
                + Comm.getAppHost().getAppLogDirPath() + "trace" + File.separator + fileName);
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
        String targetPath = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getAppLogDirPath() + "trace"
            + File.separator + fileName;
        try {
            SimpleURI uri = new SimpleURI(targetPath);
            tgt = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath);
            return;
        }

        // Ask for data
        COMPSsNode masterNode = Comm.getAppHost().getNode();
        masterNode.obtainData(new LogicalData("tracing" + this.getName()), source, tgt,
            new LogicalData("tracing" + this.getName()), new TracingCopyTransferable(), tracingListener);

        tracingListener.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            LOGGER.error("Error waiting for tracing files in resource " + getName() + " to get saved");
        }
        if (DEBUG) {
            LOGGER.debug("Removing " + this.getName() + " tracing temporary files");
        }

        File f = null;
        try {
            f = new File(source.getPath());
            if (!f.delete()) {
                LOGGER.error("Unable to remove tracing temporary files of node " + this.getName());
            }
        } catch (Exception e) {
            LOGGER.error("Unable to remove tracing temporary files of node " + this.getName(), e);
        }
    }

    /**
     * Retrieves the worker debug files.
     */
    private void getWorkersDebugInfo() {
        if (DEBUG) {
            LOGGER.debug("Copying Workers Information");
        }

        Semaphore sem = new Semaphore(0);
        WorkersDebugInformationListener wdil = new WorkersDebugInformationListener(sem);

        // Get Worker output
        wdil.addOperation();
        String outFileName = "worker_" + getName() + ".out";
        SimpleURI outFileOrigin =
            this.node.getCompletePath(DataType.FILE_T, "log" + File.separator + "static_" + outFileName);
        String outFileTarget =
            ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkersDirPath() + File.separator + outFileName;

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

        LOGGER.debug("- Source: " + outFileOrigin);
        LOGGER.debug("- Target: " + outFileTarget);
        COMPSsNode masterNode = Comm.getAppHost().getNode();
        masterNode.obtainData(new LogicalData("workerOut" + this.getName()), outSource, outTarget,
            new LogicalData("workerOut" + this.getName()), new WorkersDebugInfoCopyTransferable(), wdil);

        // Get Worker error
        wdil.addOperation();
        String errFileName = "worker_" + getName() + ".err";
        SimpleURI errFileOrigin =
            this.node.getCompletePath(DataType.FILE_T, "log" + File.separator + "static_" + errFileName);
        String errFileTarget =
            ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkersDirPath() + File.separator + errFileName;

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

        LOGGER.debug("- Source: " + errFileOrigin);
        LOGGER.debug("- Target: " + errFileTarget);
        masterNode.obtainData(new LogicalData("workerErr" + this.getName()), errSource, errTarget,
            new LogicalData("workerErr" + this.getName()), new WorkersDebugInfoCopyTransferable(), wdil);

        // Wait transfers
        wdil.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            LOGGER.error("Error waiting for worker debug files in resource " + getName() + " to get saved");
        }

        LOGGER.debug("Worker files from resource " + getName() + "received");
    }

    /**
     * Retrieves the binding worker debug files.
     */
    private void getBindingWorkersDebugInfo() {
        if (DEBUG) {
            LOGGER.debug("Copying Binding Workers Information");
        }

        Semaphore sem = new Semaphore(0);
        WorkersDebugInformationListener wdil = new WorkersDebugInformationListener(sem);

        // Get Worker output
        wdil.addOperation();
        String outFileNameOriginal = "log/binding_worker.out";
        String outFileName = "binding_worker_" + getName() + ".out";
        SimpleURI outFileOrigin = this.node.getCompletePath(DataType.FILE_T, outFileNameOriginal);
        String outFileTarget =
            ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkersDirPath() + File.separator + outFileName;

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

        LOGGER.debug("- Source: " + outFileOrigin);
        LOGGER.debug("- Target: " + outFileTarget);
        COMPSsNode masterNode = Comm.getAppHost().getNode();
        masterNode.obtainData(new LogicalData("bindingWorkerOut" + this.getName()), outSource, outTarget,
            new LogicalData("bindingWorkerOut" + this.getName()), new WorkersDebugInfoCopyTransferable(), wdil);

        // Get Worker error
        wdil.addOperation();
        String errFileNameOriginal = "log/binding_worker.err";
        String errFileName = "binding_worker_" + getName() + ".err";
        SimpleURI errFileOrigin = this.node.getCompletePath(DataType.FILE_T, errFileNameOriginal);
        String errFileTarget =
            ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkersDirPath() + File.separator + errFileName;

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

        LOGGER.debug("- Source: " + errFileOrigin);
        LOGGER.debug("- Target: " + errFileTarget);
        masterNode.obtainData(new LogicalData("bindingWorkerErr" + this.getName()), errSource, errTarget,
            new LogicalData("bindingWorkerErr" + this.getName()), new WorkersDebugInfoCopyTransferable(), wdil);

        // Wait transfers
        wdil.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            LOGGER.error("Error waiting for worker debug files in resource " + getName() + " to get saved");
        }

        LOGGER.debug("Worker files from resource " + getName() + "received");
    }

    /**
     * Retrieves cache profiler data.
     */
    private void getCacheProfilerDebugInfo() {
        if (DEBUG) {
            LOGGER.debug("Copying Cache Profiler Workers Information");
        }

        Semaphore sem = new Semaphore(0);
        WorkersDebugInformationListener wdil = new WorkersDebugInformationListener(sem);

        // Get Worker output
        wdil.addOperation();

        String cacheProfiler = "cache_profiler.json";
        String outCacheProfiler = "cache_profiler_" + getName() + ".json";
        SimpleURI cacheProfilerOutFileOrigin = this.node.getCompletePath(DataType.FILE_T, cacheProfiler);
        String cacheProfilerOutFileTarget = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkersDirPath()
            + File.separator + outCacheProfiler;

        DataLocation cacheProfilerOutSource = null;
        try {
            cacheProfilerOutSource = DataLocation.createLocation(this, cacheProfilerOutFileOrigin);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + cacheProfilerOutFileOrigin.toString(), e);
        }

        DataLocation cacheProfilerOutTarget = null;
        try {
            SimpleURI uriCacheProfiler = new SimpleURI(cacheProfilerOutFileTarget);
            cacheProfilerOutTarget = DataLocation.createLocation(Comm.getAppHost(), uriCacheProfiler);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + cacheProfilerOutFileTarget);
        }

        LOGGER.debug("- Cache Source: " + cacheProfilerOutSource);
        LOGGER.debug("- Cache Target: " + cacheProfilerOutTarget);
        COMPSsNode masterNode = Comm.getAppHost().getNode();
        masterNode.obtainData(new LogicalData("cache_profiler.json"), cacheProfilerOutSource, cacheProfilerOutTarget,
            new LogicalData("cache_profiler.json"), new WorkersDebugInfoCopyTransferable(), wdil);

        // Wait transfers
        wdil.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            LOGGER.error("Error waiting for cache profiler debug files in resource " + getName() + " to get saved");
        }

        LOGGER.debug("Cache profiler files from resource " + getName() + " received");
    }

    @Override
    public void idleReservedResourcesDetected(ResourceDescription resources) {
        // Should notify the resource user that such resources are available again
    }

    @Override
    public void reactivatedReservedResourcesDetected(ResourceDescription resources) {
        // Should notify the resource user that such resources are no longer available
    }

    public boolean isLost() {
        return isLost;
    }

    @Override
    public void lostNode() {
        this.isLost = true;
        ResourceManager.notifyRestart(this.name);
    }

    public void startingNode() {
        this.isLost = false;
    }

}
