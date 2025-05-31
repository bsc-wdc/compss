/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.log.LoggerManager;
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
import es.bsc.compss.types.data.listener.WorkersDebugInformationListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.LocationType;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.location.SharedDisk;
import es.bsc.compss.types.data.transferable.SafeCopyTransferable;
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
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    protected Map<String, String> sharedDisksSetup;

    private final List<SharedDisk> sharedDisks = new LinkedList<>();
    private final Map<SharedDisk, String> sharedDisk2Mountpoint = new HashMap<>();

    private final List<LogicalData> obsoletes = new LinkedList<>();
    private final Set<LogicalData> privateFiles = new HashSet<>();
    private boolean isLost = false;

    private static final boolean DP_ENABLED = Boolean.parseBoolean(System.getProperty(COMPSsConstants.DATA_PROVENANCE));


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
        this.sharedDisksSetup = sharedDisks;
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
        this.sharedDisksSetup = sharedDisks;
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

    // names of the folders that store the log and analysis files retrieved from each worker in the master
    private String getAnalysisFolder() {
        return LoggerManager.getWorkersLogDir() + File.separator + this.getName() + File.separator + "Analysis";
    }

    private String getLogFolder() {
        return LoggerManager.getWorkersLogDir() + File.separator + this.getName() + File.separator + "Log";
    }

    @Override
    public void addSharedDisk(String diskName, String mountpoint) {
        SharedDisk disk = SharedDisk.createDisk(diskName);
        disk.addMountpoint(this, mountpoint);
        this.sharedDisks.add(disk);
        this.sharedDisk2Mountpoint.put(disk, mountpoint);
    }

    @Override
    public SharedDisk getSharedDiskFromPath(String path) {
        if (path == null) {
            return null;
        }
        for (Entry<SharedDisk, String> e : this.sharedDisk2Mountpoint.entrySet()) {
            if (path.startsWith(e.getValue())) {
                return e.getKey();
            }
        }
        return null;
    }

    @Override
    public void start() throws InitNodeException {
        this.node.start();
        if (this.sharedDisksSetup != null) {
            for (Entry<String, String> disk : this.sharedDisksSetup.entrySet()) {
                this.addSharedDisk(disk.getKey(), disk.getValue());
            }
        }
    }

    @Override
    public Set<LogicalData> getAllDataFromHost() {
        Set<LogicalData> data = new HashSet<>();

        for (SharedDisk disk : sharedDisks) {
            Set<LogicalData> sharedData = disk.getAllSharedFiles();
            synchronized (sharedData) {
                data.addAll(sharedData);

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
        for (SharedDisk disk : sharedDisks) {
            disk.removeLogicalData(obsolete);
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

        Map<SharedDisk, String> disks = new HashMap<>();
        for (SharedDisk sd : this.sharedDisks) {
            String mountpoint = sd.removeMountpoint(this);
            disks.put(sd, mountpoint);
        }
        this.sharedDisks.clear();

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
                    safePath = ProtocolType.BINDING_URI.getSchema() + Comm.getAppHost().getWorkingDirectory()
                        + ld.getName() + "#" + bo.getType() + "#" + bo.getElements();
                } else {
                    safePath =
                        ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkingDirectory() + ld.getName();
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

    private Boolean isCompressedFile(Set<String> files) {
        if (files == null) {
            return false;
        }
        if (files.isEmpty()) {
            return false;
        }
        if (files.size() == 1) {
            String path = files.iterator().next();
            return path.endsWith(".tar.gz");
        }
        return false;
    }

    private void decompressAndDelete(String tarFile, String targetFolder) {
        if (DEBUG) {
            LOGGER.debug("Decompressing tar: " + tarFile + "; to: " + targetFolder);
        }
        StringBuilder cmd = new StringBuilder();
        cmd.append("tar -xf " + tarFile + " -C " + targetFolder + " && ");
        cmd.append("rm -rf " + tarFile);
        LOGGER.debug("Executing: " + cmd);
        try {
            new ProcessBuilder("/bin/bash", "-c", cmd.toString()).inheritIO().start().waitFor();
        } catch (InterruptedException | IOException e) {
            LOGGER.warn("Could not decompress: " + tarFile + "; to: " + targetFolder, e);
        }
    }

    private void copyTracingFilesToTracingFolder() {
        Path sourceDirectory = Paths.get(this.getAnalysisFolder());
        Path targetDirectory = Paths.get(Tracer.getExtraeOutputDir());

        try (DirectoryStream<Path> directoryStream =
            Files.newDirectoryStream(sourceDirectory, "*" + Tracer.PACKAGE_SUFFIX)) {

            for (Path sourceFile : directoryStream) {
                // Construct the target file path
                Path targetFile = targetDirectory.resolve(sourceFile.getFileName());
                Files.copy(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            LOGGER.warn("Could not copy tracing files from " + sourceDirectory + " to: " + targetDirectory);
        }

    }

    private void generateAndRetrieveWorkerAnalysis() {
        Set<String> analysisFiles = this.node.generateWorkerAnalysisFiles();
        LOGGER.debug("Analysis files generated by: " + this.getName() + " : "
            + (analysisFiles != null ? analysisFiles.toString() : "null"));

        if (analysisFiles == null || analysisFiles.isEmpty()) {
            LOGGER.debug("analysis files don't need to be retrieved");
            return;
        }
        if (DEBUG) {
            LOGGER.debug("Retrieving analysis files from worker: " + this.getName() + " : " + analysisFiles.toString());
        }
        retrieveWorkerFiles(analysisFiles, this.getAnalysisFolder());
        if (DEBUG) {
            LOGGER.debug("Tracing files obtained for " + this.getName());
        }

        // This code is for when the workers send on compressed file.
        // if (isCompressedFile(analysisFiles)) {
        // String remoteCompressedFile = analysisFiles.iterator().next();
        // String compressedFileName = Paths.get(remoteCompressedFile).getFileName().toString();
        // String localCompressedFile = this.getAnalysisFolder() + File.separator + compressedFileName;
        // decompressAndDelete(localCompressedFile, this.getAnalysisFolder());
        // }
        copyTracingFilesToTracingFolder();

    }

    private void generateAndRetrieveWorkerDebug() {
        Set<String> logFiles = this.node.generateWorkerDebugFiles();
        LOGGER.debug(
            "Debug files generated by: " + this.getName() + " : " + (logFiles != null ? logFiles.toString() : "null"));
        if (logFiles == null || logFiles.isEmpty()) {
            LOGGER.debug("log files don't need to be retrieved");
            return;
        }
        if (DEBUG) {
            LOGGER.debug("Retrieving debug files from worker: " + this.getName() + " : " + logFiles.toString());
            LOGGER.debug("    files: " + logFiles.toString());
        }
        retrieveWorkerFiles(logFiles, this.getLogFolder());
        if (DEBUG) {
            LOGGER.debug("Log files obtained for " + this.getName());
        }

        // This code is for when the workers send on compressed file.
        // if (isCompressedFile(logFiles)) {
        // String remoteCompressedFile = logFiles.iterator().next();
        // String compressedFileName = Paths.get(remoteCompressedFile).getFileName().toString();
        // String localCompressedFile = this.getLogFolder() + File.separator + compressedFileName;
        // decompressAndDelete(localCompressedFile, this.getLogFolder());
        // }
    }

    @Override
    public void retrieveTracingAndDebugData() {
        if (this.isLost) {
            LOGGER.debug("Will not retrieve Tracing and Debug Data because the node: " + this.getName() + " is lost.");
            return;
        }
        if (Tracer.isActivated() || CACHE_PROFILING_ENABLED) {
            generateAndRetrieveWorkerAnalysis();
        }

        if (DEBUG || DP_ENABLED) {
            generateAndRetrieveWorkerDebug();
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
     * Retrieves the set logFilesPaths of the worker's files to the .
     */
    private void retrieveWorkerFiles(Set<String> filesPaths, String folderPath) {
        // TODO: check if files are in an already accesible path
        if (DEBUG) {
            LOGGER.debug("Copying Workers Information");
        }

        COMPSsNode masterNode = Comm.getAppHost().getNode();

        // TODO: this block should probably be on the obtain data function
        Path pathPath = Paths.get(folderPath);
        try {
            Files.createDirectories(pathPath);
        } catch (Exception e) {
            LOGGER.warn("Error while creating folder to store worker files", e);
        }

        Semaphore[] completedObtainData = new Semaphore[filesPaths.size()];
        int semaphoreCounter = 0;

        for (String sourcePath : filesPaths) {
            final String fileName = sourcePath.substring(sourcePath.lastIndexOf("/") + 1);
            final String targetPath = folderPath + File.separator + fileName;

            Semaphore sem = new Semaphore(0);
            completedObtainData[semaphoreCounter++] = sem;
            WorkersDebugInformationListener wdil = new WorkersDebugInformationListener(sem);

            // Get Worker output
            wdil.addOperation();

            DataLocation sourceDataLocation = null;
            SimpleURI sourceUri = new SimpleURI(ProtocolType.FILE_URI.getSchema() + sourcePath);
            try {
                sourceDataLocation = DataLocation.createLocation(this, sourceUri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + sourceUri.toString(), e);
            }

            DataLocation targetDataLocation = null;
            SimpleURI targetUri = new SimpleURI(ProtocolType.FILE_URI.getSchema() + targetPath);
            try {
                targetDataLocation = DataLocation.createLocation(Comm.getAppHost(), targetUri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetUri.toString(), e);
            }

            LOGGER.debug("- Retrieving file from worker: " + sourceDataLocation.toString());
            masterNode.obtainData(new LogicalData(fileName), sourceDataLocation, targetDataLocation,
                new LogicalData(fileName), new WorkersDebugInfoCopyTransferable(), wdil);

            wdil.enable();

        }
        for (Semaphore sem : completedObtainData) {
            try {
                sem.acquire();
            } catch (InterruptedException ex) {
                LOGGER.error("Error waiting for worker debug files in resource " + getName() + " to get saved");
            }

            LOGGER.debug("Worker files from resource " + getName() + "received");
        }
    }

    /**
     * Returns the paths to the files from a folder files.
     * 
     * @param folderPath folder path to the files
     * @return Set the paths of the files in that folder
     */
    private Set<String> getFilesPathFromFolder(String folderPath) {

        Set<String> pathSet = Stream.of(new File(folderPath).listFiles()).filter(file -> !file.isDirectory())
            .map(File::getName).collect(Collectors.toSet());
        return pathSet;
    }

    private void copyTracingFiles() {
        String folderPath = this.getAnalysisFolder();
        Set<String> files = getFilesPathFromFolder(folderPath);

        LOGGER.debug("Copying files" + files.toString() + " from folder " + folderPath.toString() + " to folder "
            + Tracer.getExtraeOutputDir());
        for (String fileName : files) {
            if (fileName.endsWith(Tracer.PACKAGE_SUFFIX)) {
                Path src = Paths.get(folderPath + File.separator + fileName);
                Path tgt = Paths.get(Tracer.getExtraeOutputDir() + File.separator + this.getName() + fileName);
                try {
                    Files.copy(src, tgt);
                } catch (IOException e) {
                    LOGGER.error("Failed to copy tracing files inside master folders", e);
                }
            }
        }
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
