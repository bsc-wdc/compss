/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.exceptions.AnnounceException;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.types.data.location.DataLocation.Type;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.BindingDataManager;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Serializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;


/**
 * Representation of the COMPSs Master Node Only 1 instance per execution
 *
 */
public final class COMPSsMaster extends COMPSsWorker {

    protected static final String ERROR_UNKNOWN_HOST = "ERROR: Cannot determine the IP address of the local host";

    private final String name;

    /**
     * New COMPSs Master
     *
     * @param hostName
     */
    public COMPSsMaster(String hostName) {
        super(hostName, null);
        name = hostName;
    }

    @Override
    public void start() {
        // Do nothing.
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setInternalURI(MultiURI u) {
        for (CommAdaptor adaptor : Comm.getAdaptors().values()) {
            adaptor.completeMasterURI(u);
        }
    }

    @Override
    public void stop(ShutdownListener sl) {
        // NO need to do anything
    }

    @Override
    public void sendData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        for (Resource targetRes : target.getHosts()) {
            COMPSsNode node = targetRes.getNode();
            if (node != this) {
                try {
                    node.obtainData(ld, source, target, tgtData, reason, listener);
                } catch (Exception e) {
                    // Can not copy the file.
                    // Cannot receive the file, try with the following
                    continue;
                }
                return;
            }

        }
    }

    public void obtainBindingData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {
        BindingObject tgtBO = ((BindingObjectLocation) target).getBindingObject();
        ld.lockHostRemoval();
        Collection<Copy> copiesInProgress = ld.getCopiesInProgress();
        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            for (Copy copy : copiesInProgress) {
                if (copy != null) {
                    if (copy.getTargetLoc() != null && copy.getTargetLoc().getHosts().contains(Comm.getAppHost())) {
                        if (DEBUG) {
                            LOGGER.debug("Copy in progress tranfering " + ld.getName() + "to master. Waiting for finishing");
                        }
                        Copy.waitForCopyTofinish(copy, this);
                        // try {
                        if (DEBUG) {
                            LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget() + " to " + tgtBO.getName());
                        }
                        BindingObject bo = BindingObject.generate(copy.getFinalTarget());
                        if (ld.getName().equals(tgtBO.getName())) {
                            LOGGER.debug("Current transfer is the same as expected. Nothing to do setting data target to "
                                    + copy.getFinalTarget());
                            reason.setDataTarget(copy.getFinalTarget());
                        } else {
                            LOGGER.debug("Making cache copy from " + bo.getName() + " to " + tgtBO.getName());
                            BindingDataManager.copyCachedData(bo.getName(), tgtBO.getName());
                            if (tgtData != null) {
                                tgtData.addLocation(target);
                            }
                            LOGGER.debug("File copied set dataTarget " + copy.getFinalTarget());
                            reason.setDataTarget(copy.getFinalTarget());
                        }
                        listener.notifyEnd(null);
                        ld.releaseHostRemoval();
                        return;

                    } else if (copy.getTargetData() != null && copy.getTargetData().getAllHosts().contains(Comm.getAppHost())) {
                        Copy.waitForCopyTofinish(copy, this);
                        // try {
                        if (DEBUG) {
                            LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget() + " to " + tgtBO.getName());
                        }
                        BindingObject bo = BindingObject.generate(copy.getFinalTarget());
                        if (ld.getName().equals(tgtBO.getName())) {

                            LOGGER.debug("Current transfer is the same as expected. Nothing to do setting data target to " + bo.getName());
                            reason.setDataTarget(copy.getFinalTarget());
                        } else {
                            LOGGER.debug("Making cache copy from " + bo.getName() + " to " + tgtBO.getName());
                            BindingDataManager.copyCachedData(bo.getName(), tgtBO.getName());
                            if (tgtData != null) {
                                tgtData.addLocation(target);
                            }
                            LOGGER.debug("File copied set dataTarget " + copy.getFinalTarget());
                            reason.setDataTarget(copy.getFinalTarget());
                        }

                        listener.notifyEnd(null);
                        ld.releaseHostRemoval();
                        return;

                    } else if (DEBUG) {
                        LOGGER.debug("Current copies are not transfering " + ld.getName() + " to master. Ignoring at this moment");
                    }
                }
            }
        }

        // Checking if file is already in master
        if (DEBUG) {
            LOGGER.debug("Checking if " + ld.getName() + " is at master (" + Comm.getAppHost().getName() + ").");
        }

        for (MultiURI u : ld.getURIs()) {
            if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug(ld.getName() + " is at " + u.toString() + "(" + hostname + ")");
            }
            if (u.getHost() == Comm.getAppHost()) {
                if (DEBUG) {
                    LOGGER.debug("Master local copy " + ld.getName() + " from " + u.getHost().getName() + " to " + tgtBO.getName());
                }
                BindingObject bo = BindingObject.generate(u.getPath());
                if (ld.getName().equals(tgtBO.getName())) {
                    LOGGER.debug("Current transfer is the same as expected. Nothing to do setting data target to " + u.getPath());
                    reason.setDataTarget(u.getPath());
                } else {
                    LOGGER.debug("Making cache copy from " + u.getPath() + " to " + tgtBO.getName());
                    BindingDataManager.copyCachedData(bo.getName(), tgtBO.getName());
                    if (tgtData != null) {
                        tgtData.addLocation(target);
                    }
                    LOGGER.debug("File copied set dataTarget " + u.getPath());
                    reason.setDataTarget(u.getPath());
                }

                listener.notifyEnd(null);
                ld.releaseHostRemoval();
                return;
            } else if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug("Data " + ld.getName() + " copy in " + hostname + " not evaluated now");
            }

        }

        // Ask the transfer from an specific source
        if (source != null) {
            for (Resource sourceRes : source.getHosts()) {
                COMPSsNode node = sourceRes.getNode();
                String sourcePath = source.getURIInHost(sourceRes).getPath();
                if (node != this) {
                    try {
                        if (DEBUG) {
                            LOGGER.debug("Sending data " + ld.getName() + " from " + sourcePath + " to " + tgtBO.getName());
                        }
                        node.sendData(ld, source, target, tgtData, reason, listener);
                    } catch (Exception e) {
                        ErrorManager.warn("Not possible to sending data master to " + tgtBO.getName(), e);
                        continue;
                    }
                    LOGGER.debug("Data " + ld.getName() + " sent.");
                    ld.releaseHostRemoval();
                    return;
                } else {
                    BindingObject bo = BindingObject.generate(sourcePath);
                    if (ld.getName().equals(tgtBO.getName())) {
                        LOGGER.debug("Current transfer is the same as expected. Nothing to do setting data target to " + bo.getName());
                        reason.setDataTarget(sourcePath);
                    } else {
                        LOGGER.debug("Making cache copy from " + bo.getName() + " to " + tgtBO.getName());
                        BindingDataManager.copyCachedData(bo.getName(), tgtBO.getName());
                        if (tgtData != null) {
                            tgtData.addLocation(target);
                        }
                        LOGGER.debug("File copied set dataTarget " + tgtBO.getName());
                        reason.setDataTarget(sourcePath);
                    }
                    listener.notifyEnd(null);
                    ld.releaseHostRemoval();
                    return;
                }
            }
        } else {
            LOGGER.debug("Source data location is null. Trying other alternatives");
        }

        // Preferred source is null or copy has failed. Trying to retrieve data from any host
        for (Resource sourceRes : ld.getAllHosts()) {
            COMPSsNode node = sourceRes.getNode();
            if (node != this) {
                try {
                    LOGGER.debug("Sending data " + ld.getName() + " from " + sourceRes.getName() + " to " + tgtBO.getName());
                    node.sendData(ld, source, target, tgtData, reason, listener);
                } catch (Exception e) {
                    LOGGER.error("Error: exception sending data", e);
                    continue;
                }
                LOGGER.debug("Data " + ld.getName() + " sent.");
                ld.releaseHostRemoval();
                return;
            } else if (DEBUG) {
                LOGGER.debug("Data " + ld.getName() + " copy in " + sourceRes.getName()
                        + " not evaluated now. Should have been evaluated before");
            }
        }
    }

    public void obtainFileData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        String targetPath = target.getURIInHost(Comm.getAppHost()).getPath();
        // Check if there are current copies in progress
        if (DEBUG) {
            LOGGER.debug("Data " + ld.getName() + " not in memory. Checking if there is a copy to the master in progress");
        }
        ld.lockHostRemoval();
        Collection<Copy> copiesInProgress = ld.getCopiesInProgress();
        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            for (Copy copy : copiesInProgress) {
                if (copy != null) {
                    if (copy.getTargetLoc() != null && copy.getTargetLoc().getHosts().contains(Comm.getAppHost())) {
                        if (DEBUG) {
                            LOGGER.debug("Copy in progress tranfering " + ld.getName() + "to master. Waiting for finishing");
                        }
                        Copy.waitForCopyTofinish(copy, this);
                        try {
                            if (DEBUG) {
                                LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget() + " to " + targetPath);
                            }
                            Files.copy((new File(copy.getFinalTarget())).toPath(), new File(targetPath).toPath(),
                                    StandardCopyOption.REPLACE_EXISTING);
                            if (tgtData != null) {
                                tgtData.addLocation(target);
                            }
                            LOGGER.debug("File copied set dataTarget " + targetPath);
                            reason.setDataTarget(targetPath);

                            listener.notifyEnd(null);
                            ld.releaseHostRemoval();
                            return;
                        } catch (IOException ex) {
                            ErrorManager.warn("Error master local copying file " + copy.getFinalTarget() + " from master to " + targetPath
                                    + " with replacing", ex);
                        }

                    } else if (copy.getTargetData() != null && copy.getTargetData().getAllHosts().contains(Comm.getAppHost())) {
                        Copy.waitForCopyTofinish(copy, this);
                        try {
                            if (DEBUG) {
                                LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget() + " to " + targetPath);
                            }
                            Files.copy((new File(copy.getFinalTarget())).toPath(), new File(targetPath).toPath(),
                                    StandardCopyOption.REPLACE_EXISTING);
                            if (tgtData != null) {
                                tgtData.addLocation(target);
                            }
                            LOGGER.debug("File copied. Set data target to " + targetPath);
                            reason.setDataTarget(targetPath);

                            listener.notifyEnd(null);
                            ld.releaseHostRemoval();
                            return;
                        } catch (IOException ex) {
                            ErrorManager.warn(
                                    "Error master local copy from " + copy.getFinalTarget() + " to " + targetPath + " with replacing", ex);
                        }
                    } else if (DEBUG) {
                        LOGGER.debug("Current copies are not transfering " + ld.getName() + " to master. Ignoring at this moment");
                    }
                }
            }
        }

        // Checking if file is already in master
        if (DEBUG) {
            LOGGER.debug("Checking if " + ld.getName() + " is at master (" + Comm.getAppHost().getName() + ").");
        }

        for (MultiURI u : ld.getURIs()) {
            if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug(ld.getName() + " is at " + u.toString() + "(" + hostname + ")");
            }
            if (u.getHost() == Comm.getAppHost()) {
                try {
                    if (DEBUG) {
                        LOGGER.debug("Master local copy " + ld.getName() + " from " + u.getHost().getName() + " to " + targetPath);
                    }
                    Files.copy((new File(u.getPath())).toPath(), new File(targetPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                    if (tgtData != null) {
                        tgtData.addLocation(target);
                    }
                    LOGGER.debug("File copied. Set data target to " + targetPath);
                    reason.setDataTarget(targetPath);

                    listener.notifyEnd(null);
                    ld.releaseHostRemoval();
                    return;
                } catch (IOException ex) {
                    ErrorManager.warn("Error master local copy file from " + u.getPath() + " to " + targetPath + " with replacing", ex);
                }
            } else if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug("Data " + ld.getName() + " copy in " + hostname + " not evaluated now");
            }

        }

        // Ask the transfer from an specific source
        if (source != null) {
            for (Resource sourceRes : source.getHosts()) {
                COMPSsNode node = sourceRes.getNode();
                String sourcePath = source.getURIInHost(sourceRes).getPath();
                if (node != this) {
                    try {
                        if (DEBUG) {
                            LOGGER.debug("Sending data " + ld.getName() + " from " + sourcePath + " to " + targetPath);
                        }
                        node.sendData(ld, source, target, tgtData, reason, listener);
                    } catch (Exception e) {
                        ErrorManager.warn("Not possible to sending data master to " + targetPath, e);
                        continue;
                    }
                    LOGGER.debug("Data " + ld.getName() + " sent.");
                    ld.releaseHostRemoval();
                    return;
                } else {
                    try {
                        if (DEBUG) {
                            LOGGER.debug("Local copy " + ld.getName() + " from " + sourcePath + " to " + targetPath);
                        }
                        Files.copy(new File(sourcePath).toPath(), new File(targetPath).toPath(), StandardCopyOption.REPLACE_EXISTING);

                        LOGGER.debug("File copied. Set data target to " + targetPath);
                        reason.setDataTarget(targetPath);
                        listener.notifyEnd(null);
                        ld.releaseHostRemoval();
                        return;
                    } catch (IOException ex) {
                        ErrorManager.warn("Error master local copy file from " + sourcePath + " to " + targetPath, ex);
                    }
                }
            }
        } else {
            LOGGER.debug("Source data location is null. Trying other alternatives");
        }

        // Preferred source is null or copy has failed. Trying to retrieve data from any host
        for (Resource sourceRes : ld.getAllHosts()) {
            COMPSsNode node = sourceRes.getNode();
            if (node != this) {
                try {
                    LOGGER.debug("Sending data " + ld.getName() + " from " + sourceRes.getName() + " to " + targetPath);
                    node.sendData(ld, source, target, tgtData, reason, listener);
                } catch (Exception e) {
                    LOGGER.error("Error: exception sending data", e);
                    continue;
                }
                LOGGER.debug("Data " + ld.getName() + " sent.");
                ld.releaseHostRemoval();
                return;
            } else if (DEBUG) {
                LOGGER.debug("Data " + ld.getName() + " copy in " + sourceRes.getName()
                        + " not evaluated now. Should have been evaluated before");
            }
        }

        // If we have not exited before, any copy method was successful. Raise warning
        ErrorManager.warn("Error file " + ld.getName() + " not transferred to " + targetPath);
        ld.releaseHostRemoval();
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        LOGGER.info("Obtain Data " + ld.getName());
        if (DEBUG) {
            if (ld != null) {
                LOGGER.debug("srcData: " + ld.toString());
            }
            if (reason != null) {
                LOGGER.debug("Reason: " + reason.getType());
            }
            if (source != null) {
                LOGGER.debug("Source Data location" + source.getType().toString() + " " + source.getProtocol().toString() + " "
                        + source.getURIs().get(0));
            }
            if (target != null) {
                LOGGER.debug("Target Data location" + target.getType().toString() + " " + target.getProtocol().toString() + " "
                        + target.getURIs().get(0));
            }
            if (tgtData != null) {
                LOGGER.debug("tgtData: " + tgtData.toString());
            }
        }
        /*
         * Check if data is binding data
         */
        if (ld.isBindingData() || (reason != null && reason.getType().equals(DataType.BINDING_OBJECT_T))
                || (source != null && source.getType().equals(Type.BINDING)) || (target != null && target.getType().equals(Type.BINDING))) {
            obtainBindingData(ld, source, target, tgtData, reason, listener);
            return;
        }
        /*
         * PSCO transfers are always available, if any SourceLocation is PSCO, don't transfer
         */

        for (DataLocation loc : ld.getLocations()) {
            if (loc.getProtocol().equals(Protocol.PERSISTENT_URI)) {
                LOGGER.debug("Object in Persistent Storage. Set dataTarget to " + loc.getPath());
                reason.setDataTarget(loc.getPath());
                listener.notifyEnd(null);
                return;
            }
        }

        /*
         * Otherwise the data is a file or an object that can be already in the master memory, in the master disk or
         * being transfered
         */
        // Check if data is in memory (no need to check if it is PSCO since previous case avoids it)
        if (ld.isInMemory()) {
            String targetPath = target.getURIInHost(Comm.getAppHost()).getPath();
            // Serialize value to file
            try {
                Serializer.serialize(ld.getValue(), targetPath);
            } catch (IOException ex) {
                ErrorManager.warn("Error copying file from memory to " + targetPath, ex);
            }

            if (tgtData != null) {
                tgtData.addLocation(target);
            }
            LOGGER.debug("Object in memory. Set dataTarget to " + targetPath);
            reason.setDataTarget(targetPath);
            listener.notifyEnd(null);
            return;
        }

        obtainFileData(ld, source, target, tgtData, reason, listener);

    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res, List<String> slaveWorkersNodeNames,
            JobListener listener) {

        // Cannot run jobs
        return null;
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        String path = null;
        switch (type) {
            case FILE_T:
                path = Protocol.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + name;
                break;
            case OBJECT_T:
                path = Protocol.OBJECT_URI.getSchema() + name;
                break;
            case PSCO_T:
                path = Protocol.PERSISTENT_URI.getSchema() + name;
                break;
            case EXTERNAL_PSCO_T:
                path = Protocol.PERSISTENT_URI.getSchema() + name;
                break;
            case BINDING_OBJECT_T:
                path = Protocol.BINDING_URI.getSchema() + name;
                break;
            default:
                return null;
        }

        // Switch path to URI
        return new SimpleURI(path);
    }

    @Override
    public void deleteTemporary() {
        File dir = new File(Comm.getAppHost().getTempDirPath());
        for (File f : dir.listFiles()) {
            deleteFolder(f);
        }
    }

    private void deleteFolder(File folder) {
        if (folder.isDirectory()) {
            for (File f : folder.listFiles()) {
                deleteFolder(f);
            }
        }
        if (!folder.delete()) {
            LOGGER.error("Error deleting file " + (folder == null ? "" : folder.getName()));
        }
    }

    @Override
    public boolean generatePackage() {
        // Should not be executed on a COMPSsMaster
        return false;
    }

    @Override
    public boolean generateWorkersDebugInfo() {
        // Should not be executed on a COMPSsMaster
        return false;
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
        // Should not be executed on a COMPSsMaster
    }

    @Override
    public String getUser() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getClasspath() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getPythonpath() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void announceDestruction() throws AnnounceException {
        //No need to do it. The master no it's always up
    }

    @Override
    public void announceCreation() throws AnnounceException {
        //No need to do it. The master no it's always up
    }

}
