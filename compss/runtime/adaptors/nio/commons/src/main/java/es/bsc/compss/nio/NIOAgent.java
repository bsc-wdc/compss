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
package es.bsc.compss.nio;

import es.bsc.comm.Connection;
import es.bsc.comm.TransferManager;
import es.bsc.comm.nio.NIOConnection;
import es.bsc.comm.nio.NIOEventManager;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.stage.Transfer.Destination;
import es.bsc.compss.data.BindingDataManager;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.commands.Command;
import es.bsc.compss.nio.commands.CommandCancelTask;
import es.bsc.compss.nio.commands.CommandDataDemand;
import es.bsc.compss.nio.commands.CommandDataReceived;
import es.bsc.compss.nio.commands.CommandExecutorShutdown;
import es.bsc.compss.nio.commands.CommandExecutorShutdownACK;
import es.bsc.compss.nio.commands.CommandNIOTaskDone;
import es.bsc.compss.nio.commands.CommandNewTask;
import es.bsc.compss.nio.commands.CommandRemoveObsoletes;
import es.bsc.compss.nio.commands.CommandShutdown;
import es.bsc.compss.nio.commands.CommandShutdownACK;
import es.bsc.compss.nio.commands.CommandTracingID;
import es.bsc.compss.nio.commands.tracing.CommandGenerateAnalysisFiles;
import es.bsc.compss.nio.commands.tracing.CommandGenerateAnalysisFilesDone;
import es.bsc.compss.nio.commands.workerfiles.CommandGenerateDebugFiles;
import es.bsc.compss.nio.commands.workerfiles.CommandGenerateDebugFilesDone;
import es.bsc.compss.nio.exceptions.SerializedObjectException;
import es.bsc.compss.nio.requests.DataRequest;
import es.bsc.compss.nio.utils.NIOBindingDataManager;
import es.bsc.compss.nio.utils.NIOBindingObjectStream;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.serializers.Serializer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class NIOAgent {

    // Logging
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final String DBG_PREFIX = "[NIO Agent] ";

    // Class information
    protected static final String NIO_EVENT_MANAGER_CLASS = NIOEventManager.class.getCanonicalName();
    public static final String ID = NIOAgent.class.getCanonicalName();

    // Transfer Manager instance
    protected static final TransferManager TM = new TransferManager();

    public static final int NUM_PARAMS_PER_WORKER_SH = 7;
    public static final int NUM_PARAMS_NIO_WORKER = 41;
    public static final String BINDER_DISABLED = "disabled";
    public static final String BINDER_AUTOMATIC = "automatic";

    private static final String COMPRESSED_DIR_EXTENSION = ".zip";

    private int sendTransfers;
    private final int maxSendTransfers;
    private final Connection[] trasmittingConnections;
    private int receiveTransfers;
    private final int maxReceiveTransfers;

    private boolean finish;
    private Connection closingConnection = null;

    // Requests related to a DataId
    protected final Map<String, List<DataRequest>> dataToRequests;
    // NIOData requests that will be transferred
    private final LinkedList<DataRequest> pendingRequests;
    // Ongoing transfers
    private final Map<Connection, String> ongoingTransfers;
    // Ongoing Commands
    private static final Map<Connection, Command> ONGOING_COMMANDS = new ConcurrentHashMap<>();

    // Master information
    protected int masterPort;
    protected NIONode masterNode;

    // Tracing
    protected boolean tracing;
    protected boolean tracingTaskDependencies;
    protected int tracingId = 0; // unless NIOWorker sets this value; 0 -> master (NIOAdaptor)
    protected HashMap<Connection, Integer> connection2partner;


    /**
     * Creates a NIOAgent instance.
     *
     * @param snd Maximum simultaneous sends.
     * @param rcv Maximum simultaneous receives.
     * @param port Communication port.
     */
    public NIOAgent(int snd, int rcv, int port) {
        this.sendTransfers = 0;
        this.maxSendTransfers = snd;
        this.trasmittingConnections = new Connection[maxSendTransfers];
        this.receiveTransfers = 0;
        this.maxReceiveTransfers = rcv;
        this.masterPort = port;
        this.ongoingTransfers = new HashMap<>();
        this.pendingRequests = new LinkedList<>();
        this.dataToRequests = new HashMap<>();
        this.connection2partner = new HashMap<>();
        this.finish = false;

        LOGGER.debug(DBG_PREFIX + "Debug: " + DEBUG);
    }

    /**
     * Returns the master node.
     *
     * @return The master node.
     */
    public NIONode getMaster() {
        return this.masterNode;
    }

    /**
     * Returns the associated Transfer Manager.
     *
     * @return The associated Transfer Manager.
     */
    public static TransferManager getTransferManager() {
        return TM;
    }

    /**
     * Adds a new association between the given connection {@code c} and the given partner {@code partner}.
     *
     * @param c Connection.
     * @param partner Partner.
     * @param tag Association tag.
     */
    public void addConnectionAndPartner(Connection c, int partner, int tag) {
        this.connection2partner.put(c, partner);
    }

    /**
     * Returns DataRequests of a given dataId.
     *
     * @param dataId Data Id.
     * @return The DataRequests associated to the given data Id.
     */
    protected List<DataRequest> getDataRequests(String dataId) {
        return this.dataToRequests.get(dataId);
    }

    /**
     * Returns whether there are pending transfers or not.
     *
     * @return {@code true} if there are pending transfers, {@code false} otherwise.
     */
    public boolean hasPendingTransfers() {
        LOGGER.debug("pending: " + !this.pendingRequests.isEmpty() + " sendTransfers: " + (this.sendTransfers != 0)
            + " receiveTrasnfers: " + (this.receiveTransfers != 0) + "\n");
        return !this.pendingRequests.isEmpty() || this.sendTransfers != 0 || this.receiveTransfers != 0;
    }

    /**
     * Requests transfers if there are available receive slots.
     */
    public void requestTransfers() {
        DataRequest dr = null;
        synchronized (pendingRequests) {
            if (!this.pendingRequests.isEmpty() && tryAcquireReceiveSlot()) {
                dr = this.pendingRequests.remove();
            }
        }
        while (dr != null) {
            NIOData source = dr.getSource();
            NIOUri uri = source.getFirstURI();

            if (NIOTracer.isActivated()) {
                NIOTracer.emitDataTransferEvent(source.getDataMgmtId(), false);
            }
            NIONode nn = uri.getHost();
            if (nn.getIp() == null) {
                nn = this.masterNode;
            }
            Connection c = null;

            try {
                c = TM.startConnection(nn);
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will be used to acquire data "
                        + dr.getTarget() + " stored in " + nn + " with name " + dr.getSource().getDataMgmtId());
                }
                NIOData remoteData = new NIOData(source.getDataMgmtId(), uri);
                CommandDataDemand cdd = new CommandDataDemand(remoteData, this.tracingId);
                registerOngoingCommand(c, cdd);
                this.ongoingTransfers.put(c, dr.getSource().getDataMgmtId());
                c.sendCommand(cdd);

                if (NIOTracer.isActivated()) {
                    c.receive();
                }
                switch (dr.getType()) {
                    case DIRECTORY_T:
                        // directories are compressed right before being transferred
                        c.receiveDataFile(dr.getTarget().concat(COMPRESSED_DIR_EXTENSION));
                        c.finishConnection();
                        break;
                    case FILE_T:
                    case EXTERNAL_STREAM_T:
                        c.receiveDataFile(dr.getTarget());
                        c.finishConnection();
                        break;
                    case BINDING_OBJECT_T:
                        if (isPersistentCEnabled()) {
                            receiveBindingObject(c, dr);
                        } else {
                            receiveBindingObjectAsFile(c, dr);
                        }
                        break;
                    case STRING_T:
                    case STRING_64_T:
                    case OBJECT_T:
                    case STREAM_T:
                    case COLLECTION_T:
                    case DICT_COLLECTION_T:
                    case EXTERNAL_PSCO_T:
                    case PSCO_T:
                    default:
                        c.receiveDataObject();
                        c.finishConnection();
                        break;
                }

            } catch (Exception e) {
                e.printStackTrace(System.err);
                if (c != null) {
                    c.finishConnection();
                }
            }
            synchronized (pendingRequests) {
                if (!this.pendingRequests.isEmpty() && tryAcquireReceiveSlot()) {
                    dr = this.pendingRequests.remove();
                } else {
                    dr = null;
                }
            }

            if (NIOTracer.isActivated()) {
                NIOTracer.emitDataTransferEvent(source.getDataMgmtId(), true);
            }
        }
    }

    private void receiveBindingObject(Connection c, DataRequest dr) {
        if (DEBUG) {
            LOGGER.debug(DBG_PREFIX + "Receiving binding data " + dr.getTarget() + " from "
                + dr.getSource().getFirstURI().getPath());
        }
        // BindingObject bo = BindingObject.generate(dr.getSource().getFirstURI().getPath());
        String targetId = dr.getTarget();
        BindingObject bo = BindingObject.generate(targetId);
        if (bo.getElements() > 0) {
            c.receiveDataByteBuffer();
            c.finishConnection();
        } else {
            NIOBindingDataManager.receiveBindingObject(this, (NIOConnection) c, bo.getName(), bo.getType());
        }

    }

    private void receiveBindingObjectAsFile(Connection c, DataRequest dr) {
        if (DEBUG) {
            LOGGER.debug(DBG_PREFIX + "Receiving binding data " + dr.getTarget() + " as file from "
                + dr.getSource().getFirstURI().getPath());
        }
        // BindingObject bo = BindingObject.generate(dr.getSource().getFirstURI().getPath());
        String targetId = dr.getTarget();
        BindingObject bo = BindingObject.generate(targetId);
        c.receiveDataFile(bo.getId());
        c.finishConnection();
    }

    /**
     * Adds a new NIOData Transfer Request.
     *
     * @param dr Data Request to add.
     */
    public void addTransferRequest(DataRequest dr) {
        List<DataRequest> list = this.dataToRequests.get(dr.getSource().getDataMgmtId());
        if (list == null) {
            list = new LinkedList<>();
            this.dataToRequests.put(dr.getSource().getDataMgmtId(), list);
            synchronized (this.pendingRequests) {
                this.pendingRequests.add(dr);
            }
        }
        list.add(dr);
    }

    /**
     * Reply the data.
     *
     * @param c Connection.
     * @param d Data.
     * @param receiverID Receiver Id.
     */
    public void sendData(Connection c, NIOData d, int receiverID) {
        if (NIOTracer.isActivated()) {
            int tag = Math.abs(d.getDataMgmtId().hashCode());
            CommandTracingID cmd = new CommandTracingID(this.tracingId, tag);
            c.sendCommand(cmd);
            NIOTracer.emitDataTransferEvent(d.getDataMgmtId(), false);
            NIOTracer.emitCommEvent(true, receiverID, tag);
        }

        String path = d.getFirstURI().getPath();
        ProtocolType scheme = d.getFirstURI().getProtocol();
        switch (scheme) {
            case DIR_URI:
                compressAndSendDir(c, path, d);
                break;
            case FILE_URI:
            case SHARED_URI:
            case EXTERNAL_STREAM_URI:
                sendFile(c, path, d);
                break;
            case BINDING_URI:
                if (isPersistentCEnabled()) {
                    sendBindingObject(c, path, d);
                } else {
                    sendBindingObjectAsFile(c, path, d);
                }
                break;
            case OBJECT_URI:
            case STREAM_URI:
            case PERSISTENT_URI:
                sendObject(c, path, d);
                break;
            case ANY_URI:
                if (path.startsWith(File.separator)) {
                    sendFile(c, path, d);
                } else {
                    sendObject(c, path, d);
                }
                break;
        }

        if (NIOTracer.isActivated()) {
            NIOTracer.emitDataTransferEvent(d.getDataMgmtId(), true);
        }
        c.finishConnection();
    }

    private void sendObject(Connection c, String path, NIOData d) {
        try {
            Object o = getObject(path);
            if (DEBUG) {
                LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will transfer an object as data "
                    + d.getDataMgmtId());
            }
            c.sendDataObject(o);
        } catch (SerializedObjectException soe) {
            // Exception has been raised because object has been serialized
            String newLocation = getObjectAsFile(path);
            if (DEBUG) {
                LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will transfer an object-file " + newLocation
                    + " as data " + d.getDataMgmtId());
            }
            sendFile(c, newLocation, d);
        }

    }

    private void sendFile(Connection c, String path, NIOData d) {
        // TODO: Not sure if it is needed with the addition of the protocol in the NIOURI. To check
        if (path.startsWith(File.separator)) {
            File f = new File(path);
            if (f.exists()) {
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will transfer file " + path + " as data "
                        + d.getDataMgmtId());
                }
                c.sendDataFile(path);
            } else {
                // Not found check if it has been moved to the target name (renames
                if (!f.getName().equals(d.getDataMgmtId())) {
                    File renamed = new File(getPossiblyRenamedFileName(f, d));
                    if (renamed.exists()) {
                        if (DEBUG) {
                            LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will transfer file "
                                + renamed.getAbsolutePath() + " as data " + d.getDataMgmtId());
                        }
                        c.sendDataFile(renamed.getAbsolutePath());
                    } else {
                        ErrorManager
                            .warn("Can't send neither file '" + path + "' nor file '" + renamed.getAbsolutePath()
                                + "' via connection " + c.hashCode() + " because files don't exist.");
                        releaseSendSlot(c);
                        handleDataToSendNotAvailable(c, d);
                    }
                } else {
                    ErrorManager.warn("Can't send file '" + path + "' via connection " + c.hashCode()
                        + " because file doesn't exist.");
                    releaseSendSlot(c);
                    handleDataToSendNotAvailable(c, d);
                }
            }
        } else {
            if (DEBUG) {
                LOGGER.debug(
                    DBG_PREFIX + "Connection " + c.hashCode() + " will transfer object of data " + d.getDataMgmtId());
            }
            sendObject(c, path, d);
        }
    }

    private void compressAndSendDir(Connection c, String path, NIOData d) {
        File f = new File(path);
        if (f.exists()) {
            if (path.endsWith(File.separator)) {
                // This avoids /path/to/directory/.zip with infinite size
                // File must be /path/to/directory.zip to avoid self compression
                path = path.substring(0, path.length() - 1);
            }
            if (DEBUG) {
                LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will compress and transfer directory " + path
                    + " as data " + d.getDataMgmtId());
            }
            String zipFile = path.concat(COMPRESSED_DIR_EXTENSION);
            boolean zipCreated = createZip(path, zipFile);
            if (!zipCreated) {
                ErrorManager.warn("Can't send directory '" + path + "'" + "' via connection " + c.hashCode()
                    + " because '" + COMPRESSED_DIR_EXTENSION + "' file couldn't be created.");
                releaseSendSlot(c);
                handleDataToSendNotAvailable(c, d);
            }
            c.sendDataFile(zipFile, true);
        } else {
            // todo: make sure this is not the case!
            ErrorManager.warn(
                "Can't send directory '" + path + "' via connection " + c.hashCode() + " because it doesn't exist.");
            releaseSendSlot(c);
            handleDataToSendNotAvailable(c, d);
        }

    }

    private boolean createZip(String sourceDirPath, String zipFilePath) {

        Path p;
        try {
            p = Files.createFile(Paths.get(zipFilePath));
        } catch (FileAlreadyExistsException fae) {
            File oldZipFile = new File(zipFilePath);
            try {
                Files.delete(oldZipFile.toPath());
            } catch (IOException e) {
                LOGGER.error("Error removing old zip file " + zipFilePath, e);
                return false;
            }
            try {
                p = Files.createFile(Paths.get(zipFilePath));
            } catch (IOException e) {
                LOGGER.error("Error creating zip file " + zipFilePath, e);
                return false;
            }
        } catch (IOException e) {
            LOGGER.error("Error creating zip file " + zipFilePath, e);
            return false;
        }

        // walk through the directory and add everything to the zip file
        try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(p))) {
            Path pp = Paths.get(sourceDirPath);
            Files.walk(pp).filter(path -> !Files.isDirectory(path)).forEach(path -> {
                ZipEntry zipEntry = new ZipEntry(pp.relativize(path).toString());
                try {
                    zs.putNextEntry(zipEntry);
                    Files.copy(path, zs);
                    zs.closeEntry();
                } catch (IOException e) {
                    LOGGER.error("Error adding zip entry", e);
                }
            });
            LOGGER.debug("zip file of the directory '" + sourceDirPath + "' has been created");
        } catch (IOException e) {
            LOGGER.error(e);
            return false;
        }
        return true;
    }

    private void sendBindingObject(Connection c, String path, NIOData d) {
        if (path.contains("#")) {
            BindingObject bo = BindingObject.generate(path);
            if (bo.getElements() > 0) {
                ByteBuffer bb = BindingDataManager.getByteArray(bo.getName());
                if (bb != null) {
                    c.sendDataByteBuffer(bb);
                } else {
                    ErrorManager.warn("Can't send binding data '" + path + "' via connection " + c.hashCode()
                        + " because bytebuffer is null.");
                    releaseSendSlot(c);
                    handleDataToSendNotAvailable(c, d);
                }
            } else {
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Sending native object " + bo.getName());
                }
                NIOBindingObjectStream ncs = new NIOBindingObjectStream((NIOConnection) c, null);
                int res = NIOBindingDataManager.sendNativeObject(bo.getName(), ncs);
                if (res != 0) {
                    ErrorManager.warn("Can't send binding data '" + path + "' via connection " + c.hashCode()
                        + " because sending native object call returned " + res);
                    releaseSendSlot(c);
                    handleDataToSendNotAvailable(c, d);
                }
            }
        } else {
            ErrorManager.warn("Can't send binding data '" + path + "' via connection " + c.hashCode()
                + " because incorrect path (doesn't contain #).");
            releaseSendSlot(c);
            handleDataToSendNotAvailable(c, d);
        }

    }

    private void sendBindingObjectAsFile(Connection c, String path, NIOData d) {
        if (path.contains("#")) {
            BindingObject bo = BindingObject.generate(path);
            File f = new File(bo.getId());
            if (BindingDataManager.isInBinding(bo.getName())) {
                int res = BindingDataManager.storeInFile(bo.getName(), bo.getId());
                if (res == 0) {
                    sendFile(c, f.getAbsolutePath(), d);
                } else {
                    ErrorManager.warn("Can't send binding data '" + path + "' via connection " + c.hashCode()
                        + " because error serializing binding object.");
                    releaseSendSlot(c);
                    handleDataToSendNotAvailable(c, d);
                }
            } else {
                if (f.exists()) {
                    sendFile(c, f.getAbsolutePath(), d);
                } else {
                    ErrorManager.warn("Can't send binding data '" + bo.getId() + "' via connection " + c.hashCode()
                        + " because file doesn't exists.");
                    releaseSendSlot(c);
                    handleDataToSendNotAvailable(c, d);
                }
            }

        } else {
            File f = new File(path);
            if (f.exists()) {
                sendFile(c, f.getAbsolutePath(), d);
            } else {
                ErrorManager.warn("Can't send binding data '" + path + "' via connection " + c.hashCode()
                    + " because incorrect path (doesn't contain #).");
                releaseSendSlot(c);
                handleDataToSendNotAvailable(c, d);
            }
        }

    }

    /**
     * Received NIOData.
     *
     * @param c Connection.
     * @param t Transfer.
     */
    public void receivedData(Connection c, Transfer t) {
        String dataId = this.ongoingTransfers.remove(c);
        if (dataId == null) {
            // It has received the output and error of a job execution
            return;
        }
        if (DEBUG) {
            LOGGER.debug(DBG_PREFIX + "Receiving data " + dataId);
        }
        releaseReceiveSlot();

        // Add tracing event
        if (NIOTracer.isActivated()) {
            int tag = Math.abs(dataId.hashCode());
            NIOTracer.emitDataTransferEvent(dataId, false);
            NIOTracer.emitCommEvent(false, this.connection2partner.get(c), tag, t.getSize());
            this.connection2partner.remove(c);
        }

        // Get all data requests for this source data_id/filename, and group by the target(final) data_id/filename
        List<DataRequest> requests = dataToRequests.remove(dataId);
        if (requests == null || requests.isEmpty()) {
            LOGGER.warn("WARN: No data removed for received data " + dataId);
            return;
        }
        Map<String, List<DataRequest>> byTarget = new HashMap<>();
        for (DataRequest req : requests) {
            LOGGER.debug(DBG_PREFIX + "Group by target:" + req.getTarget() + "(" + dataId + ")");
            List<DataRequest> sameTarget = byTarget.get(req.getTarget());
            if (sameTarget == null) {
                sameTarget = new LinkedList<>();
                byTarget.put(req.getTarget(), sameTarget);
            }
            sameTarget.add(req);
        }

        // files, binding objects, and directories are all transferred as files, get the exact data type from the
        // request
        DataType drType = requests.get(0).getType();
        boolean isBindingType = drType.equals(DataType.BINDING_OBJECT_T);
        boolean isDirectory = drType.equals(DataType.DIRECTORY_T);

        if (byTarget.size() == 1) {
            // if only target data_id value requested raise reception notification with target name

            String targetName = requests.get(0).getTarget();
            if (DEBUG) {
                LOGGER.debug(DBG_PREFIX + "Data " + dataId + " will be saved as name " + targetName);
            }
            if (t.isFile()) {
                if (!isPersistentCEnabled() && isBindingType) {
                    // When worker binding is not persistent binding objects can be transferred as files
                    receivedBindingObjectAsFile(t.getFileName(), targetName);
                }

                if (isDirectory) {
                    String zipFile = targetName.concat(COMPRESSED_DIR_EXTENSION);
                    LOGGER.debug(DBG_PREFIX + "Compressed data " + zipFile + " will be decompressed  and saved as "
                        + targetName);
                    // todo: what to do if decompression fails?
                    extractFolder(zipFile, targetName);
                    receivedValue(t.getDestination(), targetName, t.getObject(), requests);
                } else {
                    receivedValue(t.getDestination(), targetName, t.getObject(), requests);
                }
            } else {
                if (t.isObject()) {
                    receivedValue(t.getDestination(), getName(targetName), t.getObject(), requests);

                } else {
                    if (t.isByteBuffer()) {
                        String boPath = requests.get(0).getSource().getFirstURI().getPath();
                        BindingObject bo = getTargetBindingObject(targetName, boPath);
                        NIOBindingDataManager.setByteArray(bo.getName(), t.getByteBuffer(), bo.getType(),
                            bo.getElements());
                        receivedValue(t.getDestination(), targetName, bo.toString(), requests);
                    } else {
                        // Object already store in the cache
                        BindingObject bo =
                            getTargetBindingObject(targetName, requests.get(0).getSource().getFirstURI().getPath());
                        receivedValue(t.getDestination(), targetName, bo.toString(), requests);
                    }
                }
            }
        } else {
            String workingDir = getWorkingDir();
            if (!workingDir.endsWith(File.separator)) {
                workingDir = workingDir + File.separator;
            }
            // If more than one. First notify reception with original name (IN case)
            if (DEBUG) {
                LOGGER.debug(DBG_PREFIX + "Data " + dataId + " will be saved as name " + dataId);
            }
            if (t.isFile()) {
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Data " + dataId + " will be saved as name " + t.getFileName());
                }
                List<DataRequest> reqs;
                if (!isPersistentCEnabled() && isBindingType) {
                    // When worker binding is not persistent binding objects can be transferred as files
                    BindingObject bo = getTargetBindingObject(t.getFileName(), requests.get(0).getTarget());
                    reqs = byTarget.remove(bo.toString());
                    receivedBindingObjectAsFile(t.getFileName(), reqs.get(0).getTarget());
                } else {
                    reqs = byTarget.remove(t.getFileName());
                }

                if (isDirectory) {
                    String zipFile = new File(t.getFileName()).getName();
                    String targetName = t.getFileName().replace(COMPRESSED_DIR_EXTENSION, "");
                    LOGGER.debug(DBG_PREFIX + "Compressed data " + zipFile + " will be decompressed  and saved as "
                        + targetName);
                    extractFolder(zipFile, targetName);
                    receivedValue(t.getDestination(), targetName, t.getObject(), reqs);
                } else {
                    receivedValue(t.getDestination(), t.getFileName(), t.getObject(), reqs);
                }
            } else {
                if (t.isObject()) {
                    if (DEBUG) {
                        LOGGER.debug(DBG_PREFIX + "Data " + dataId + " will be saved as name " + dataId);
                    }
                    receivedValue(t.getDestination(), getName(dataId), t.getObject(), byTarget.remove(dataId));
                } else {
                    if (t.isByteBuffer()) {
                        BindingObject bo = getTargetBindingObject(workingDir + dataId, requests.get(0).getTarget());
                        if (DEBUG) {
                            LOGGER.debug(DBG_PREFIX + "Data " + dataId + " with target " + bo.toString()
                                + " will be saved as name " + dataId);
                        }
                        NIOBindingDataManager.setByteArray(bo.getName(), t.getByteBuffer(), bo.getType(),
                            bo.getElements());
                        receivedValue(t.getDestination(), dataId, bo.toString(), byTarget.remove(bo.toString()));
                    } else {
                        BindingObject bo = getTargetBindingObject(workingDir + dataId, requests.get(0).getTarget());
                        if (DEBUG) {
                            LOGGER.debug(DBG_PREFIX + "Data " + dataId + " with target " + bo.toString()
                                + "will be saved as name " + dataId);
                        }

                        receivedValue(t.getDestination(), dataId, bo.toString(), byTarget.remove(bo.toString()));
                    }
                }
            }
            // Then, replicate value with target data_id/filename (INOUT case) and notify reception with target
            // data_id/filename

            // TODO: We copy byTarget to avoid concurrent modifications. Should be synchronized somehow.
            @SuppressWarnings("unchecked")
            Entry<String, List<DataRequest>>[] targetEntries = byTarget.entrySet().toArray(new Entry[byTarget.size()]);

            for (Entry<String, List<DataRequest>> entry : targetEntries) {
                String targetName = entry.getKey();
                List<DataRequest> reqs = entry.getValue();
                try {
                    if (DEBUG) {
                        LOGGER.debug(DBG_PREFIX + "Data " + dataId + " will be saved as name " + targetName);
                    }

                    if (t.isFile()) {
                        if (!isPersistentCEnabled() && isBindingType) {
                            BindingObject bo = getTargetBindingObject(targetName, requests.get(0).getTarget());
                            // When worker binding is not persistent binding objects can be transferred as files
                            receivedBindingObjectAsFile(t.getFileName(), targetName);
                            receivedValue(t.getDestination(), bo.getName(), bo.toString(), byTarget.remove(targetName));

                        } else {
                            Files.copy((new File(t.getFileName())).toPath(), (new File(targetName)).toPath());
                            receivedValue(t.getDestination(), targetName, t.getObject(), byTarget.remove(targetName));
                        }
                    } else {
                        if (t.isObject()) {
                            Object o = Serializer.deserialize(t.getArray());
                            receivedValue(t.getDestination(), getName(targetName), o, reqs);
                        } else {
                            BindingObject bo = getTargetBindingObject(targetName, requests.get(0).getTarget());
                            NIOBindingDataManager.copyCachedData(dataId, bo.getName());
                            receivedValue(t.getDestination(), bo.getName(), bo.toString(), byTarget.remove(targetName));
                        }
                    }

                } catch (IOException | ClassNotFoundException e) {
                    LOGGER.warn("Can not replicate received Data", e);
                }
            }
        }
        requestTransfers();

        // Check if shutdown and ready
        if (this.finish == true && !hasPendingTransfers()) {
            shutdown(closingConnection);
        }
    }

    private boolean extractFolder(String zipFilePath, String destination) {
        // Remove destination directory
        File destDir = new File(destination);
        if (destDir.exists()) {
            LOGGER.warn("Removing existing file: " + destination);
            if (destDir.isDirectory()) {
                // Delete directory
                try {
                    FileUtils.deleteDirectory(destDir);
                } catch (IOException ioe) {
                    LOGGER.error("Cannot remove existing folder: " + destination, ioe);
                    LOGGER.error("Cannot extract: " + zipFilePath);
                    return false;
                }
            } else {
                // Delete file
                if (!destDir.delete()) {
                    LOGGER.error("Cannot remove existing file: " + destination);
                    LOGGER.error("Cannot extract: " + zipFilePath);
                    return false;
                }
            }
        }

        // Create extraction directory
        destDir.mkdirs();

        // Extract data
        final int bufferSize = 2_048;

        File zipAsFile = new File(zipFilePath);
        try (ZipFile zip = new ZipFile(zipAsFile)) {
            @SuppressWarnings("unchecked")
            Enumeration<ZipEntry> zipFileEntries = (Enumeration<ZipEntry>) zip.entries();
            while (zipFileEntries.hasMoreElements()) {
                // Grab a zip file entry
                ZipEntry entry = zipFileEntries.nextElement(); // NOSONAR extracted folder is ziped by COMPSs
                String currentEntry = entry.getName();

                File destFile = new File(destination, currentEntry);
                File destinationParent = destFile.getParentFile();

                // Create the parent directory structure if needed
                destinationParent.mkdirs();

                if (!entry.isDirectory()) {
                    try (BufferedInputStream is = new BufferedInputStream(zip.getInputStream(entry));
                        BufferedOutputStream dest =
                            new BufferedOutputStream(new FileOutputStream(destFile), bufferSize)) {

                        // Read and write until last byte is encountered
                        int currentByte;
                        byte[] data = new byte[bufferSize];
                        while ((currentByte = is.read(data, 0, bufferSize)) != -1) {
                            dest.write(data, 0, currentByte);
                        }
                        dest.flush();
                    }
                }
            }
        } catch (IOException ioe) {
            LOGGER.error("ERROR extracting ZIP file " + zipFilePath, ioe);
            return false;
        }

        // Remove ZIP file
        if (!zipAsFile.delete()) {
            LOGGER.warn(" Cannot remove zip file after decompression: " + zipAsFile.getName());
        }

        // All ok
        return true;
    }

    private String getName(String path) {
        int index = path.lastIndexOf(File.separator);
        if (index > 0) {
            return path.substring(index + 1);
        } else {
            return path;
        }
    }

    private BindingObject getTargetBindingObject(String target, String originPath) {
        BindingObject bo;
        if (target.contains("#")) {
            bo = BindingObject.generate(target);
        } else {
            BindingObject boOr = BindingObject.generate(originPath);
            bo = new BindingObject(target, boOr.getType(), boOr.getElements());
        }
        return bo;
    }

    /**
     * Receives the Shutdown request.
     *
     * @param requester Requester connection of the shutdown.
     * @param filesToSend List of files to send.
     */
    public void receivedShutdown(Connection requester, List<NIOData> filesToSend) {
        if (DEBUG) {
            LOGGER.debug(DBG_PREFIX + "Command for shutdown received. Preparing for shutdown...");
        }
        this.closingConnection = requester;
        this.finish = true;

        // Order copies of filesToSend?
        if (!hasPendingTransfers()) {
            shutdown(closingConnection);
        } else {
            LOGGER.error("[ERROR] Pending transfers...");
            for (DataRequest dr : pendingRequests) {
                LOGGER.debug("[DEBUG] Pending: " + dr.getTarget() + " " + dr.getSource().getDataMgmtId());
            }
        }
    }

    /**
     * Check whether there are available receive slots or not.
     *
     * @return {@code true} If there are receive slots available, {@code false} otherwise.
     */
    private boolean tryAcquireReceiveSlot() {
        boolean b = false;
        synchronized (this) {
            if (this.receiveTransfers < this.maxReceiveTransfers) {
                this.receiveTransfers++;
                b = true;
            }
        }
        return b;
    }

    /**
     * Release Receive slot.
     */
    private void releaseReceiveSlot() {
        synchronized (this) {
            this.receiveTransfers--;
        }
    }

    /**
     * Check whether there is a transfer slot available or not.
     *
     * @param c Connection.
     * @return {@code true} if there is a transfer slot available, {@code false} otherwise.
     */
    public boolean tryAcquireSendSlot(Connection c) {
        boolean b = false;
        if (this.sendTransfers < this.maxSendTransfers) {
            this.sendTransfers++;

            b = true;
            for (int i = 0; i < this.maxSendTransfers; i++) {
                if (this.trasmittingConnections[i] == null) {
                    this.trasmittingConnections[i] = c;
                    break;
                }
            }
        }
        return b;
    }

    /**
     * Release send slot.
     *
     * @param c Connection.
     */
    public void releaseSendSlot(Connection c) {
        synchronized (this) {
            for (int i = 0; i < this.maxSendTransfers; i++) {
                if (this.trasmittingConnections[i] == c) {
                    this.trasmittingConnections[i] = null;
                    this.sendTransfers--;
                    if (this.finish) {
                        if (!hasPendingTransfers()) {
                            shutdown(this.closingConnection);
                        }
                    }
                    break;
                }
            }

        }
    }

    /**
     * Receive notification data not available.
     *
     * @param c Connection.
     */
    public boolean checkAndHandleRequestedDataNotAvailableError(Connection c) {
        String dataId = this.ongoingTransfers.remove(c);
        if (dataId == null) { // It has received the output and error of a job
            LOGGER.error("Failed data connection not a tranfer");
            return false;
        }
        // Remove connection from commands Hasmap
        unregisterConnectionInOngoingCommands(c);
        releaseReceiveSlot();
        List<DataRequest> requests = this.dataToRequests.remove(dataId);
        handleRequestedDataNotAvailableError(requests, dataId);
        requestTransfers();

        // Check if shutdown and ready
        if (this.finish == true && !hasPendingTransfers()) {
            shutdown(this.closingConnection);
        }
        return true;
    }

    /**
     * Returns whether the persistent C storage is enabled or not.
     *
     * @return {@code true} if the persistent C storage is enabled, {@code false} otherwise.
     */
    public abstract boolean isPersistentCEnabled();

    // Must be implemented on both sides (Master will do nothing)
    public abstract void setMaster(NIONode master);

    protected abstract String getPossiblyRenamedFileName(File originalFile, NIOData d);

    public abstract boolean isMyUuid(String uuid, String nodeName);

    // This will use the TreeMap to set the corresponding worker starter as ready
    public abstract void setWorkerIsReady(String nodeName);

    public abstract String getWorkingDir();

    public abstract void receivedNewTask(NIONode master, NIOTask t, List<String> obsoleteFiles);

    public abstract void receivedNewDataFetchOrder(NIOParam data, int transferId);

    public abstract Object getObject(String s) throws SerializedObjectException;

    public abstract String getObjectAsFile(String name);

    // Called when a value couldn't be SENT because, for example, the file to be sent it didn't exist
    protected abstract void handleDataToSendNotAvailable(Connection c, NIOData d);

    // Called when a value couldn't be RETRIEVED because, for example, the file
    // to be retrieved it didn't exist in the sender
    public abstract void handleRequestedDataNotAvailableError(List<DataRequest> failedRequests, String dataId);

    public abstract void receivedBindingObjectAsFile(String filename, String targetPath);

    public abstract void receivedValue(Destination type, String dataId, Object object,
        List<DataRequest> achievedRequests);

    public abstract void copiedData(int transfergroupID);

    public abstract void receivedNIOTaskDone(Connection c, NIOTaskResult tr, NIOTaskProfile profile, boolean successful,
        Exception e);

    public abstract void shutdown(Connection closingConnection);

    public abstract void shutdownNotification(Connection c);

    public abstract void shutdownExecutionManager(Connection closingConnection);

    public abstract void shutdownExecutionManagerNotification(Connection c);

    // Generates the debug files to be retrieved by the master post mortem and returns the paths to those files
    public abstract void generateDebugFiles(Connection c);

    // Generates the analysis files to be retrieved by the master post mortem and returns the paths to those files
    public abstract void generateAnalysisFiles(Connection c);

    public abstract void notifyDebugFilesDone(Set<String> logPath);

    public abstract void notifyAnalysisFilesDone(Set<String> tracingFilesPaths);

    public void receivedPartialBindingObjects(Connection c, Transfer t) {
        NIOBindingDataManager.receivedPartialBindingObject((NIOConnection) c, t);
    }

    public abstract void increaseResources(MethodResourceDescription description);

    public abstract void reduceResources(MethodResourceDescription description);

    public abstract void performedResourceUpdate(Connection c);

    public abstract void cancelRunningTask(NIONode node, int jobId);

    public abstract void unhandeledError(Connection c);

    public abstract void handleCancellingTaskCommandError(Connection c, CommandCancelTask commandCancelTask);

    public abstract void handleDataReceivedCommandError(Connection c, CommandDataReceived commandDataReceived);

    public abstract void handleExecutorShutdownCommandError(Connection c,
        CommandExecutorShutdown commandExecutorShutdown);

    public abstract void handleExecutorShutdownCommandACKError(Connection c,
        CommandExecutorShutdownACK commandExecutorShutdownACK);

    public abstract void handleTaskDoneCommandError(Connection c, CommandNIOTaskDone commandNIOTaskDone);

    public abstract void handleNewTaskCommandError(Connection c, CommandNewTask commandNewTask);

    public abstract void handleShutdownCommandError(Connection c, CommandShutdown commandShutdown);

    public abstract void handleShutdownACKCommandError(Connection c, CommandShutdownACK commandShutdownACK);

    public abstract void handleTracingGenerateDoneCommandError(Connection c,
        CommandGenerateAnalysisFilesDone commandGenerateAnalysisFilesDone);

    public abstract void handleTracingGenerateCommandError(Connection c,
        CommandGenerateAnalysisFiles commandGenerateAnalysisFiles);

    public abstract void handleGenerateWorkerDebugCommandError(Connection c,
        CommandGenerateDebugFiles commandGenerateDebugFiles);

    public abstract void handleGenerateWorkerDebugDoneCommandError(Connection c,
        CommandGenerateDebugFilesDone commandGenerateDebugFilesDone);

    /**
     * Re-send a given command to a given NIONode.
     *
     * @param node NIO node to re-send the command
     * @param cmd Command to re-send
     */
    protected void resendCommand(NIONode node, Command cmd) {
        Connection c = TM.startConnection(node);
        registerOngoingCommand(c, cmd);
        c.sendCommand(cmd);
        c.finishConnection();
    }

    public static void registerOngoingCommand(Connection connection, Command command) {
        ONGOING_COMMANDS.put(connection, command);
    }

    protected void unregisterConnectionInOngoingCommands(Connection connection) {
        ONGOING_COMMANDS.remove(connection);
    }

    /**
     * Check and handle if error in connection is for a command.
     *
     * @param c Connection with an error.
     * @return Returns true True if error is in a command and it has been managed, otherwise returns False
     */
    public boolean checkAndHandleCommandError(Connection c) {
        Command command = ONGOING_COMMANDS.remove(c);
        if (command == null) {
            LOGGER.error("Failed connection not a command");
            return false;
        }
        command.error(this, c);
        return true;

    }

    public abstract void receivedRemoveObsoletes(NIONode node, List<String> obsolete);

    public abstract void handleRemoveObsoletesCommandError(Connection c, CommandRemoveObsoletes commandRemoveObsoletes);

    public abstract void workerPongReceived(String nodeName);

    /**
     * Node is down, do something..
     *
     * @param nodeName String
     */
    public abstract void handleNodeIsDownError(String nodeName);
}
