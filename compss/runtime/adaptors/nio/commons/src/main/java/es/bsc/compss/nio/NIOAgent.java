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
package es.bsc.compss.nio;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import es.bsc.comm.Connection;
import es.bsc.comm.TransferManager;
import es.bsc.comm.nio.NIOConnection;
import es.bsc.comm.nio.NIOEventManager;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.stage.Transfer.Destination;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.nio.commands.CommandDataDemand;
import es.bsc.compss.nio.commands.CommandTracingID;
import es.bsc.compss.nio.commands.Data;
import es.bsc.compss.nio.commands.tracing.CommandGenerateDone;
import es.bsc.compss.nio.dataRequest.DataRequest;
import es.bsc.compss.nio.exceptions.SerializedObjectException;
import es.bsc.compss.nio.utils.NIOBindingDataManager;
import es.bsc.compss.nio.utils.NIOBindingObjectStream;
import es.bsc.compss.util.BindingDataManager;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Serializer;

import java.io.File;
import java.io.IOException;

import static java.lang.Math.abs;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class NIOAgent {

    protected static final String NIO_EVENT_MANAGER_CLASS = NIOEventManager.class.getCanonicalName();
    public static final String ID = NIOAgent.class.getCanonicalName();

    public static final int NUM_PARAMS_PER_WORKER_SH = 5;
    public static final int NUM_PARAMS_NIO_WORKER = 32;
    public static final String BINDER_DISABLED = "disabled";
    public static final String BINDER_AUTOMATIC = "automatic";

    private int sendTransfers;
    private final int MAX_SEND_TRANSFERS;
    private final Connection[] trasmittingConnections;
    private int receiveTransfers;
    private final int MAX_RECEIVE_TRANSFERS;

    private boolean finish;
    private Connection closingConnection = null;

    // Requests related to a DataId
    protected final Map<String, List<DataRequest>> dataToRequests;
    // Data requests that will be transferred
    private final LinkedList<DataRequest> pendingRequests;
    // Ongoing transfers
    private final Map<Connection, String> ongoingTransfers;

    // Transfers to send as soon as there is a slot available
    // private LinkedList<Data> prioritaryData;
    // IP of the master node
    // protected String masterIP;
    protected static int masterPort;
    protected NIONode masterNode;

    // Transfer Manager instance
    protected static final TransferManager TM = new TransferManager();

    // Logging
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final String DBG_PREFIX = "[NIO Agent] ";

    // Tracing
    protected static boolean tracing;
    protected static boolean persistentC;
    protected static int tracing_level;
    protected static int tracingID = 0; // unless NIOWorker sets this value; 0 -> master (NIOAdaptor)
    protected static HashMap<Connection, Integer> connection2Partner;


    /**
     * Constructor
     *
     * @param snd
     * @param rcv
     * @param port
     */
    public NIOAgent(int snd, int rcv, int port) {
        sendTransfers = 0;
        MAX_SEND_TRANSFERS = snd;
        trasmittingConnections = new Connection[MAX_SEND_TRANSFERS];
        receiveTransfers = 0;
        MAX_RECEIVE_TRANSFERS = rcv;
        masterPort = port;
        ongoingTransfers = new HashMap<>();
        pendingRequests = new LinkedList<>();
        dataToRequests = new HashMap<>();
        connection2Partner = new HashMap<>();
        finish = false;
        LOGGER.debug(DBG_PREFIX + "Debug: " + DEBUG + " persistent: " + persistentC);
    }

    /**
     * Returns the master node
     *
     * @return
     */
    public NIONode getMaster() {
        return masterNode;
    }

    public static TransferManager getTransferManager() {
        return TM;
    }

    /**
     * Adds connection and partner
     *
     * @param c
     * @param partner
     * @param tag
     */
    public void addConnectionAndPartner(Connection c, int partner, int tag) {
        connection2Partner.put(c, partner);
    }

    /**
     * Returns DataRequests of a given dataId
     *
     * @param dataId
     * @return
     */
    protected List<DataRequest> getDataRequests(String dataId) {
        return dataToRequests.get(dataId);
    }

    /**
     * Returns if there are pending transfers or not
     *
     * @return
     */
    public boolean hasPendingTransfers() {
    	LOGGER.debug("pending: " + !pendingRequests.isEmpty() + " sendTransfers: " + (sendTransfers != 0) + " receiveTrasnfers: " + (receiveTransfers != 0) + "\n");
        return !pendingRequests.isEmpty() || sendTransfers != 0 || receiveTransfers != 0;
    }

    /**
     * Check if receive slots available
     */
    public void requestTransfers() {
        DataRequest dr = null;
        synchronized (pendingRequests) {
            if (!pendingRequests.isEmpty() && tryAcquireReceiveSlot()) {
                dr = pendingRequests.remove();
            }
        }
        while (dr != null) {
            Data source = dr.getSource();
            NIOURI uri = source.getFirstURI();

            if (NIOTracer.isActivated()) {
                NIOTracer.emitDataTransferEvent(source.getName());
            }
            NIONode nn = uri.getHost();
            if (nn.getIp() == null) {
                nn = masterNode;
            }
            Connection c = null;

            try {
                c = TM.startConnection(nn);
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will be used to acquire data " + dr.getTarget()
                            + " stored in " + nn + " with name " + dr.getSource().getName());
                }
                Data remoteData = new Data(source.getName(), uri);
                CommandDataDemand cdd = new CommandDataDemand(this, remoteData, tracingID);
                ongoingTransfers.put(c, dr.getSource().getName());
                c.sendCommand(cdd);

                if (NIOTracer.isActivated()) {
                    c.receive();
                }
                if (dr.getType() == DataType.FILE_T) {
                    c.receiveDataFile(dr.getTarget());
                    c.finishConnection();
                } else if (dr.getType() == DataType.BINDING_OBJECT_T) {
                    if (persistentC) {
                        receiveBindingObject(c, dr);
                    } else {
                        receiveBindingObjectAsFile(c, dr);
                    }
                } else {
                    c.receiveDataObject();
                    c.finishConnection();
                }

            } catch (Exception e) {
                e.printStackTrace(System.err);
                if (c != null) {
                    c.finishConnection();
                }
            }
            synchronized (pendingRequests) {
                if (!pendingRequests.isEmpty() && tryAcquireReceiveSlot()) {
                    dr = pendingRequests.remove();
                } else {
                    dr = null;
                }
            }

            if (NIOTracer.isActivated()) {
                NIOTracer.emitDataTransferEvent(NIOTracer.TRANSFER_END);
            }
        }
    }

    private void receiveBindingObject(Connection c, DataRequest dr) {
        if (DEBUG) {
            LOGGER.debug(DBG_PREFIX + "Receiving binding data " + dr.getTarget() + " from " + dr.getSource().getFirstURI().getPath());
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
            LOGGER.debug(
                    DBG_PREFIX + "Receiving binding data " + dr.getTarget() + " as file from " + dr.getSource().getFirstURI().getPath());
        }
        // BindingObject bo = BindingObject.generate(dr.getSource().getFirstURI().getPath());
        String targetId = dr.getTarget();
        BindingObject bo = BindingObject.generate(targetId);
        c.receiveDataFile(bo.getId());
        c.finishConnection();
    }

    /**
     * Adds a new Data Transfer Request
     *
     * @param dr
     */
    public void addTransferRequest(DataRequest dr) {
        List<DataRequest> list = dataToRequests.get(dr.getSource().getName());
        if (list == null) {
            list = new LinkedList<DataRequest>();
            dataToRequests.put(dr.getSource().getName(), list);
            synchronized (pendingRequests) {
                pendingRequests.add(dr);
            }
        }
        list.add(dr);
    }

    /**
     * Reply the data
     *
     * @param c
     * @param d
     * @param receiverID
     */
    public void sendData(Connection c, Data d, int receiverID) {
        if (NIOTracer.isActivated()) {
            int tag = abs(d.getName().hashCode());
            CommandTracingID cmd = new CommandTracingID(tracingID, tag);
            c.sendCommand(cmd);
            NIOTracer.emitDataTransferEvent(d.getName());
            NIOTracer.emitCommEvent(true, receiverID, tag);
        }

        String path = d.getFirstURI().getPath();
        Protocol scheme = d.getFirstURI().getProtocol();
        if (scheme == Protocol.FILE_URI) {
            sendFile(c, path, d);
        } else if (scheme == Protocol.BINDING_URI) {
            if (persistentC) {
                sendBindingObject(c, path, d);
            } else {
                sendBindingObjectAsFile(c, path, d);
            }
        } else {
            sendObject(c, path, d);

        }

        if (NIOTracer.isActivated()) {
            NIOTracer.emitDataTransferEvent(NIOTracer.TRANSFER_END);
        }
        c.finishConnection();
    }

    private void sendObject(Connection c, String path, Data d) {
        try {
            Object o = getObject(path);
            if (DEBUG) {
                LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will transfer an object as data " + d.getName());
            }
            c.sendDataObject(o);
        } catch (SerializedObjectException soe) {
            // Exception has been raised because object has been serialized
            String newLocation = getObjectAsFile(path);
            if (DEBUG) {
                LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will transfer an object-file " + newLocation + " as data "
                        + d.getName());
            }
            sendFile(c, newLocation, d);
        }

    }

    private void sendFile(Connection c, String path, Data d) {
        // TODO: Not sure if it is needed with the addition of the protocol in the NIOURI. To check
        if (path.startsWith(File.separator)) {
            File f = new File(path);
            if (f.exists()) {
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will transfer file " + path + " as data " + d.getName());
                }
                c.sendDataFile(path);
            } else {
                // Not found check if it has been moved to the target name (renames
                if (!f.getName().equals(d.getName())) {
                    File renamed;
                    if (isMaster()) { // renamed will be in the masters file path
                        renamed = new File(Comm.getAppHost().getCompleteRemotePath(DataType.FILE_T, d.getName()).getPath());
                    } else { // worker renamed will be in the same path
                        renamed = new File(f.getParentFile().getAbsolutePath() + File.separator + d.getName());
                    }
                    if (renamed.exists()) {
                        if (DEBUG) {
                            LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will transfer file " + renamed.getAbsolutePath()
                                    + " as data " + d.getName());
                        }
                        c.sendDataFile(renamed.getAbsolutePath());
                    } else {
                        ErrorManager.warn("Can't send niether file '" + path + "' nor file '" + renamed.getAbsolutePath()
                                + "' via connection " + c.hashCode() + " because files don't exist.");
                        handleDataToSendNotAvailable(c, d);
                    }
                } else {
                    ErrorManager.warn("Can't send file '" + path + "' via connection " + c.hashCode() + " because file doesn't exist.");
                    handleDataToSendNotAvailable(c, d);
                }
            }
        } else {
            if (DEBUG) {
                LOGGER.debug(DBG_PREFIX + "Connection " + c.hashCode() + " will transfer object of data " + d.getName());
            }
            sendObject(c, path, d);
        }
    }

    protected abstract boolean isMaster();

    private void sendBindingObject(Connection c, String path, Data d) {
        if (path.contains("#")) {
            BindingObject bo = BindingObject.generate(path);
            if (bo.getElements() > 0) {
                ByteBuffer bb = BindingDataManager.getByteArray(bo.getName());
                if (bb != null) {
                    c.sendDataByteBuffer(bb);
                } else {
                    ErrorManager
                            .warn("Can't send binding data '" + path + "' via connection " + c.hashCode() + " because bytebuffer is null.");
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
                    handleDataToSendNotAvailable(c, d);
                }
            }
        } else {
            ErrorManager.warn("Can't send binding data '" + path + "' via connection " + c.hashCode()
                    + " because incorrect path (doesn't contain #).");
            handleDataToSendNotAvailable(c, d);
        }

    }

    private void sendBindingObjectAsFile(Connection c, String path, Data d) {
        if (path.contains("#")) {
            BindingObject bo = BindingObject.generate(path);
            File f = new File(bo.getId());
            if (BindingDataManager.isInBinding(bo.getName())) {
                int res = BindingDataManager.storeInFile(bo.getName(), bo.getId());
                if (res == 0) {
                    sendFile(c, new File(bo.getId()).getAbsolutePath(), d);
                } else {
                    ErrorManager.warn("Can't send binding data '" + path + "' via connection " + c.hashCode()
                            + " because error serializing binding object.");
                    handleDataToSendNotAvailable(c, d);
                }
            } else {
                if (f.exists()) {
                    sendFile(c, bo.getId(), d);
                } else {
                    ErrorManager.warn(
                            "Can't send binding data '" + path + "' via connection " + c.hashCode() + " because file doesn't exists.");
                    handleDataToSendNotAvailable(c, d);
                }
            }

        } else {
            File f = new File(path);
            if (f.exists()) {
                sendFile(c, path, d);
            } else {
                ErrorManager.warn("Can't send binding data '" + path + "' via connection " + c.hashCode()
                        + " because incorrect path (doesn't contain #).");
                handleDataToSendNotAvailable(c, d);
            }
        }

    }

    /**
     * Received Data
     *
     * @param c
     * @param t
     */
    public void receivedData(Connection c, Transfer t) {

        String dataId = ongoingTransfers.remove(c);
        if (dataId == null) { // It has received the output and error of a job
            // execution
            return;
        }
        if (DEBUG) {
            LOGGER.debug(DBG_PREFIX + "Receiving data " + dataId);
        }
        releaseReceiveSlot();
        // Get all data requests for this source data_id/filename, and group by the target(final) data_id/filename
        List<DataRequest> requests = dataToRequests.remove(dataId);
        boolean isBindingType = requests.get(0).getType().equals(DataType.BINDING_OBJECT_T);
        Map<String, List<DataRequest>> byTarget = new HashMap<>();
        for (DataRequest req : requests) {
            LOGGER.debug(DBG_PREFIX + "Group by target:" +req.getTarget()+ "("+dataId+")");
            List<DataRequest> sameTarget = byTarget.get(req.getTarget());
            if (sameTarget == null) {
                sameTarget = new LinkedList<DataRequest>();
                byTarget.put(req.getTarget(), sameTarget);
            }
            sameTarget.add(req);
        }
        // Add tracing event
        if (NIOTracer.isActivated()) {
            int tag = abs(dataId.hashCode());
            NIOTracer.emitDataTransferEvent(dataId);
            NIOTracer.emitCommEvent(false, connection2Partner.get(c), tag, t.getSize());
            connection2Partner.remove(c);
        }

        if (byTarget.size() == 1) {
            // if only target data_id value requested raise reception notification with target name

            String targetName = requests.get(0).getTarget();
            if (DEBUG) {
                LOGGER.debug(DBG_PREFIX + "Data " + dataId + " will be saved as name " + targetName);
            }
            if (t.isFile() || t.isObject()) {
                if (!isPersistentEnabled() && isBindingType) {
                    // When worker binding is not persistent binding objects can be transferred as files
                    receivedBindingObjectAsFile(t.getFileName(), targetName);
                }
                receivedValue(t.getDestination(), targetName, t.getObject(), requests);

            } else if (t.isByteBuffer()) {
                BindingObject bo = getTargetBindingObject(targetName, requests.get(0).getSource().getFirstURI().getPath());
                NIOBindingDataManager.setByteArray(bo.getName(), t.getByteBuffer(), bo.getType(), bo.getElements());
                receivedValue(t.getDestination(), targetName, bo.toString(), requests);
            } else {
                // Object already store in the cache
                BindingObject bo = getTargetBindingObject(targetName, requests.get(0).getSource().getFirstURI().getPath());
                receivedValue(t.getDestination(), targetName, bo.toString(), requests);
            }
        } else {
            String workingDir = getWorkingDir();
            if (!workingDir.endsWith(File.separator)){
                workingDir = workingDir+File.separator;
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
                if (!isPersistentEnabled() && isBindingType) {
                    // When worker binding is not persistent binding objects can be transferred as files
                    BindingObject bo = getTargetBindingObject(t.getFileName(), requests.get(0).getTarget());
                    reqs = byTarget.remove(bo.toString());
                    receivedBindingObjectAsFile(t.getFileName(), reqs.get(0).getTarget());
                }else{
                    reqs = byTarget.remove(t.getFileName());
                }
                receivedValue(t.getDestination(), t.getFileName(), t.getObject(), reqs);
            } else if (t.isObject()) {
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Data " + dataId + " will be saved as name " + dataId);
                }
                receivedValue(t.getDestination(), dataId, t.getObject(), byTarget.remove(dataId));
            } else if (t.isByteBuffer()) {                
                BindingObject bo = getTargetBindingObject(workingDir+ dataId, requests.get(0).getTarget());
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Data " + dataId + " with target " + bo.toString() + " will be saved as name " + dataId);
                }
                NIOBindingDataManager.setByteArray(bo.getName(), t.getByteBuffer(), bo.getType(), bo.getElements());
                receivedValue(t.getDestination(), dataId, bo.toString(), byTarget.remove(bo.toString()));
            } else {
                BindingObject bo = getTargetBindingObject(workingDir+ dataId, requests.get(0).getTarget());
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Data " + dataId + " with target " + bo.toString() + "will be saved as name " + dataId);
                }
                
                receivedValue(t.getDestination(), dataId, bo.toString(), byTarget.remove(bo.toString()));
            }
            // Then, replicate value with target data_id/filename (INOUT case) and notify reception with target
            // data_id/filename
            for (Entry<String, List<DataRequest>> entry : byTarget.entrySet()) {
                String targetName = entry.getKey();
                List<DataRequest> reqs = entry.getValue();
                try {
                    if (DEBUG) {
                        LOGGER.debug(DBG_PREFIX + "Data " + dataId + " will be saved as name " + targetName);
                    }
                    if (t.isFile()) {

                        if (!isPersistentEnabled() && isBindingType) {
                            BindingObject bo = getTargetBindingObject(targetName, requests.get(0).getTarget());
                            // When worker binding is not persistent binding objects can be transferred as files
                            receivedBindingObjectAsFile(t.getFileName(), targetName);
                            receivedValue(t.getDestination(), bo.getName(), bo.toString(), byTarget.remove(targetName));
                        
                        } else {
                            Files.copy((new File(t.getFileName())).toPath(), (new File(targetName)).toPath());
                            receivedValue(t.getDestination(), targetName, t.getObject(), byTarget.remove(targetName));
                        }
                        
                    } else if (t.isObject()) {
                        Object o = Serializer.deserialize(t.getArray());
                        receivedValue(t.getDestination(), targetName, o, reqs);
                    } else {
                        BindingObject bo = getTargetBindingObject(targetName, requests.get(0).getTarget());
                        NIOBindingDataManager.copyCachedData(dataId, bo.getName());
                        receivedValue(t.getDestination(), bo.getName(), bo.toString(), byTarget.remove(targetName));
                    }
                } catch (IOException | ClassNotFoundException e) {
                    LOGGER.warn("Can not replicate received Data", e);
                }

            }
        }
        requestTransfers();

        // Check if shutdown and ready
        if (finish == true && !hasPendingTransfers()) {
            shutdown(closingConnection);
        }

    }

    private BindingObject getTargetBindingObject(String target, String origin_path) {
        BindingObject bo;
        if (target.contains("#")) {
            bo = BindingObject.generate(target);
        } else {
            BindingObject bo_or = BindingObject.generate(origin_path);
            bo = new BindingObject(target, bo_or.getType(), bo_or.getElements());
        }
        return bo;
    }

    /**
     * Receives the Shutdown
     *
     * @param requester
     * @param filesToSend
     */
    public void receivedShutdown(Connection requester, List<Data> filesToSend) {
        if (DEBUG) {
            LOGGER.debug(DBG_PREFIX + "Command for shutdown received. Preparing for shutdown...");
        }
        closingConnection = requester;
        finish = true;

        // Order copies of filesToSend?
        if (!hasPendingTransfers()) {
            shutdown(closingConnection);
        }
        else {
        	LOGGER.error("[ERROR] Pending transfers...");
        	for (DataRequest dr : pendingRequests) {
        		LOGGER.debug("[DEBUG] Pending: " + dr.getTarget() + " " + dr.getSource().getName());
        	}
        }
    }

    /**
     * Check if there is a transfer slot available
     *
     * @return
     */
    private boolean tryAcquireReceiveSlot() {
        boolean b = false;
        synchronized (this) {
            if (receiveTransfers < MAX_RECEIVE_TRANSFERS) {
                receiveTransfers++;
                b = true;
            }
        }
        return b;
    }

    /**
     * Release Receive slot
     */
    private void releaseReceiveSlot() {
        synchronized (this) {
            receiveTransfers--;
        }
    }

    /**
     * Check if there is a transfer slot available
     *
     * @param c
     * @return
     */
    public boolean tryAcquireSendSlot(Connection c) {
        boolean b = false;
        if (sendTransfers < MAX_SEND_TRANSFERS) {
            sendTransfers++;

            b = true;
            for (int i = 0; i < MAX_SEND_TRANSFERS; i++) {
                if (trasmittingConnections[i] == null) {
                    trasmittingConnections[i] = c;
                    break;
                }
            }
        }
        return b;
    }

    /**
     * Release send slot
     *
     * @param c
     */
    public void releaseSendSlot(Connection c) {
        synchronized (this) {
            for (int i = 0; i < MAX_SEND_TRANSFERS; i++) {
                if (trasmittingConnections[i] == c) {
                    trasmittingConnections[i] = null;
                    sendTransfers--;
                    if (finish) {
                        if (!hasPendingTransfers()) {
                            shutdown(closingConnection);
                        }
                    }
                    break;
                }
            }

        }
    }

    /**
     * Receive notification data not available
     *
     * @param c
     * @param t
     */
    public void receivedRequestedDataNotAvailableError(Connection c, Transfer t) {
        String dataId = ongoingTransfers.remove(c);
        if (dataId == null) { // It has received the output and error of a job
            // execution
            return;
        }

        releaseReceiveSlot();
        List<DataRequest> requests = dataToRequests.remove(dataId);
        handleRequestedDataNotAvailableError(requests, dataId);
        requestTransfers();

        // Check if shutdown and ready
        if (finish == true && !hasPendingTransfers()) {
            shutdown(closingConnection);
        }
    }

    protected static void setPersistent(boolean persistent) {
        if (DEBUG) {
            LOGGER.debug(DBG_PREFIX + "Setting persistent as " + persistent);
        }
        persistentC = persistent;
    }

    public static boolean isPersistentEnabled() {
        return persistentC;
    }

    /**
     * Generate Tracing package
     *
     * @param c
     */
    public void generatePackage(Connection c) {
        NIOTracer.generatePackage();
        c.sendCommand(new CommandGenerateDone());
        c.finishConnection();
    }

    // Must be implemented on both sides (Master will do anything)
    public abstract void setMaster(NIONode master);

    public abstract boolean isMyUuid(String uuid, String nodeName);

    // This will use the TreeMap to set the corresponding worker starter as ready
    public abstract void setWorkerIsReady(String nodeName);

    public abstract String getWorkingDir();

    public abstract void receivedNewTask(NIONode master, NIOTask t, List<String> obsoleteFiles);

    public abstract Object getObject(String s) throws SerializedObjectException;

    public abstract String getObjectAsFile(String name);

    // Called when a value couldn't be SENT because, for example, the file to be sent it didn't exist
    protected abstract void handleDataToSendNotAvailable(Connection c, Data d);

    // Called when a value couldn't be RETRIEVED because, for example, the file
    // to be retrieved it didn't exist in the sender
    public abstract void handleRequestedDataNotAvailableError(List<DataRequest> failedRequests, String dataId);

    public abstract void receivedBindingObjectAsFile(String filename, String targetPath);

    public abstract void receivedValue(Destination type, String dataId, Object object, List<DataRequest> achievedRequests);

    public abstract void copiedData(int transfergroupID);

    public abstract void receivedNIOTaskDone(Connection c, NIOTaskResult tr, boolean successful);

    public abstract void shutdown(Connection closingConnection);

    public abstract void shutdownNotification(Connection c);

    public abstract void shutdownExecutionManager(Connection closingConnection);

    public abstract void shutdownExecutionManagerNotification(Connection c);

    public abstract void waitUntilTracingPackageGenerated();

    public abstract void notifyTracingPackageGeneration();

    public abstract void generateWorkersDebugInfo(Connection c);

    public abstract void waitUntilWorkersDebugInfoGenerated();

    public abstract void notifyWorkersDebugInfoGeneration();

    public void receivedPartialBindingObjects(Connection c, Transfer t) {
        NIOBindingDataManager.receivedPartialBindingObject((NIOConnection) c, t);

    }

}
