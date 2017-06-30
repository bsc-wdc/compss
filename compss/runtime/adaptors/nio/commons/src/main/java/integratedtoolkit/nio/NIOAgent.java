package integratedtoolkit.nio;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import es.bsc.comm.Connection;
import es.bsc.comm.TransferManager;
import es.bsc.comm.nio.NIOEventManager;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.stage.Transfer.Destination;

import integratedtoolkit.log.Loggers;

import integratedtoolkit.types.annotations.parameter.DataType;

import integratedtoolkit.nio.commands.CommandDataDemand;
import integratedtoolkit.nio.commands.CommandTracingID;
import integratedtoolkit.nio.commands.Data;
import integratedtoolkit.nio.commands.tracing.CommandGenerateDone;
import integratedtoolkit.nio.dataRequest.DataRequest;
import integratedtoolkit.nio.exceptions.SerializedObjectException;

import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Serializer;

import java.io.File;
import java.io.IOException;

import static java.lang.Math.abs;

import java.nio.file.Files;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class NIOAgent {

    protected static final String NIO_EVENT_MANAGER_CLASS = NIOEventManager.class.getCanonicalName();
    public static final String ID = NIOAgent.class.getCanonicalName();

    public static final int NUM_PARAMS_PER_WORKER_SH = 5;
    public static final int NUM_PARAMS_NIO_WORKER = 25;
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
    // TODO
    // private LinkedList<Data> prioritaryData;
    // IP of the master node
    protected String masterIP;
    protected static int masterPort;
    protected NIONode masterNode;

    // Transfer Manager instance
    protected static final TransferManager TM = new TransferManager();

    // Logging
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    // Tracing
    protected static boolean tracing;
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
                LOGGER.debug("Connection " + c.hashCode() + " will be used to acquire data " + dr.getTarget() + " stored in " + nn
                        + " with name " + dr.getSource().getName());
                Data remoteData = new Data(source.getName(), uri);
                CommandDataDemand cdd = new CommandDataDemand(this, remoteData, tracingID);
                ongoingTransfers.put(c, dr.getSource().getName());
                c.sendCommand(cdd);

                if (NIOTracer.isActivated()) {
                    c.receive();
                }
                if (dr.getType() == DataType.FILE_T) {
                    c.receiveDataFile(dr.getTarget());
                } else {
                    c.receiveDataObject();
                }

            } catch (Exception e) {
                e.printStackTrace(System.err);
            } finally {
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

        if (path.startsWith(File.separator)) {
            File f = new File(path);
            if (f.exists()) {
                LOGGER.debug("Connection " + c.hashCode() + " will transfer file " + path + " as data " + d.getName());
                c.sendDataFile(path);
            } else {
                ErrorManager.warn("Can't send file '" + path + "' via connection " + c.hashCode() + " because file doesn't exist.");
                handleDataToSendNotAvailable(c, d);
            }
        } else {
            try {
                Object o = getObject(path);
                LOGGER.debug("Connection " + c.hashCode() + " will transfer an object as data " + d.getName());
                c.sendDataObject(o);
            } catch (SerializedObjectException soe) {
                // Exception has been raised because object has been serialized
                String newLocation = getObjectAsFile(path);
                LOGGER.debug("Connection " + c.hashCode() + " will transfer an object-file " + newLocation + " as data " + d.getName());
                c.sendDataFile(newLocation);
            }

        }

        if (NIOTracer.isActivated()) {
            NIOTracer.emitDataTransferEvent(NIOTracer.TRANSFER_END);
        }
        c.finishConnection();
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
        releaseReceiveSlot();
        List<DataRequest> requests = dataToRequests.remove(dataId);
        Map<String, List<DataRequest>> byTarget = new HashMap<>();
        for (DataRequest req : requests) {
            List<DataRequest> sameTarget = byTarget.get(req.getTarget());
            if (sameTarget == null) {
                sameTarget = new LinkedList<DataRequest>();
                byTarget.put(req.getTarget(), sameTarget);
            }
            sameTarget.add(req);
        }

        if (NIOTracer.isActivated()) {
            int tag = abs(dataId.hashCode());
            NIOTracer.emitDataTransferEvent(dataId);

            NIOTracer.emitCommEvent(false, connection2Partner.get(c), tag, t.getSize());
            connection2Partner.remove(c);
        }

        if (byTarget.size() == 1) {
            String targetName = requests.get(0).getTarget();
            receivedValue(t.getDestination(), targetName, t.getObject(), requests);
        } else {
            if (t.isFile()) {
                receivedValue(t.getDestination(), t.getFileName(), t.getObject(), byTarget.remove(t.getFileName()));
            } else {
                receivedValue(t.getDestination(), dataId, t.getObject(), byTarget.remove(dataId));
            }
            for (Entry<String, List<DataRequest>> entry : byTarget.entrySet()) {
                String targetName = entry.getKey();
                List<DataRequest> reqs = entry.getValue();
                try {
                    if (t.isFile()) {
                        Files.copy((new File(t.getFileName())).toPath(), (new File(targetName)).toPath());
                        receivedValue(t.getDestination(), targetName, t.getObject(), byTarget.remove(targetName));
                    } else {
                        Object o = Serializer.deserialize(t.getArray());
                        receivedValue(t.getDestination(), targetName, o, reqs);
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

    /**
     * Receives the Shutdown
     *
     * @param requester
     * @param filesToSend
     */
    public void receivedShutdown(Connection requester, List<Data> filesToSend) {
        LOGGER.debug("Command for shutdown received. Preparing for shutdown...");
        closingConnection = requester;
        finish = true;

        // Order copies of filesToSend?
        if (!hasPendingTransfers()) {
            shutdown(closingConnection);
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

    public abstract boolean isMyUuid(String uuid);

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

    public abstract void receivedValue(Destination type, String dataId, Object object, List<DataRequest> achievedRequests);

    public abstract void copiedData(int transfergroupID);

    public abstract void receivedTaskDone(Connection c, NIOTaskResult tr, boolean successful);

    public abstract void shutdown(Connection closingConnection);

    public abstract void shutdownNotification(Connection c);

    public abstract void shutdownExecutionManager(Connection closingConnection);

    public abstract void shutdownExecutionManagerNotification(Connection c);

    public abstract void waitUntilTracingPackageGenerated();

    public abstract void notifyTracingPackageGeneration();

    public abstract void generateWorkersDebugInfo(Connection c);

    public abstract void waitUntilWorkersDebugInfoGenerated();

    public abstract void notifyWorkersDebugInfoGeneration();

}
