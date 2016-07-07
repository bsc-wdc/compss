package integratedtoolkit.nio;

import java.util.LinkedList;

import es.bsc.comm.Connection;
import es.bsc.comm.TransferManager;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.stage.Transfer.Destination;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.commands.CommandDataDemand;
import integratedtoolkit.nio.commands.CommandTracingID;
import integratedtoolkit.nio.commands.Data;
import integratedtoolkit.nio.commands.tracing.CommandGenerateDone;
import integratedtoolkit.nio.exceptions.SerializedObjectException;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Serializer;

import java.io.File;

import static java.lang.Math.abs;

import java.nio.file.Files;
import java.util.HashMap;

import org.apache.log4j.Logger;


public abstract class NIOAgent {

    protected static final String NIOEventManagerClass = "es.bsc.comm.nio.NIOEventManager";

    public static final String ID = NIOAgent.class.getCanonicalName();
    private int sendTransfers;
    private final int MAX_SEND_TRANSFERS;
    private final Connection[] trasmittingConnections;
    private int receiveTransfers;
    private final int MAX_RECEIVE_TRANSFERS;

    private boolean finish;
    private Connection closingConnection = null;

    // Requests related to a DataId
    protected final HashMap<String, LinkedList<DataRequest>> dataToRequests;
    // Data requests that will be transferred
    private final LinkedList<DataRequest> pendingRequests;
    // Ongoing transfers
    private final HashMap<Connection, String> ongoingTransfers;

    // Transfers to send as soon as there is a slot available
    // TODO
    //private LinkedList<Data> prioritaryData;
    // IP of the master node
    protected String masterIP;
    protected static int masterPort;
    protected NIONode masterNode;

    // Transfer Manager instance
    public static final TransferManager tm = new TransferManager();

    //Logging
    private static final Logger logger = Logger.getLogger(Loggers.COMM);

    // Tracing
    protected static boolean tracing;
    protected static int tracing_level;
    protected static int tracingID = 0; // unless NIOWorker sets this value; 0 -> master (NIOAdaptor)
    protected static HashMap<Connection, Integer> connection2Partner;

    public NIOAgent(int snd, int rcv, int port) {
        sendTransfers = 0;
        MAX_SEND_TRANSFERS = snd;
        trasmittingConnections = new Connection[MAX_SEND_TRANSFERS];
        receiveTransfers = 0;
        MAX_RECEIVE_TRANSFERS = rcv;
        masterPort = port;
        ongoingTransfers = new HashMap<Connection, String>();
        pendingRequests = new LinkedList<DataRequest>();
        dataToRequests = new HashMap<String, LinkedList<DataRequest>>();
        connection2Partner = new HashMap<Connection, Integer>();
        finish = false;
    }

    public abstract void receivedNewTask(NIONode master, NIOTask t, LinkedList<String> obsoleteFiles);

    public abstract void setMaster(NIONode master); // must be implemented on both sides (on master will do nothing)

    public abstract boolean isMyUuid(String uuid);

    public abstract void setWorkerIsReady(String nodeName); // this will use the Treemap to set the corresponding worker starter as ready (I can use connection.getNode())

    public void addConnectionAndPartner(Connection c, int partner, int tag) {
        connection2Partner.put(c, partner);
    }

    // Reply the data
    public void sendData(Connection c, Data d, int receiverID) {

        if (tracing) {
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
                logger.debug("Connection " + c.hashCode() + " will transfer file " + path + " as data " + d.getName());
                c.sendDataFile(path);
            } else {
                ErrorManager.warn("Can't send file '" + path + "' via connection " + c.hashCode() + " because file doesn't exist.");
                handleDataToSendNotAvailable(c, d);
            }
        } else {
            try {
                Object o = getObject(path);
                logger.debug("Connection " + c.hashCode() + " will transfer an object as data " + d.getName());
                c.sendDataObject(o);
            } catch (SerializedObjectException soe) {
                // Exception has been raised because object has been serialized
                String newLocation = getObjectAsFile(path);
                logger.debug("Connection " + c.hashCode() + " will transfer an object-file " + newLocation + " as data " + d.getName());
                c.sendDataFile(newLocation);
            }

        }
        if (tracing) {
            NIOTracer.emitDataTransferEvent(NIOTracer.TRANSFER_END);
        }
        c.finishConnection();
    }

    // Check if there is a transfer slot available
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

    private void releaseReceiveSlot() {
        synchronized (this) {
            receiveTransfers--;
        }
    }

    // Check if there is a transfer slot available
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

    // Check if this node has the data
    //public abstract boolean checkData(Data d);
    public NIONode getMaster() {
        return masterNode;
    }

    public abstract Object getObject(String s) throws SerializedObjectException;

    public abstract String getObjectAsFile(String name);

    public abstract String getWorkingDir();

    public void receivedShutdown(Connection requester, LinkedList<Data> filesToSend) {
    	logger.debug("Command for shutdown received. Preparing for shutdown...");
        closingConnection = requester;
        finish = true;

        // Order copies of filesToSend?
        if (!hasPendingTransfers()) {
            shutdown(closingConnection);
        }
    }

    public abstract void receivedTaskDone(Connection c, int jobID, NIOTask nt, boolean successful);

    public abstract void copiedData(int transfergroupID);

    public abstract void shutdownNotification(Connection c);

    public abstract static class DataRequest {

        private final Data source;
        private final DataType type;
        private final String getTarget;

        public DataRequest(DataType type, Data source, String target) {
            this.source = source;
            this.getTarget = target;
            this.type = type;
        }

        public Data getSource() {
            return source;
        }

        public String getTarget() {
            return getTarget;
        }

        public DataType getType() {
            return type;
        }

        public static class MasterDataRequest extends DataRequest {

            final DataOperation fOp;

            public MasterDataRequest(DataOperation fOp, DataType type, Data source, String target) {
                super(type, source, target);
                this.fOp = fOp;
            }

            public DataOperation getOperation() {
                return this.fOp;
            }

        }
    }

    public void addTransferRequest(DataRequest dr) {
        LinkedList<DataRequest> list = dataToRequests.get(dr.source.getName());
        if (list == null) {
            list = new LinkedList<DataRequest>();
            dataToRequests.put(dr.source.getName(), list);
            synchronized (pendingRequests) {
                pendingRequests.add(dr);
            }
        }
        list.add(dr);
    }

    // Check if receive slots available
    public void requestTransfers() {

        DataRequest dr = null;
        synchronized (pendingRequests) {
            if (!pendingRequests.isEmpty() && tryAcquireReceiveSlot()) {
                dr = pendingRequests.remove();
            }
        }
        while (dr != null) {
            Data source = dr.source;
            NIOURI uri = source.getFirstURI();

            if (tracing) {
                NIOTracer.emitDataTransferEvent(source.getName());
            }
            NIONode nn = uri.getHost();
            if (nn.ip == null) {
                nn = masterNode;
            }
            Connection c = null;

            try {
                c = tm.startConnection(nn);
                logger.debug("Connection " + c.hashCode() + " will be used to acquire data " + dr.getTarget + " stored in " + nn + " with name " + dr.source.getName());
                Data remoteData = new Data(source.getName(), uri);
                CommandDataDemand cdd = new CommandDataDemand(this, remoteData, tracingID);
                ongoingTransfers.put(c, dr.source.getName());
                c.sendCommand(cdd);
                if (tracing) {
                    c.receive();
                }
                if (dr.type == DataType.FILE_T) {
                    c.receiveDataFile(dr.getTarget);
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
            if (tracing) {
                NIOTracer.emitDataTransferEvent(NIOTracer.TRANSFER_END);
            }
        }
    }

    public void receivedRequestedDataNotAvailableError(Connection c, Transfer t) {

        String dataId = ongoingTransfers.remove(c);
        if (dataId == null) { // It has received the output and error of a job execution
            return;
        }

        releaseReceiveSlot();
        LinkedList<DataRequest> requests = dataToRequests.remove(dataId);
        handleRequestedDataNotAvailableError(requests, dataId);
        requestTransfers();

        // Check if shutdown and ready
        if (finish == true && !hasPendingTransfers()) {
            shutdown(closingConnection);
        }
    }

    public void receivedData(Connection c, Transfer t) {

        String dataId = ongoingTransfers.remove(c);
        if (dataId == null) { // It has received the output and error of a job execution
            return;
        }
        releaseReceiveSlot();
        LinkedList<DataRequest> requests = dataToRequests.remove(dataId);
        HashMap<String, LinkedList<DataRequest>> byTarget = new HashMap<String, LinkedList<DataRequest>>();
        for (DataRequest req : requests) {
            LinkedList<DataRequest> sameTarget = byTarget.get(req.getTarget());
            if (sameTarget == null) {
                sameTarget = new LinkedList<DataRequest>();
                byTarget.put(req.getTarget(), sameTarget);
            }
            sameTarget.add(req);
        }

        if (tracing) {
            int tag = abs(dataId.hashCode());
            NIOTracer.emitDataTransferEvent(dataId);

            NIOTracer.emitCommEvent(false, connection2Partner.get(c), tag, t.getSize());
            connection2Partner.remove(c);
        }

        if (byTarget.size() == 1) {
            String targetName = requests.getFirst().getTarget();
            receivedValue(t.getDestination(), targetName, t.getObject(), requests);
        } else {
            if (t.isFile()) {
                receivedValue(t.getDestination(), t.getFileName(), t.getObject(), byTarget.remove(t.getFileName()));
            } else {
                receivedValue(t.getDestination(), dataId, t.getObject(), byTarget.remove(dataId));
            }
            for (java.util.Map.Entry<String, LinkedList<DataRequest>> entry : byTarget.entrySet()) {
                String targetName = entry.getKey();
                LinkedList<DataRequest> reqs = entry.getValue();
                try {
                    if (t.isFile()) {
                        Files.copy((new File(t.getFileName())).toPath(), (new File(targetName)).toPath());
                        receivedValue(t.getDestination(), targetName, t.getObject(), byTarget.remove(targetName));
                    } else {
                        Object o = Serializer.deserialize(t.getArray());
                        receivedValue(t.getDestination(), targetName, o, reqs);
                    }
                } catch (Exception e) {
                    System.err.println("Can not replicate received Data");
                    e.printStackTrace(System.err);
                }

            }
        }
        requestTransfers();

        // Check if shutdown and ready
        if (finish == true && !hasPendingTransfers()) {
            shutdown(closingConnection);
        }

    }

    protected LinkedList<DataRequest> getDataRequests(String dataId) {
        return dataToRequests.get(dataId);
    }

    //Called when a value couldn't be SENT because, for example, the file to be sent it didnt exist
    protected abstract void handleDataToSendNotAvailable(Connection c, Data d);

    //Called when a value couldn't be RETRIEVED because, for example, the file to be retrieved it didnt exist in the sender
    public abstract void handleRequestedDataNotAvailableError(LinkedList<DataRequest> failedRequests, String dataId);

    public abstract void receivedValue(Destination type, String dataId, Object object, LinkedList<DataRequest> achievedRequests);

    public abstract void shutdown(Connection closingConnection);

    public boolean hasPendingTransfers() {
        return !pendingRequests.isEmpty() || sendTransfers != 0 || receiveTransfers != 0;
    }

    public void generatePackage(Connection c) {
        NIOTracer.generatePackage();
        c.sendCommand(new CommandGenerateDone());
        c.finishConnection();
    }

    public abstract void generateWorkersDebugInfo(Connection c);

    public abstract void waitUntilTracingPackageGenerated();

    public abstract void notifyTracingPackageGeneration();

    public abstract void waitUntilWorkersDebugInfoGenerated();

    public abstract void notifyWorkersDebugInfoGeneration();

}
