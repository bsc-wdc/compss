package es.bsc.compss.nio.listeners;

import es.bsc.comm.Connection;
import es.bsc.compss.data.MultiOperationFetchListener;
import es.bsc.compss.nio.commands.CommandDataReceived;
import es.bsc.compss.nio.worker.NIOWorker;


public class FetchDataOperationListener extends MultiOperationFetchListener {

    private final int transferId;
    private final NIOWorker nw;


    /**
     * Creates a new Fetch Data operation listener.
     * 
     * @param transferId Transfer data Id.
     * @param nw Associated NIO Worker.
     */
    public FetchDataOperationListener(int transferId, NIOWorker nw) {
        this.transferId = transferId;
        this.nw = nw;
    }

    @Override
    public void doCompleted() {
        CommandDataReceived cdr = new CommandDataReceived(this.nw, this.transferId);
        Connection c = this.nw.startConnection();
        c.sendCommand(cdr);
        c.finishConnection();
    }

    @Override
    public void doFailure(String failedDataId, Exception cause) {
        // Nothing to do
    }

}
