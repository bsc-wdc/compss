package es.bsc.compss.types.request.ap;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.data.location.DataLocation;
import java.util.concurrent.Semaphore;

import es.bsc.compss.types.data.DataAccessId.RAccessId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.operation.FileTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;

/**
 * The TransferRawFileRequest class represents a request to transfer a file
 * located in a worker to be transferred to another location without register
 * the transfer
 */
public class TransferRawFileRequest extends APRequest {

    /**
     * Data Id and version of the requested file
     */
    private RAccessId faId;
    /**
     * Location where to leave the requested file
     */
    private DataLocation location;
    /**
     * Semaphore where to synchronize until the operation is done
     */
    private Semaphore sem;

    /**
     * Constructs a new TransferOpenFileRequest
     *
     * @param faId Data Id and version of the requested file
     * @param location Location where to leave the requested file
     * @param sem Semaphore where to synchronize until the operation is done
     */
    public TransferRawFileRequest(RAccessId faId, DataLocation location, Semaphore sem) {
        this.faId = faId;
        this.location = location;
        this.sem = sem;
    }

    /**
     * Returns the semaphore where to synchronize until the operation is done
     *
     * @return Semaphore where to synchronize until the operation is done
     */
    public Semaphore getSemaphore() {
        return sem;
    }

    /**
     * Sets the semaphore where to synchronize until the operation is done
     *
     * @param sem Semaphore where to synchronize until the operation is done
     */
    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    /**
     * Returns the data Id and version of the requested file
     *
     * @return Data Id and version of the requested file
     */
    public RAccessId getFaId() {
        return faId;
    }

    /**
     * Sets the data Id and version of the requested file
     *
     * @param faId Data Id and version of the requested file
     */
    public void setFaId(RAccessId faId) {
        this.faId = faId;
    }

    /**
     * Returns the location where to leave the requested file
     *
     * @return the location where to leave the requested file
     */
    public DataLocation getLocation() {
        return location;
    }

    /**
     * Sets the location where to leave the requested file
     *
     * @param location Location where to leave the requested file
     */
    public void setLocation(DataLocation location) {
        this.location = location;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        // Make a copy of the original logical file, we don't want to leave track
        String sourceName = faId.getReadDataInstance().getRenaming();
        Comm.getAppHost().getData(sourceName, location, (LogicalData) null, new FileTransferable(), new OneOpWithSemListener(sem));
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.TRANSFER_RAW_FILE;
    }

}
