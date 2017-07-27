package es.bsc.compss.types.request.ap;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.data.location.DataLocation;

import java.util.concurrent.Semaphore;

import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.RAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.operation.FileTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;

/**
 * The TransferRawFileRequest class represents a request to transfer a file
 * located in a worker to be transferred to another location without register
 * the transfer
 */
public class TransferOpenFileRequest extends APRequest {

    /**
     * Data Id and version of the requested file
     */
    private DataAccessId faId;
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
     * @param sem Semaphore where to synchronize until the operation is done
     */
    public TransferOpenFileRequest(DataAccessId faId, Semaphore sem) {
        this.faId = faId;
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
    public DataAccessId getFaId() {
        return faId;
    }

    /**
     * Sets the data Id and version of the requested file
     *
     * @param faId Data Id and version of the requested file
     */
    public void setFaId(DataAccessId faId) {
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
        LOGGER.debug("Process TransferOpenFileRequest");
        if (faId instanceof DataAccessId.WAccessId) {
            DataAccessId.WAccessId waId = (DataAccessId.WAccessId) faId;
            DataInstanceId targetFile = waId.getWrittenDataInstance();
            String targetName = targetFile.getRenaming();
            String targetPath = Comm.getAppHost().getTempDirPath() + targetName;

            DataLocation targetLocation = null;
            try {
                SimpleURI targetURI = new SimpleURI(DataLocation.Protocol.FILE_URI.getSchema() + targetPath);
                targetLocation = DataLocation.createLocation(Comm.getAppHost(), targetURI);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
            }

            LOGGER.debug(targetFile + " to be opened as " + targetLocation);
            Comm.registerLocation(targetName, targetLocation);
            setLocation(targetLocation);

            sem.release();
        } else if (faId instanceof DataAccessId.RWAccessId) {
            DataAccessId.RWAccessId waId = (DataAccessId.RWAccessId) faId;
            String srcName = waId.getReadDataInstance().getRenaming();
            String targetName = waId.getWrittenDataInstance().getRenaming();
            String targetPath = Comm.getAppHost().getTempDirPath() + targetName;

            DataLocation targetLocation = null;
            try {
                SimpleURI targetURI = new SimpleURI(DataLocation.Protocol.FILE_URI.getSchema() + targetPath);
                targetLocation = DataLocation.createLocation(Comm.getAppHost(), targetURI);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath);
            }

            setLocation(targetLocation);
            Comm.getAppHost().getData(srcName, targetName, (LogicalData) null, new FileTransferable(), new OneOpWithSemListener(sem));
        } else {
            RAccessId waId = (RAccessId) faId;
            String srcName = waId.getReadDataInstance().getRenaming();
            String targetName = waId.getReadDataInstance().getRenaming();
            String targetPath = Comm.getAppHost().getTempDirPath() + targetName;

            DataLocation targetLocation = null;
            try {
                SimpleURI targetURI = new SimpleURI(DataLocation.Protocol.FILE_URI.getSchema() + targetPath);
                targetLocation = DataLocation.createLocation(Comm.getAppHost(), targetURI);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath);
            }

            setLocation(targetLocation);
            Comm.getAppHost().getData(srcName, srcName, new FileTransferable(), new OneOpWithSemListener(sem));
        }
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.TRANSFER_OPEN_FILE;
    }

}
