package es.bsc.compss.types.request.ap;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.data.location.DataLocation;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.RAccessId;
import es.bsc.compss.types.data.DataAccessId.RWAccessId;
import es.bsc.compss.types.data.DataAccessId.WAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.operation.FileTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;


/**
 * The TransferRawFileRequest class represents a request to transfer a file located in a worker to be transferred to
 * another location without register the transfer
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
     * @param faId
     *            Data Id and version of the requested file
     * @param sem
     *            Semaphore where to synchronize until the operation is done
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
     * @param sem
     *            Semaphore where to synchronize until the operation is done
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
     * @param faId
     *            Data Id and version of the requested file
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
     * @param location
     *            Location where to leave the requested file
     */
    public void setLocation(DataLocation location) {
        this.location = location;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        LOGGER.debug("Process TransferOpenFileRequest");

        // Get target information
        String targetName;
        String targetPath;
        if (faId instanceof WAccessId) {
            // Write mode
            WAccessId waId = (WAccessId) faId;
            DataInstanceId targetFile = waId.getWrittenDataInstance();
            targetName = targetFile.getRenaming();
            targetPath = Comm.getAppHost().getTempDirPath() + targetName;
        } else if (faId instanceof RWAccessId) {
            // Read write mode
            RWAccessId rwaId = (RWAccessId) faId;
            targetName = rwaId.getWrittenDataInstance().getRenaming();
            targetPath = Comm.getAppHost().getTempDirPath() + targetName;
        } else {
            // Read only mode
            RAccessId raId = (RAccessId) faId;
            targetName = raId.getReadDataInstance().getRenaming();
            targetPath = Comm.getAppHost().getTempDirPath() + targetName;
        }
        LOGGER.debug("Openning file " + targetName + " at " + targetPath);

        // Create location
        DataLocation targetLocation = null;

        String pscoId = Comm.getData(targetName).getId();
        if (pscoId == null) {
            try {
                SimpleURI targetURI = new SimpleURI(DataLocation.Protocol.FILE_URI.getSchema() + targetPath);
                targetLocation = DataLocation.createLocation(Comm.getAppHost(), targetURI);
            } catch (IOException ioe) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, ioe);
            }
        } else {
            // It is an external object persisted inside the task
            try {
                SimpleURI targetURI = new SimpleURI(DataLocation.Protocol.PERSISTENT_URI.getSchema() + pscoId);
                targetLocation = DataLocation.createLocation(Comm.getAppHost(), targetURI);
            } catch (IOException ioe) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, ioe);
            }
        }

        // Register target location
        LOGGER.debug("Setting target location to " + targetLocation);
        setLocation(targetLocation);

        // Ask for transfer when required
        if (pscoId != null) {
            LOGGER.debug("External object detected. Auto-release");
            Comm.registerLocation(targetName, targetLocation);
            sem.release();
        } else if (faId instanceof WAccessId) {
            LOGGER.debug("Write only mode. Auto-release");
            Comm.registerLocation(targetName, targetLocation);
            sem.release();
        } else if (faId instanceof RWAccessId) {
            LOGGER.debug("RW mode. Asking for transfer");
            RWAccessId rwaId = (RWAccessId) faId;
            String srcName = rwaId.getReadDataInstance().getRenaming();
            Comm.getAppHost().getData(srcName, targetName, (LogicalData) null, new FileTransferable(), new OneOpWithSemListener(sem));
        } else {
            LOGGER.debug("Read only mode. Asking for transfer");
            RAccessId raId = (RAccessId) faId;
            String srcName = raId.getReadDataInstance().getRenaming();
            Comm.getAppHost().getData(srcName, srcName, new FileTransferable(), new OneOpWithSemListener(sem));
        }
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.TRANSFER_OPEN_FILE;
    }

}
