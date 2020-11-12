/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.TaskListener;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import java.util.concurrent.Semaphore;


/**
 * Request to be notified when a data is ready to be accessed by the main; i.e., the value has been produced an no other
 * accesses are being done in concurrent.
 */
public class WaitForDataRequest extends APRequest implements TaskListener {

    private final int dataId;
    private final AccessMode taskMode;
    private final AccessMode concurrentMode;
    private final Semaphore sem;
    private int pendingOperation = 0;


    /**
     * Creates a new request to wait for a task completion.
     *
     * @param dataId Data Id.
     * @param taskMode Access mode for to the value
     * @param concurrentMode Access mode for the concurrent
     * @param sem Waiting semaphore.
     */
    public WaitForDataRequest(int dataId, AccessMode taskMode, AccessMode concurrentMode, Semaphore sem) {
        this.dataId = dataId;
        this.taskMode = taskMode;
        this.concurrentMode = concurrentMode;
        this.sem = sem;
    }

    /**
     * Returns the associated data Id.
     *
     * @return The associated data Id.
     */
    public int getDataId() {
        return this.dataId;
    }

    /**
     * Returns the associated access mode to the data.
     *
     * @return The associated access mode to the data.
     */
    public AccessParams.AccessMode getTaskAccessMode() {
        return this.taskMode;
    }

    /**
     * Returns the associated access mode to the data for the concurrent accesses.
     *
     * @return The associated access mode to the data for the concurrent accesses.
     */
    public AccessParams.AccessMode getConcurrentAccessMode() {
        return this.concurrentMode;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        ta.waitForData(this);
        if (pendingOperation == 0) {
            sem.release();
        }
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.WAIT_FOR_DATA;
    }

    public void addPendingOperation() {
        pendingOperation++;
    }

    @Override
    public void taskFinished() {
        pendingOperation--;
        if (pendingOperation == 0) {
            sem.release();
        }
    }
}
