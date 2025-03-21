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
package es.bsc.compss.checkpoint;

import es.bsc.compss.checkpoint.types.CheckpointCopy;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.operation.FileTransferable;
import java.util.LinkedList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CheckpointCopiesHandler {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.CP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Concurrent copies limit management (pendingCopies acts as semaphore for atomicity)
    private static final int CONCURRENT_COPIES_LIMIT = 5;

    // List of the data pending to be copied
    private final LinkedList<CheckpointCopy> pendingCopies;
    private int ongoingCopies;


    public CheckpointCopiesHandler() {
        this.ongoingCopies = 0;
        this.pendingCopies = new LinkedList<>();
    }

    /**
     * Request the handler to perform a copy.
     *
     * @param data data to copy
     * @param targetLocation location where to copy the data
     * @param listener element monitoring changes on the copy
     */
    public void requestCopy(LogicalData data, DataLocation targetLocation, EventListener listener) {
        boolean doCopy = false;
        synchronized (this) {
            if (ongoingCopies < CONCURRENT_COPIES_LIMIT) {
                ongoingCopies++;
                doCopy = true;
            } else {
                CheckpointCopy pc = new CheckpointCopy(data, targetLocation, listener);
                pendingCopies.add(pc);
            }
        }

        if (doCopy) {
            orderCopy(data, targetLocation, listener);
        }
    }

    /**
     * Notifies that a copy has ended.
     */
    public void completedCopy() {
        CheckpointCopy pr;
        synchronized (this) {
            pr = pendingCopies.poll();
            if (pr == null) {
                ongoingCopies--;
            }
        }

        if (pr != null) {
            orderCopy(pr);
        }
    }

    /**
     * Launches all the copies in parallel.
     */
    public void ignoreConcurrencyLimit() {
        synchronized (this) {
            ongoingCopies += this.pendingCopies.size();
            for (CheckpointCopy pr : this.pendingCopies) {
                orderCopy(pr);
            }
            this.pendingCopies.clear();
        }
    }

    private void orderCopy(CheckpointCopy pr) {
        orderCopy(pr.getData(), pr.getTargetLocation(), pr.getListener());
    }

    private void orderCopy(LogicalData srcData, DataLocation targetLocation, EventListener listener) {
        if (DEBUG) {
            LOGGER.debug("Ordering Checkpoint copy " + srcData + " to " + targetLocation);
        }
        Comm.getAppHost().getData(srcData, targetLocation, srcData, new FileTransferable(true), listener);
    }

}
