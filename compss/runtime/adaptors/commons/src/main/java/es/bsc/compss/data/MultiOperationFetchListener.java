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

package es.bsc.compss.data;

import es.bsc.compss.data.FetchDataListener;


/**
 * This class handles the execution of several data fetching operations.
 */
public abstract class MultiOperationFetchListener implements FetchDataListener {

    private int missingOperations;
    private boolean enabled;
    private boolean error;
    private String failedDataId;
    private Exception failCause;


    /**
     * Constructs a new MultiOperationFetchListener with no errors and 0 pending operations.
     */
    public MultiOperationFetchListener() {
        this.missingOperations = 0;
        this.error = false;
        this.enabled = false;
        this.failedDataId = null;
        this.failCause = null;
    }

    /**
     * Increases the count of pending operations to complete the group.
     */
    public final synchronized void addOperation() {
        ++this.missingOperations;
    }

    /**
     * Returns the number of missing operations.
     *
     * @return number of missing operationss
     */
    public final int getMissingOperations() {
        return this.missingOperations;
    }

    /**
     * Enables the notifications for the listener.
     */
    public final synchronized void enable() {
        if (!enabled) {
            this.enabled = true;
            if (error) {
                doFailure(failedDataId, failCause);
            } else {
                if (this.missingOperations == 0) {
                    doCompleted();
                }
            }
        }
    }

    @Override
    public final synchronized void fetchedValue(String fetchedDataId) {
        --this.missingOperations;
        if (enabled && !error && missingOperations == 0) {
            doCompleted();
        }
    }

    @Override
    public final synchronized void errorFetchingValue(String failedDataId, Exception cause) {
        --this.missingOperations;
        if (!error) {
            if (enabled) {
                error = true;
                doFailure(failedDataId, cause);
            } else {
                this.failedDataId = failedDataId;
                this.failCause = cause;
            }
        }
    }

    /**
     * Action to perform when all the operations of the group have finished correctly.
     */
    public abstract void doCompleted();

    /**
     * Action to perform when one operation of the group has failed.
     *
     * @param failedDataId Id of the dataValue that couldn't be fetched.
     * @param cause cause of the failure
     */
    public abstract void doFailure(String failedDataId, Exception cause);
}
