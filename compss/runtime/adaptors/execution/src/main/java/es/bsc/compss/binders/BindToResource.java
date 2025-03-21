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

package es.bsc.compss.binders;

import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableComputingUnitsException;


/**
 * Class to bind the threads to the resource (which is supposed to have as many cores as the given computing Units of
 * the resource).
 */
public class BindToResource implements ThreadBinder {

    private static final String UNSUFFICIENT_CUS = "Not enough available computing units for task execution";

    private final int[] bindedComputingUnits;


    /**
     * Creates a new thread binder for unaware binds.
     *
     * @param numThreads number of managed threads
     */
    public BindToResource(int numThreads) {
        this.bindedComputingUnits = new int[numThreads];
        for (int i = 0; i < numThreads; i++) {
            this.bindedComputingUnits[i] = -1;
        }
    }

    @Override
    public int[] bindComputingUnits(int jobId, int numCUs, int[] preferredAllocation)
        throws UnsufficientAvailableComputingUnitsException {
        if (preferredAllocation != null && preferredAllocation.length == numCUs) {
            synchronized (this.bindedComputingUnits) {
                if (isAllocationAvailable(preferredAllocation)) {
                    assignAllocation(preferredAllocation, jobId);
                    return preferredAllocation;
                }
            }
        }
        int[] assignedCoreUnits = new int[numCUs];
        int numAssignedCores = 0;
        // Assign free CUs to the job
        if (numCUs > 0) {
            synchronized (this.bindedComputingUnits) {
                for (int coreId = 0; coreId < this.bindedComputingUnits.length; ++coreId) {
                    if (this.bindedComputingUnits[coreId] == -1) {
                        this.bindedComputingUnits[coreId] = jobId;
                        assignedCoreUnits[numAssignedCores] = coreId;
                        numAssignedCores++;
                    }
                    if (numAssignedCores == numCUs) {
                        break;
                    }
                }
            }
            // If the job doesn't have all the CUs it needs, it cannot run on occupied ones
            // Raise exception
            if (numAssignedCores != numCUs) {
                releaseComputingUnits(jobId);
                throw new UnsufficientAvailableComputingUnitsException(UNSUFFICIENT_CUS);
            }
        }
        return assignedCoreUnits;
    }

    private void assignAllocation(int[] previousAllocation, int jobId) {
        for (int coreId : previousAllocation) {
            this.bindedComputingUnits[coreId] = jobId;
        }
    }

    private boolean isAllocationAvailable(int[] previousAllocation) {
        for (int coreId : previousAllocation) {
            if (this.bindedComputingUnits[coreId] != -1) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void releaseComputingUnits(int jobId) {
        synchronized (bindedComputingUnits) {
            for (int coreId = 0; coreId < bindedComputingUnits.length; coreId++) {
                if (bindedComputingUnits[coreId] == jobId) {
                    bindedComputingUnits[coreId] = -1;
                }
            }
        }
    }

}
