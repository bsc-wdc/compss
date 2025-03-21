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
package es.bsc.compss.types.execution;

import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableComputingUnitsException;


public interface ThreadBinder {

    public static final String BINDER_DISABLED = "disabled";
    public static final String BINDER_AUTOMATIC = "automatic";
    public static final String BINDER_DLB = "dlb";


    /**
     * Binds @numCUs computing units of a @jobId. Tries to assign in the previous allocation.
     *
     * @param jobId job identifier requesting the assignment
     * @param numCUs number of computing units requested to be bound.
     * @param preferredAllocation preferred assignment of computing units
     * @return Assignment of computing cores to the job
     * @throws UnsufficientAvailableComputingUnitsException Error no enough computing available for the request
     */
    public int[] bindComputingUnits(int jobId, int numCUs, int[] preferredAllocation)
        throws UnsufficientAvailableComputingUnitsException;

    /**
     * Releases the computing units previously requested by the job @jobId.
     *
     * @param jobId Job Identifier
     */
    public void releaseComputingUnits(int jobId);

}
