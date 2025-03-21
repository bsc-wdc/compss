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
 * Class for unbinded thread affinity.
 */
public class Unbinded implements ThreadBinder {

    /**
     * Creates a new thread binder without any binding.
     */
    public Unbinded() {
    }

    @Override
    public int[] bindComputingUnits(int jobId, int numCUs, int[] preferredAllocation)
        throws UnsufficientAvailableComputingUnitsException {
        return new int[] {};
    }

    @Override
    public void releaseComputingUnits(int jobId) {
    }

}
