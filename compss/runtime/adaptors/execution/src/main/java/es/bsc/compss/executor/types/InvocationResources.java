/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.executor.types;

public class InvocationResources {

    private final int[] cpus;
    private final int[] gpus;
    private final int[] fpgas;

    /**
     * Invocation resources constructor.
     * 
     * @param cpus Assigned CPUs array
     * @param gpus Assigned GPUs array
     * @param fpgas Assigned FPGAs array
     */
    public InvocationResources(int[] cpus, int[] gpus, int[] fpgas) {
        this.cpus = cpus;
        this.gpus = gpus;
        this.fpgas = fpgas;
    }

    public int[] getAssignedCPUs() {
        return this.cpus;
    }

    public int[] getAssignedGPUs() {
        return this.gpus;
    }

    public int[] getAssignedFPGAs() {
        return this.fpgas;
    }
}
