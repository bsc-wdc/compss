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
package es.bsc.compss.execution.types;

public class InvocationResources {

    private int[] cpus;
    private int[] gpus;
    private int[] fpgas;


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

    /**
     * Reconfigures the InvocationResources to contain the resources passed in as a parameter.
     * 
     * @param res InvocationResources to copy
     */
    public void reconfigure(InvocationResources res) {
        this.cpus = res.cpus;
        this.gpus = res.gpus;
        this.fpgas = res.fpgas;
    }
}
