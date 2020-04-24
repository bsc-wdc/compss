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
package es.bsc.compss.executor.utils;

import es.bsc.compss.binders.BindToMap;
import es.bsc.compss.binders.BindToResource;
import es.bsc.compss.binders.Unbinded;
import es.bsc.compss.executor.types.InvocationResources;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.types.execution.exceptions.InvalidMapException;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableComputingUnitsException;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ResourceManager {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXEC_MANAGER);

    private final ThreadBinder binderCPUs;
    private final ThreadBinder binderGPUs;
    private final ThreadBinder binderFPGAs;

    /**
     * Resource Manager constructor.
     * 
     * @param cusCPU CPU Computing units
     * @param cpuMap CPU Mapping
     * @param cusGPU GPU Computing units
     * @param gpuMap GPU Mapping
     * @param cusFPGA FPGA Computing units
     * @param fpgaMap FPGA Mapping
     * @throws InvalidMapException Incorrect mapping specification
     */
    public ResourceManager(int cusCPU, String cpuMap, int cusGPU, String gpuMap, int cusFPGA, String fpgaMap) 
    		throws InvalidMapException {
        // Instantiate CPU binders
        LOGGER.debug("Instantiate CPU Binder with " + cusCPU + " CUs");

        ThreadBinder binderCPUsTmp;
        try {
            switch (cpuMap) {
                case ThreadBinder.BINDER_DISABLED:
                    binderCPUsTmp = new Unbinded();
                    break;
                case ThreadBinder.BINDER_AUTOMATIC:
                    String resourceMap = BindToMap.getResourceCpuDescription();
                    binderCPUsTmp = new BindToMap(cusCPU, resourceMap);
                    break;
                default:
                    // Custom user map
                    binderCPUsTmp = new BindToMap(cusCPU, cpuMap);
                    break;
            }
        } catch (Exception e) {
            LOGGER
                .warn("Could not load the desidered mapping policy for the CPU computing units. Using default policy ("
                    + ThreadBinder.BINDER_AUTOMATIC + ")");
            String resourceMap = BindToMap.getResourceCpuDescription();
            binderCPUsTmp = new BindToMap(cusCPU, resourceMap);
        }
        binderCPUs = binderCPUsTmp;

        // Instantiate GPU Binders
        ThreadBinder binderGPUsTmp;
        LOGGER.debug("Instantiate GPU Binder with " + cusGPU + " CUs");
        try {
            switch (gpuMap) {
                case ThreadBinder.BINDER_DISABLED:
                    binderGPUsTmp = new Unbinded();
                    break;
                case ThreadBinder.BINDER_AUTOMATIC:
                    binderGPUsTmp = new BindToResource(cusGPU);
                    break;
                default:
                    // Custom user map
                    binderGPUsTmp = new BindToMap(cusGPU, gpuMap);
                    break;
            }
        } catch (Exception e) {
            LOGGER
                .warn("Could not load the desidered mapping policy for the GPU computing units. Using default policy ("
                    + ThreadBinder.BINDER_AUTOMATIC + ")");
            binderGPUsTmp = new BindToResource(cusGPU);
        }
        binderGPUs = binderGPUsTmp;

        // Instantiate FPGA Binders
        ThreadBinder binderFPGAsTmp;
        LOGGER.debug("Instantiate FPGA Binder with " + cusFPGA + " CUs");
        try {
            switch (fpgaMap) {
                case ThreadBinder.BINDER_DISABLED:
                    binderFPGAsTmp = new Unbinded();
                    break;
                case ThreadBinder.BINDER_AUTOMATIC:
                    binderFPGAsTmp = new BindToResource(cusFPGA);
                    break;
                default:
                    // Custom user map
                    binderFPGAsTmp = new BindToMap(cusFPGA, fpgaMap);
                    break;
            }
        } catch (Exception e) {
            LOGGER
                .warn("Could not load the desidered mapping policy for the FPGA computing units. Using default policy ("
                    + ThreadBinder.BINDER_AUTOMATIC + ")");
            binderFPGAsTmp = new BindToResource(cusFPGA);
        }
        binderFPGAs = binderFPGAsTmp;
    }

    /**
     * Bind numCUs core units to the job.
     *
     * @param jobId Job identifier
     * @param rd Resource descripton
     * @return Assigned resources
     * @throws UnsufficientAvailableComputingUnitsException Not enough available computing units
     */
    public InvocationResources acquireResources(int jobId, ResourceDescription rd)
        throws UnsufficientAvailableComputingUnitsException {
        int cpus;
        int gpus;
        int fpgas;
        int ios = 0;
        MethodResourceDescription mrd;
        try {
            mrd = (MethodResourceDescription) rd;
            cpus = mrd.getTotalCPUComputingUnits();
            gpus = mrd.getTotalGPUComputingUnits();
            fpgas = mrd.getTotalFPGAComputingUnits();
        } catch (ClassCastException e) {
            cpus = 0;
            gpus = 0;
            fpgas = 0;
        }
        int[] assignedCPUs = this.binderCPUs.bindComputingUnits(jobId, cpus);
        int[] assignedGPUs = this.binderGPUs.bindComputingUnits(jobId, gpus);
        int[] assignedFPGAs = this.binderFPGAs.bindComputingUnits(jobId, fpgas);

        return new InvocationResources(assignedCPUs, assignedGPUs, assignedFPGAs);
    }

    /**
     * Release core units occupied by the job.
     *
     * @param jobId Job identifier
     */
    public void releaseResources(int jobId) {
        this.binderCPUs.releaseComputingUnits(jobId);
        this.binderGPUs.releaseComputingUnits(jobId);
        this.binderFPGAs.releaseComputingUnits(jobId);
    }

}
