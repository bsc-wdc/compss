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
package es.bsc.compss.execution.utils;

import es.bsc.compss.binders.BindToMap;
import es.bsc.compss.binders.BindToResource;
import es.bsc.compss.binders.Unbinded;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.types.execution.exceptions.InvalidMapException;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableComputingUnitsException;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableResourcesException;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ResourceManager {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXEC_MANAGER);

    private final List<PendingRequest> pendingRequests;
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
        this.pendingRequests = new LinkedList<>();
    }

    /**
     * Bind numCUs core units to the job.
     *
     * @param jobId Job identifier
     * @param rd Resource descripton
     * @param preferredAllocation resources expected to receive
     * @return Assigned resources
     * @throws UnsufficientAvailableResourcesException Not enough available resources
     */
    public synchronized InvocationResources acquireResources(int jobId, ResourceDescription rd,
        InvocationResources preferredAllocation) throws UnsufficientAvailableResourcesException {

        int cpus;
        int gpus;
        int fpgas;
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

        int[] preferredCPUs = null;
        int[] preferredGPUs = null;
        int[] preferredFPGAs = null;
        if (preferredAllocation != null) {
            preferredCPUs = preferredAllocation.getAssignedCPUs();
            preferredGPUs = preferredAllocation.getAssignedGPUs();
            preferredFPGAs = preferredAllocation.getAssignedFPGAs();
        }
        int[] assignedCPUs = null;
        int[] assignedGPUs = null;
        int[] assignedFPGAs = null;
        try {
            assignedCPUs = this.binderCPUs.bindComputingUnits(jobId, cpus, preferredCPUs);
            assignedGPUs = this.binderGPUs.bindComputingUnits(jobId, gpus, preferredGPUs);
            assignedFPGAs = this.binderFPGAs.bindComputingUnits(jobId, fpgas, preferredFPGAs);
        } catch (UnsufficientAvailableResourcesException uare) {
            if (assignedCPUs != null) {
                this.binderCPUs.releaseComputingUnits(jobId);
                if (assignedGPUs != null) {
                    this.binderCPUs.releaseComputingUnits(jobId);
                }
            }
            throw uare;
        }

        return new InvocationResources(assignedCPUs, assignedGPUs, assignedFPGAs);
    }

    /**
     * Bind numCUs core units to the job.
     *
     * @param jobId Job identifier
     * @param rd Resource descripton
     * @param allocatedResources resources expected to receive
     * @param sem Semaphore to notify when resources are assigned. If {@literal null} and no enough available computing
     *            units to satify the request, throws an exception.
     */
    public synchronized void reacquireResources(int jobId, ResourceDescription rd,
        InvocationResources allocatedResources, Semaphore sem) {
        try {
            InvocationResources newResources = acquireResources(jobId, rd, allocatedResources);
            allocatedResources.reconfigure(newResources);
            sem.release();
        } catch (UnsufficientAvailableResourcesException uare) {
            PendingRequest p = new PendingRequest(jobId, rd, allocatedResources, sem);
            this.pendingRequests.add(p);
        }
    }

    /**
     * Release core units occupied by the job.
     *
     * @param jobId Job identifier
     */
    public synchronized void releaseResources(int jobId) {
        this.binderCPUs.releaseComputingUnits(jobId);
        this.binderGPUs.releaseComputingUnits(jobId);
        this.binderFPGAs.releaseComputingUnits(jobId);
        Iterator<PendingRequest> iter = this.pendingRequests.iterator();
        while (iter.hasNext()) {
            PendingRequest req = iter.next();
            int reqJobId = req.jobId;
            ResourceDescription reqRequirments = req.requirements;
            InvocationResources reqAllocation = req.allocation;
            try {
                InvocationResources newResources = acquireResources(reqJobId, reqRequirments, reqAllocation);
                reqAllocation.reconfigure(newResources);
                req.sem.release();
                iter.remove();
            } catch (UnsufficientAvailableResourcesException uare) {
                // Do nothing. There are not enough resources yet. Move to next request
            }
        }
    }


    private static class PendingRequest {

        private final int jobId;
        private final ResourceDescription requirements;
        private final InvocationResources allocation;
        private final Semaphore sem;


        public PendingRequest(int jobId, ResourceDescription requirements, InvocationResources allocation,
            Semaphore sem) {
            this.jobId = jobId;
            this.requirements = requirements;
            this.allocation = allocation;
            this.sem = sem;
        }
    }
}
