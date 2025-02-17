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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.types.execution.exceptions.InvalidMapException;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableComputingUnitsException;
import es.bsc.compss.util.Tracer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class to bind the threads to an specific resource map (obtained by lscpu or given by the user).
 */
public class BindToMap implements ThreadBinder {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_BINDER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private ArrayList<ArrayList<Integer>> idList = new ArrayList<>();
    private int[] bindedComputingUnits;


    /**
     * Constructor for thread binder. The format is the one followed by lscpu ("," to separate groups, "-" to separate
     * bounds of groups). In addition, "/" is used to separate sockets For example: "1,2,3,6-8/1,3-5" =
     * "1-3,6,7,8/1,3,4,5"
     *
     * @param numThreads number of threads to be managed by the Binders
     * @param socketString Description of the available socket
     */
    public BindToMap(int numThreads, String socketString) {
        ArrayList<ArrayList<Integer>> computingUnitsIds = new ArrayList<>();
        int realAmountThreads = 0;
        int maxThreadNum = 0;
        String[] slots = socketString.split("/");
        for (String availableCpus : slots) {
            String[] intervals = availableCpus.split(",");
            ArrayList<Integer> currentIds = new ArrayList<>();
            for (String currentInterval : intervals) {
                String[] bounds = currentInterval.split("-");
                int lowerBound = Integer.parseInt(bounds[0]);
                int upperBound;
                if (bounds.length == 2) {
                    upperBound = Integer.parseInt(bounds[1]);
                } else {
                    upperBound = lowerBound;
                }
                if (upperBound > maxThreadNum) {
                    maxThreadNum = upperBound;
                }
                realAmountThreads += (upperBound - lowerBound + 1);
                for (int i = 0; i < (upperBound - lowerBound + 1); i++) {
                    currentIds.add(lowerBound + i);
                }
            }
            computingUnitsIds.add(currentIds);
        }
        auxiliarConstructor(numThreads, computingUnitsIds, realAmountThreads, maxThreadNum);
    }

    /**
     * Retrieves the nodes description by running the lscpu command and processes it to provide a resourde description
     * summary.
     *
     * @return summary description of the cpu resources of the node
     * @throws InvalidMapException Cannot retrieve the CPU description using the lscpu command
     */
    public static String getResourceCpuDescription() throws InvalidMapException {
        // Get LSCPU output
        String cmdOutput = getLsCpuOutput();

        // Process the LSCPU output
        return processLsCpuOutput(cmdOutput);
    }

    private static String getLsCpuOutput() throws InvalidMapException {
        // **************************************************************************************
        // Get LSCPU output
        // **************************************************************************************
        String cmdOutput = null;
        ProcessBuilder pb = new ProcessBuilder("lscpu");
        try {
            // Setup process environment -- Tracing entries
            Tracer.prepareEnvironment(pb.environment(), false);

            Process process = pb.start();

            // Disable inputs to process
            process.getOutputStream().close();

            // Wait and retrieve exit value
            int exitValue = process.waitFor();
            if (exitValue != 0) {
                throw new InvalidMapException("ERROR: LSCPU command failed with exitValue " + exitValue);
            }

            // Retrieve command output
            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line = null;
                while ((line = br.readLine()) != null) {
                    sb.append(line).append("\n");
                }
            } catch (IOException ioe) {
                throw new InvalidMapException("ERROR: Cannot retrieve LSCPU command output", ioe);
            }
            cmdOutput = sb.toString();
        } catch (IOException ioe) {
            throw new InvalidMapException("ERROR: Cannot start LSCPU ProcessBuilder", ioe);
        } catch (InterruptedException ie) {
            throw new InvalidMapException("ERROR: LSCPU command interrupted", ie);
        }

        return cmdOutput;
    }

    /**
     * Parses the output of the lscpu command and returns a summary description of the node sockets.
     *
     * @param cmdOutput lscpu-like description of the worker
     * @return summary description of the cpu resources of the node
     */
    public static String processLsCpuOutput(String cmdOutput) {
        if (DEBUG) {
            LOGGER.debug("Parsing LSCPU Output : " + cmdOutput);
        }
        String[] cmdLines = cmdOutput.split("\n");
        Integer numSockets = null;
        Integer coresPerSocket = null;
        Integer threadsPerCore = null;
        Integer numCpus = null;
        List<String> numaDescription = new ArrayList<>();
        for (String line : cmdLines) {
            if (line.contains("Socket(s):")) {
                String[] lineValues = line.split(" ");
                String numSocketsString = lineValues[lineValues.length - 1];
                if (numSocketsString != null && !numSocketsString.isEmpty()) {
                    numSockets = Integer.parseInt(numSocketsString);
                }
            } else {
                if (line.contains("Core(s) per socket:")) {
                    String[] lineValues = line.split(" ");
                    String coresPerSocketString = lineValues[lineValues.length - 1];
                    if (coresPerSocketString != null && !coresPerSocketString.isEmpty()) {
                        coresPerSocket = Integer.parseInt(coresPerSocketString);
                    }
                } else {
                    if (line.contains("Thread(s) per core:")) {
                        String[] lineValues = line.split(" ");
                        String threadsPerCoreString = lineValues[lineValues.length - 1];
                        if (threadsPerCoreString != null && !threadsPerCoreString.isEmpty()) {
                            threadsPerCore = Integer.parseInt(threadsPerCoreString);
                        }
                    } else {
                        if (line.contains("NUMA node") && line.contains("CPU(s):")) {
                            while (line.contains("  ")) {
                                line = line.replaceAll("  ", " ");
                            }
                            String[] lineValues = line.split(" ");
                            if (lineValues.length == 4) {
                                String coresDescription = lineValues[lineValues.length - 1];
                                numaDescription.add(coresDescription);
                            }
                        } else {
                            if (line.contains("CPU(s):")) {
                                String[] lineValues = line.split(" ");
                                String numCpusString = lineValues[lineValues.length - 1];
                                if (numCpusString != null && !numCpusString.isEmpty()) {
                                    numCpus = Integer.parseInt(numCpusString);
                                }
                            }
                        }
                    }
                }
            }
        }

        String cpuMap;
        if (numSockets == null || numSockets <= 0) {
            // Do general affinity: per core
            LOGGER.debug("CPU Map constructed by General Affinity");
            cpuMap = "0-" + String.valueOf(numCpus);
        } else {
            if (!numaDescription.isEmpty()) {
                // Do affinity per NUMA Node
                LOGGER.debug("CPU Map constructed by Affinity per Numa Node");
                StringBuilder sb = new StringBuilder();
                boolean first = true;
                for (String descr : numaDescription) {
                    if (!first) {
                        sb.append("/");
                    } else {
                        first = false;
                    }
                    sb.append(descr);
                }
                cpuMap = sb.toString();
            } else {
                // Do affinity per socket
                LOGGER.debug("CPU Map constructed by Affinity per socket");
                StringBuilder sb = new StringBuilder();
                int cpusPerSocket = coresPerSocket * threadsPerCore;
                for (int i = 0; i < numSockets; ++i) {
                    int low = i * cpusPerSocket;
                    int high = (i + 1) * cpusPerSocket - 1;
                    if (i != 0) {
                        sb.append("/");
                    }
                    sb.append(String.valueOf(low)).append("-").append(String.valueOf(high));
                }
                cpuMap = sb.toString();
            }
        }

        LOGGER.info("CPU Map: " + cpuMap);
        return cpuMap;
    }

    @Override
    public int[] bindComputingUnits(int jobId, int numCUs, int[] preferredAllocation)
        throws UnsufficientAvailableComputingUnitsException {
        int[] assignedCoreUnits = new int[numCUs];
        ArrayList<Integer> usedSockets;

        // Assign free CUs to the job

        synchronized (this) {
            if (preferredAllocation != null && preferredAllocation.length == numCUs) {
                if (isAllocationAvailable(preferredAllocation)) {
                    assignAllocation(preferredAllocation, jobId);
                    updateSocketPriority();
                    return preferredAllocation;
                }
            }
            usedSockets = recursiveBindingComputingUnits(jobId, numCUs, 0);

            // If the job doesn't have all the CUs it needs, it cannot run on occupied ones
            // Raise exception
            if (usedSockets == null) {
                throw new UnsufficientAvailableComputingUnitsException(
                    "Not enough available computing units for task execution " + jobId);
            }

            // Handle assignedCoreUnits
            int numAssignedCores = 0;
            for (int socket : usedSockets) {
                ArrayList<Integer> currentSocketThreads = this.idList.get(socket);
                for (int i = 0; i < currentSocketThreads.size() && (numAssignedCores < numCUs); ++i) {
                    int coreNum = currentSocketThreads.get(i);
                    if (bindedComputingUnits[coreNum] == -1) {
                        bindedComputingUnits[coreNum] = jobId;
                        assignedCoreUnits[numAssignedCores] = coreNum;
                        ++numAssignedCores;
                        // this.availableSlots.set(socket, this.availableSlots.get(socket) - 1);
                    }
                }
                if (numAssignedCores == numCUs) {
                    break;
                }
            }
            updateSocketPriority();
        }

        if (DEBUG) {
            StringBuilder sb = new StringBuilder("[ThreadBinderCPUs] Task " + jobId + " binded to cores ");
            if (assignedCoreUnits.length != 0) {
                sb.append(assignedCoreUnits[0]);
            }
            for (int i = 1; i < assignedCoreUnits.length; ++i) {
                sb.append(" ").append(assignedCoreUnits[i]);
            }
            LOGGER.debug(sb.toString());
        }
        return assignedCoreUnits;
    }

    @Override
    public void releaseComputingUnits(int jobId) {
        synchronized (this) {
            for (int i = 0; i < this.bindedComputingUnits.length; ++i) {
                if (bindedComputingUnits[i] == jobId) {
                    bindedComputingUnits[i] = -1;
                }
            }
            updateSocketPriority();
        }
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

    private void auxiliarConstructor(int numThreads, ArrayList<ArrayList<Integer>> computingUnitsIds,
        int totalAmountThreads, int maxThreadNum) {
        // Initialize binds ArrayList
        this.bindedComputingUnits = new int[maxThreadNum + 1];
        for (int i = 0; i <= maxThreadNum; i++) {
            this.bindedComputingUnits[i] = -1;
        }

        this.idList = computingUnitsIds;

        // Replicate socket structure
        for (int i = 0; i < this.idList.get(0).size() && (totalAmountThreads < numThreads); i++) {
            for (int j = 0; j < this.idList.size() && (totalAmountThreads < numThreads); j++) {
                ArrayList<Integer> currentSocketIds = this.idList.get(j);
                currentSocketIds.add(this.idList.get(j).get(i));
                this.idList.set(j, currentSocketIds);
                totalAmountThreads++;
            }
        }
        if (DEBUG) {
            for (int i = 0; i < this.idList.size(); ++i) {
                LOGGER.debug("[ThreadBinderCPUs] Socket " + i);
                StringBuilder sb = new StringBuilder("[ThreadBinderCPUs] Registered slots: ");
                StringBuilder sb2 = new StringBuilder("[ThreadBinderCPUs] Registered ids: ");
                for (int j = 0; j < this.idList.get(i).size(); ++j) {
                    int coreNum = this.idList.get(i).get(j);
                    sb.append(this.bindedComputingUnits[coreNum]).append(" ");
                    sb2.append(coreNum).append(" ");
                }
                LOGGER.debug(sb.toString());
                LOGGER.debug(sb2.toString());
            }
        }
    }

    private int getAvailableSlots(ArrayList<Integer> socket) {
        int counter = 0;
        for (int i : socket) {
            if (bindedComputingUnits[i] == -1) {
                ++counter;
            }
        }
        return counter;
    }

    /**
     * Recursive method for binding computing units.
     * 
     * @param jobId : job associated to the threads to locate
     * @param amount : amount of threads that needs to be allocated
     * @param index : lower socket allowed to hosts the threads
     * @return : True if succeeded, False if failed
     */
    private ArrayList<Integer> recursiveBindingComputingUnits(int jobId, int amount, int index) {
        // With the current index, we can fulfill the thread requirements
        int availableSlots = getAvailableSlots(this.idList.get(index));
        if (availableSlots >= amount) {
            ArrayList<Integer> socketUsed = new ArrayList<>();
            socketUsed.add(index);
            return socketUsed;
        }
        // More sockets are needed
        if (availableSlots > 0) {
            int newAmountThreads = amount - availableSlots;
            ArrayList<Integer> nextSocketsUsed = recursiveBindingComputingUnits(jobId, newAmountThreads, index + 1);
            if (nextSocketsUsed != null) {
                nextSocketsUsed.add(index);
                return nextSocketsUsed;
            }
        }
        return null;
    }

    // Update structures to ensure that the ones with more slots are stored first in the ArrayList
    private void updateSocketPriority() {
        ArrayList<Integer> sockets = new ArrayList<>();
        ArrayList<Integer> availableSlots = new ArrayList<>();

        for (int i = 0; i < this.idList.size(); ++i) {
            sockets.add(i);
            availableSlots.add(getAvailableSlots(this.idList.get(i)));
        }

        Comparator<Integer> customComparator = new Comparator<Integer>() {

            @Override
            public int compare(Integer first, Integer second) {
                return Integer.compare(availableSlots.get(second), availableSlots.get(first));
            }
        };
        Collections.sort(sockets, customComparator);
        ArrayList<ArrayList<Integer>> newComputingUnitsIds = new ArrayList<>();
        for (int i = 0; i < this.idList.size(); ++i) {
            int currentSocketIndex = sockets.get(i);
            newComputingUnitsIds.add(this.idList.get(currentSocketIndex));

        }
        this.idList = newComputingUnitsIds;
    }

}
