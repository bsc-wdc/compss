package integratedtoolkit.nio.worker.binders;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import integratedtoolkit.log.Loggers;

import integratedtoolkit.nio.worker.exceptions.InvalidMapException;
import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableComputingUnitsException;


/**
 * Class to bind the threads to an specific resource map (obtained by lscpu or given by the user)
 *
 */
public class BindToMap implements ThreadBinder {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_BINDER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private ArrayList<ArrayList<Integer>> idList = new ArrayList<>();
    private ArrayList<ArrayList<Integer>> bindedCPUs = new ArrayList<>();


    /**
     * Constructor for thread binder
     * 
     * The format is the one followed by lscpu ("," to separate groups, "-" to separate bounds of groups) In addition,
     * "/" is used to separate sockets For example: "1,2,3,6-8/1,3-5" = "1-3,6,7,8/1,3,4,5"
     * 
     * @param numThreads
     * @param socketString
     */
    public BindToMap(int numThreads, String socketString) {
        ArrayList<ArrayList<Integer>> computingUnitsIds = new ArrayList<>();
        int realAmountThreads = 0;

        String[] slots = socketString.split("/");
        for (String availableCPUs : slots) {
            String[] intervals = availableCPUs.split(",");
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
                realAmountThreads += (upperBound - lowerBound + 1);
                for (int i = 0; i < (upperBound - lowerBound + 1); i++) {
                    currentIds.add(lowerBound + i);
                }
            }
            computingUnitsIds.add(currentIds);
        }
        auxiliarConstructor(numThreads, computingUnitsIds, realAmountThreads);
    }

    public static String getResourceCPUDescription() throws InvalidMapException {
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

    public static String processLsCpuOutput(String cmdOutput) throws InvalidMapException {
        LOGGER.debug("Parsing LSCPU Output : " + cmdOutput);
        String[] cmdLines = cmdOutput.split("\n");
        Integer numSockets = null;
        Integer coresPerSocket = null;
        Integer threadsPerCore = null;
        Integer numCPUs = null;
        for (String line : cmdLines) {
            if (line.contains("Socket(s):")) {
                String[] lineValues = line.split(" ");
                String numSocketsSTR = lineValues[lineValues.length - 1];
                if (numSocketsSTR != null && !numSocketsSTR.isEmpty()) {
                    numSockets = Integer.parseInt(numSocketsSTR);
                }
            } else if (line.contains("Core(s) per socket:")) {
                String[] lineValues = line.split(" ");
                String coresPerSocketSTR = lineValues[lineValues.length - 1];
                if (coresPerSocketSTR != null && !coresPerSocketSTR.isEmpty()) {
                    coresPerSocket = Integer.parseInt(coresPerSocketSTR);
                }
            } else if (line.contains("Thread(s) per core:")) {
                String[] lineValues = line.split(" ");
                String threadsPerCoreSTR = lineValues[lineValues.length - 1];
                if (threadsPerCoreSTR != null && !threadsPerCoreSTR.isEmpty()) {
                    threadsPerCore = Integer.parseInt(threadsPerCoreSTR);
                }
            } else if (line.contains("CPU(s):") && !line.contains("NUMA")) {
                String[] lineValues = line.split(" ");
                String numCPUsSTR = lineValues[lineValues.length - 1];
                if (numCPUsSTR != null && !numCPUsSTR.isEmpty()) {
                    numCPUs = Integer.parseInt(numCPUsSTR);
                }
            }
        }

        String cpuMap;
        if (numSockets == null || numSockets <= 0) {
            // Do general affinity: per core
            LOGGER.debug("CPU Map constructed by General Affinity");
            cpuMap = "0-" + String.valueOf(numCPUs);
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

        LOGGER.info("CPU MAP: " + cpuMap);
        return cpuMap;
    }

    @Override
    public int[] bindComputingUnits(int jobId, int numCUs) throws UnsufficientAvailableComputingUnitsException {
        int assignedCoreUnits[] = new int[numCUs];
        ArrayList<Integer> usedSockets = null;

        // Assign free CUs to the job
        synchronized (this.bindedCPUs) {

            usedSockets = recursiveBindingComputingUnits(jobId, numCUs, 0);

            // If the job doesn't have all the CUs it needs, it cannot run on occupied ones
            // Raise exception
            if (usedSockets == null) {
                throw new UnsufficientAvailableComputingUnitsException("Not enough available computing units for task execution");
            }

            // Handle assignedCoreUnits
            int numAssignedCores = 0;
            for (int socket : usedSockets) {
                ArrayList<Integer> currentSocketThreads = this.bindedCPUs.get(socket);
                for (int i = 0; i < currentSocketThreads.size() && (numAssignedCores < numCUs); ++i) {
                    if (currentSocketThreads.get(i) == -1) {
                        currentSocketThreads.set(i, jobId);
                        assignedCoreUnits[numAssignedCores] = this.idList.get(socket).get(i);
                        ++numAssignedCores;
                        // this.availableSlots.set(socket, this.availableSlots.get(socket) - 1);
                    }
                }
                if (numAssignedCores == numCUs) {
                    break;
                }
            }
            handleSlotsAdded();
        }

        if (DEBUG) {
            StringBuilder sb = new StringBuilder("[ThreadBinderCPUs] Task " + jobId + " binded to cores ");
            sb.append(assignedCoreUnits[0]);
            for (int i = 1; i < assignedCoreUnits.length; ++i) {
                sb.append(" " + assignedCoreUnits[i]);
            }
            LOGGER.debug(sb.toString());
        }
        return assignedCoreUnits;
    }

    @Override
    public void releaseComputingUnits(int jobId) {
        synchronized (this.bindedCPUs) {
            for (int i = 0; i < this.bindedCPUs.size(); ++i) {
                ArrayList<Integer> vector = this.bindedCPUs.get(i);
                for (int j = 0; j < vector.size(); ++j) {
                    if (vector.get(j) == jobId) {
                        vector.set(j, -1);
                    }
                }
            }
            handleSlotsFreeded();
        }
    }

    private void auxiliarConstructor(int numThreads, ArrayList<ArrayList<Integer>> computingUnitsIds, int totalAmountThreads) {
        this.idList = computingUnitsIds;
        // Initialize de binds ArrayList
        for (ArrayList<Integer> currentSocket : this.idList) {
            ArrayList<Integer> currentBounds = new ArrayList<>();
            for (int i = 0; i < currentSocket.size(); ++i) {
                currentBounds.add(-1);
            }
            this.bindedCPUs.add(currentBounds);
        }
        // Replicate socket structure
        for (int i = 0; i < this.idList.get(0).size() && (totalAmountThreads < numThreads); ++i) {
            for (int j = 0; j < this.idList.size() && (totalAmountThreads < numThreads); ++j) {
                ArrayList<Integer> currentSocketIds = this.idList.get(j);
                currentSocketIds.add(this.idList.get(j).get(i));
                this.idList.set(j, currentSocketIds);
                ArrayList<Integer> currentBinds = this.bindedCPUs.get(j);
                currentBinds.add(-1);
                this.bindedCPUs.set(j, currentBinds);
                ++totalAmountThreads;
            }
        }
        if (DEBUG) {
            for (int i = 0; i < this.idList.size(); ++i) {
                LOGGER.debug("[ThreadBinderCPUs] Socket " + i);
                StringBuilder sb = new StringBuilder("[ThreadBinderCPUs] Registered slots: ");
                StringBuilder sb2 = new StringBuilder("[ThreadBinderCPUs] Registered ids: ");
                for (int j = 0; j < this.idList.get(i).size(); ++j) {
                    sb.append(this.bindedCPUs.get(i).get(j) + " ");
                    sb2.append(this.idList.get(i).get(j) + " ");
                }
                LOGGER.debug(sb.toString());
                LOGGER.debug(sb2.toString());
            }
        }
    }

    private int getAvailableSlots(ArrayList<Integer> socket) {
        int counter = 0;
        for (int i : socket) {
            if (i == -1) {
                ++counter;
            }
        }
        return counter;
    }

    /**
     * @param jobId
     *            : job associated to the threads to locate
     * @param amount
     *            : amount of threads that needs to be allocated
     * @param index
     *            : lower socket allowed to hosts the threads
     * @return : True if succeeded, False if failed
     */
    private ArrayList<Integer> recursiveBindingComputingUnits(int jobId, int amount, int index) {
        // With the current index, we can fulfill the thread requirements
        int availableSlots = getAvailableSlots(this.bindedCPUs.get(index));
        if (availableSlots >= amount) {
            ArrayList<Integer> socketUsed = new ArrayList<Integer>();
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
        ArrayList<Integer> sockets = new ArrayList<Integer>();
        ArrayList<Integer> availableSlots = new ArrayList<>();
        for (int i = 0; i < this.bindedCPUs.size(); ++i) {
            sockets.add(i);
            availableSlots.add(getAvailableSlots(this.bindedCPUs.get(i)));
        }

        Comparator<Integer> customComparator = new Comparator<Integer>() {

            @Override
            public int compare(Integer first, Integer second) {
                return Integer.compare(availableSlots.get(second), availableSlots.get(first));
            }
        };
        Collections.sort(sockets, customComparator);
        ArrayList<ArrayList<Integer>> newComputingUnitsIds = new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> newBindedComputingUnits = new ArrayList<ArrayList<Integer>>();
        for (int i = 0; i < this.bindedCPUs.size(); ++i) {
            int currentSocketIndex = sockets.get(i);
            newComputingUnitsIds.add(this.idList.get(currentSocketIndex));
            newBindedComputingUnits.add(this.bindedCPUs.get(currentSocketIndex));
        }
        this.idList = newComputingUnitsIds;
        this.bindedCPUs = newBindedComputingUnits;
    }

    private void handleSlotsAdded() {
        // Update data structures in order to tune the behaviour of the binder
        updateSocketPriority();
    }

    private void handleSlotsFreeded() {
        // Rearrange the thread affinity to avoid the apparition of 'holes'
        // Look up for a mechanism to obtain the PID of processes
        updateSocketPriority();
    }

}
