package integratedtoolkit.nio.worker.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.log.Loggers;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableComputingUnitsException;


public class ThreadBinderCPU implements ThreadBinder {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_BINDER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private ArrayList<ArrayList<Integer>> computingUnitsIds = new ArrayList<>();
    private ArrayList<ArrayList<Integer>> bindedComputingUnits = new ArrayList<>();

    /**
     * Constructor for thread binder
     * 
     * It is important to realize that NUMA node information is expected to be of type lowerBound-upperBound. In case
     * the information is given in a different format (for example, lowerBound1-upperBound1, lowerBound2-upperBound2)
     * the second interval won't be taken in account In case several lines references different NUMANodes (a single node
     * has several CPU intervals), the system will consider each line as a different NUMANode
     * 
     * @param numThreads
     *            amount of tasks to be launched in a given node
     * @throws InitializationException
     */
    public ThreadBinderCPU(int numThreads) throws InitializationException {
        ArrayList<ArrayList<Integer>> computingUnitsIds = new ArrayList<>();
        int realAmountThreads = 0;
        try {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec("lscpu");
            BufferedReader stdOutput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String s;
            while ((s = stdOutput.readLine()) != null) {
                if (s.contains("NUMA") && !s.contains("NUMA node(s)")) {
                    String availableCPUs = s.replaceAll("\\s+", "").split(":")[1];
                    String[] intervals = availableCPUs.split(",");
                    ArrayList<Integer> currentIds = new ArrayList<>();
                    for (String currentInterval : intervals) {
                        String[] bounds = currentInterval.split("-");
                        int lowerBound = Integer.parseInt(bounds[0]);
                        int upperBound;
                        if (bounds.length == 2) {
                            upperBound = Integer.parseInt(bounds[1]);
                        }
                        else {
                            upperBound = lowerBound;
                        }
                        realAmountThreads += (upperBound - lowerBound + 1);
                        for (int i = 0; i < (upperBound - lowerBound + 1); i++) {
                            currentIds.add(lowerBound + i);
                        }
                        computingUnitsIds.add(currentIds);  
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.debug("[ThreadBinderCPU] Unable to obtain the total amount of sockets");
            throw new InitializationException("Unable to obtain the total amount of sockets");
        }
        auxiliarConstructor(numThreads, computingUnitsIds, realAmountThreads);
    }
    
    public ThreadBinderCPU(int numThreads, ArrayList<ArrayList<Integer>> computingUnitsIds){
        int realAmountThreads = 0;
        for(ArrayList<Integer> currentBinds : this.computingUnitsIds) {
            realAmountThreads += currentBinds.size();
        }
        auxiliarConstructor(numThreads, computingUnitsIds, realAmountThreads);
    }

    /**
     * Bind numCUs core units to the job
     * 
     * @param jobId
     * @param numCUs
     * @return
     * @throws UnsufficientAvailableComputingUnitsException
     */
    public int[] bindComputingUnits(int jobId, int numCUs) throws UnsufficientAvailableComputingUnitsException {
        int assignedCoreUnits[] = new int[numCUs];
        ArrayList<Integer> usedSockets = null;

        // Assign free CUs to the job
        synchronized (this.bindedComputingUnits) {
            /*
             * if (debug) { StringBuilder sb = new StringBuilder("[ThreadBinderCPUs] Available slots for task " + jobId
             * + ": "); sb.append(this.availableSlots.get(0)); for (int i = 1; i < this.availableSlots.size(); ++i){
             * sb.append(" " + this.availableSlots.get(i)); } logger.debug(sb.toString()); }
             */
            usedSockets = recursiveBindingComputingUnits(jobId, numCUs, 0);

            // If the job doesn't have all the CUs it needs, it cannot run on occupied ones
            // Raise exception
            if (usedSockets == null) {
                throw new UnsufficientAvailableComputingUnitsException("Not enough available computing units for task execution");
            }

            // Handle assignedCoreUnits
            int numAssignedCores = 0;
            for (int socket : usedSockets) {
                ArrayList<Integer> currentSocketThreads = this.bindedComputingUnits.get(socket);
                for (int i = 0; i < currentSocketThreads.size() && (numAssignedCores < numCUs); ++i) {
                    if (currentSocketThreads.get(i) == -1) {
                        currentSocketThreads.set(i, jobId);
                        assignedCoreUnits[numAssignedCores] = this.computingUnitsIds.get(socket).get(i);
                        ++numAssignedCores;
                        //this.availableSlots.set(socket, this.availableSlots.get(socket) - 1);
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

    /**
     * Release computing units occupied by the job
     * 
     * @param jobId
     */
    public void releaseComputingUnits(int jobId) {
        synchronized (this.bindedComputingUnits) {
            for (int i = 0; i < this.bindedComputingUnits.size(); ++i) {
                ArrayList<Integer> vector = this.bindedComputingUnits.get(i);
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
        this.computingUnitsIds = computingUnitsIds;
        for (ArrayList<Integer> currentSocket : this.computingUnitsIds) {
            ArrayList<Integer> currentBounds = new ArrayList<>();
            for(int i = 0; i < currentSocket.size(); ++i){
                currentBounds.add(-1);
            }
            this.bindedComputingUnits.add(currentBounds);
        }
        // Replicate socket structure
        for (int i = 0; i < this.computingUnitsIds.get(0).size() && (totalAmountThreads < numThreads); ++i) {
            for (int j = 0; j < this.computingUnitsIds.size() && (totalAmountThreads < numThreads); ++j) {
                ArrayList<Integer> currentSocketIds = this.computingUnitsIds.get(j);
                currentSocketIds.add(this.computingUnitsIds.get(j).get(i));
                this.computingUnitsIds.set(j, currentSocketIds);
                ArrayList<Integer> currentBinds = this.bindedComputingUnits.get(j);
                currentBinds.add(-1);
                this.bindedComputingUnits.set(j, currentBinds);
                ++totalAmountThreads;
            }
        }
        if (DEBUG) {
            for (int i = 0; i < this.computingUnitsIds.size(); ++i) {
                LOGGER.debug("[ThreadBinderCPUs] Socket " + i);
                StringBuilder sb = new StringBuilder("[ThreadBinderCPUs] Registered slots: ");
                StringBuilder sb2 = new StringBuilder("[ThreadBinderCPUs] Registered ids: ");
                for (int j = 0; j < this.computingUnitsIds.get(i).size(); ++j) {
                    sb.append(this.bindedComputingUnits.get(i).get(j) + " ");
                    sb2.append(this.computingUnitsIds.get(i).get(j) + " ");
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
     * @param sockets
     *            : maximum different amount of sockets allowed to host the threads
     * @return : True if succeeded, False if failed
     */
    private ArrayList<Integer> recursiveBindingComputingUnits(int jobId, int amount, int index) {
        // With the current index, we can fulfill the thread requirements
        int availableSlots = getAvailableSlots(this.bindedComputingUnits.get(index));
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
        for (int i = 0; i < this.bindedComputingUnits.size(); ++i) {
            sockets.add(i);
            availableSlots.add(getAvailableSlots(this.bindedComputingUnits.get(i)));
        }

        Comparator<Integer> customComparator = new Comparator<Integer>() {

            public int compare(Integer first, Integer second) {
                return Integer.compare(availableSlots.get(second), availableSlots.get(first));
            }
        };
        Collections.sort(sockets, customComparator);
        ArrayList<ArrayList<Integer>> newComputingUnitsIds = new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> newBindedComputingUnits = new ArrayList<ArrayList<Integer>>();
        for (int i = 0; i < this.bindedComputingUnits.size(); ++i) {
            int currentSocketIndex = sockets.get(i);
            newComputingUnitsIds.add(this.computingUnitsIds.get(currentSocketIndex));
            newBindedComputingUnits.add(this.bindedComputingUnits.get(currentSocketIndex));
        }
        this.computingUnitsIds = newComputingUnitsIds;
        this.bindedComputingUnits = newBindedComputingUnits;
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
