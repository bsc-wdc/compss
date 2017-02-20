package integratedtoolkit.nio.worker.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import integratedtoolkit.log.Loggers;

import integratedtoolkit.nio.worker.exceptions.BadAmountSocketsException;
import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableComputingUnitsException;


public class ThreadBinderCPU implements ThreadBinder {

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
     *            amount of tasks to be launched in a given node
     * @throws InitializationException
     */
    public ThreadBinderCPU(int numThreads, int amountSockets, String socketString) throws BadAmountSocketsException {
        ArrayList<ArrayList<Integer>> computingUnitsIds = new ArrayList<>();
        int realAmountThreads = 0;

        String[] slots = socketString.split("/");
        if (amountSockets != slots.length) {
            throw new BadAmountSocketsException(amountSockets + " sockets declared but " + slots.length + " defined");
        }
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

    public ThreadBinderCPU(int numThreads, ArrayList<ArrayList<Integer>> computingUnitsIds) {
        int realAmountThreads = 0;
        for (ArrayList<Integer> currentBinds : this.idList) {
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
        synchronized (this.bindedCPUs) {
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

    /**
     * Release computing units occupied by the job
     * 
     * @param jobId
     */
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
     * @param sockets
     *            : maximum different amount of sockets allowed to host the threads
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
