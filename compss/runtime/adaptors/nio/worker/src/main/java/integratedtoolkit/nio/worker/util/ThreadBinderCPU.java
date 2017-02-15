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

    private static final Logger logger = LogManager.getLogger(Loggers.WORKER_BINDER);
    private static final boolean debug = logger.isDebugEnabled();

    private ArrayList<ArrayList<Integer>> computingUnitsIds = new ArrayList<>();
    private ArrayList<ArrayList<Integer>> bindedComputingUnits = new ArrayList<>();
    private ArrayList<Integer> availableSlots = new ArrayList<>();


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
        try {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec("lscpu");
            BufferedReader stdOutput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String s;
            int totalAmountThreads = 0;
            while ((s = stdOutput.readLine()) != null) {
                if (s.contains("NUMA") && !s.contains("NUMA node(s)")) {
                    String[] bounds = s.replaceAll("\\s+", "").split(":")[1].split("-");
                    logger.debug("[ThreadBinderCPUs] New bounds registered: " + bounds[0] + " " + bounds[1]);
                    int lowerBound = Integer.parseInt(bounds[0]);
                    int upperBound = Integer.parseInt(bounds[1]);
                    ArrayList<Integer> currentBounds = new ArrayList<>();
                    ArrayList<Integer> currentIds = new ArrayList<>();
                    totalAmountThreads += (upperBound - lowerBound + 1);
                    for (int i = 0; i < (upperBound - lowerBound + 1); i++) {
                        currentBounds.add(-1);
                        currentIds.add(lowerBound + i);
                    }
                    this.bindedComputingUnits.add(currentBounds);
                    this.computingUnitsIds.add(currentIds);
                    this.availableSlots.add(upperBound - lowerBound + 1);
                }
            }
            for (int i = 0; i < this.computingUnitsIds.get(0).size() && (totalAmountThreads < numThreads); ++i) {
                for (int j = 0; j < this.computingUnitsIds.size() && (totalAmountThreads < numThreads); ++j) {
                    ArrayList<Integer> currentSocketIds = this.computingUnitsIds.get(j);
                    currentSocketIds.add(this.computingUnitsIds.get(j).get(i));
                    this.computingUnitsIds.set(j, currentSocketIds);

                    ArrayList<Integer> currentBinds = this.bindedComputingUnits.get(j);
                    currentBinds.add(-1);
                    this.bindedComputingUnits.set(j, currentBinds);
                    this.availableSlots.set(j, this.availableSlots.get(j) + 1);
                    ++totalAmountThreads;
                }
            }

            if (debug) {
                for (int i = 0; i < this.computingUnitsIds.size(); ++i) {
                    logger.debug("[ThreadBinderCPUs] Socket " + i);
                    StringBuilder sb = new StringBuilder("[ThreadBinderCPUs] Registed slots: ");
                    StringBuilder sb2 = new StringBuilder("[ThreadBinderCPUs] Registed ids: ");
                    for (int j = 0; j < this.computingUnitsIds.get(i).size(); ++j) {
                        sb.append(this.bindedComputingUnits.get(i).get(j) + " ");
                        sb2.append(this.computingUnitsIds.get(i).get(j) + " ");
                    }
                    logger.debug(sb.toString());
                    logger.debug(sb2.toString());
                }
            }
        } catch (IOException e) {
            logger.debug("[ThreadBinderCPU] Unable to obtain the total amount of sockets");
            throw new InitializationException("Unable to obtain the total amount of sockets");
        }
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
            for (int i = 1; i <= this.bindedComputingUnits.size(); ++i) {
                usedSockets = recursiveBindingComputingUnits(jobId, numCUs, 0, i);
                if (usedSockets != null) {
                    break;
                }
            }

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
                        this.availableSlots.set(socket, this.availableSlots.get(socket) - 1);
                    }
                }
                if (numAssignedCores == numCUs) {
                    break;
                }
            }
            handleSlotsAdded();
        }

        if (debug) {
            StringBuilder sb = new StringBuilder("[ThreadBinderCPUs] Task " + jobId + " binded to cores ");
            sb.append(assignedCoreUnits[0]);
            for (int i = 1; i < assignedCoreUnits.length; ++i) {
                sb.append(" " + assignedCoreUnits[i]);
            }
            logger.debug(sb.toString());
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
                int slotsFreeded = 0;
                for (int j = 0; j < vector.size(); ++j) {
                    if (vector.get(j) == jobId) {
                        vector.set(j, -1);
                        ++slotsFreeded;
                    }
                }
                this.availableSlots.set(i, this.availableSlots.get(i) + slotsFreeded);
            }
            handleSlotsFreeded();
        }
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
    private ArrayList<Integer> recursiveBindingComputingUnits(int jobId, int amount, int index, int sockets) {
        // No sockets left to assign threads
        if (sockets == 0) {
            return null;
        }
        // With the current index, we can fulfill the thread requirements
        if (this.availableSlots.get(index) >= amount) {
            ArrayList<Integer> socketUsed = new ArrayList<Integer>();
            socketUsed.add(index);
            return socketUsed;
        }
        // More sockets are needed
        // Current socket is used
        if (this.availableSlots.get(index) > 0) {
            int newAmountThreads = amount - this.availableSlots.get(index);
            int newAmountSockets = sockets - 1;
            for (int j = index + 1; j < (this.availableSlots.size() - newAmountSockets + 1); ++j) {
                ArrayList<Integer> nextSocketsUsed = recursiveBindingComputingUnits(jobId, newAmountThreads, j, newAmountSockets);
                if (nextSocketsUsed != null) {
                    nextSocketsUsed.add(index);
                    return nextSocketsUsed;
                }
            }
        }
        // Current socket is not used
        return recursiveBindingComputingUnits(jobId, amount, index + 1, sockets);
    }

    // Update structures to ensure that the ones with more slots are stored first in the ArrayList
    private void updateSocketPriority() {
        ArrayList<Integer> sockets = new ArrayList<Integer>();
        for (int i = 0; i < this.availableSlots.size(); ++i) {
            sockets.add(i);
        }
        ArrayList<Integer> availableSlots = this.availableSlots;
        Comparator<Integer> customComparator = new Comparator<Integer>() {

            public int compare(Integer first, Integer second) {
                return Integer.compare(availableSlots.get(second), availableSlots.get(first));
            }
        };
        Collections.sort(sockets, customComparator);
        ArrayList<ArrayList<Integer>> newComputingUnitsIds = new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> newBindedComputingUnits = new ArrayList<ArrayList<Integer>>();
        ArrayList<Integer> newAvailableSlots = new ArrayList<Integer>();
        for (int i = 0; i < this.availableSlots.size(); ++i) {
            int currentSocketIndex = sockets.get(i);
            newComputingUnitsIds.add(this.computingUnitsIds.get(currentSocketIndex));
            newBindedComputingUnits.add(this.bindedComputingUnits.get(currentSocketIndex));
            newAvailableSlots.add(this.availableSlots.get(currentSocketIndex));
        }
        this.computingUnitsIds = newComputingUnitsIds;
        this.bindedComputingUnits = newBindedComputingUnits;
        this.availableSlots = newAvailableSlots;
    }

    private void handleSlotsAdded() {
        // Update data structures in order to tune the behaviour of the binder
        updateSocketPriority();
    }

    private void handleSlotsFreeded() {
        // Rearrange the thread affinity to avoid the apparition of 'holes'
        // Look up for a mechanism to obtain the pid of processes
        updateSocketPriority();
    }

}
