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
package es.bsc.compss.types.resources;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.AnnounceException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.util.CoreManager;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class Worker<T extends WorkerResourceDescription> extends ResourceImpl {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.RM_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    protected final T description;

    // CoreIds that can be executed by this resource
    private List<Integer> executableCores;
    // Implementations that can be executed by the resource
    private List<Implementation>[] executableImpls;
    // ImplIds per core that can be executed by this resource
    private int[][] implSimultaneousTasks;

    // Number of tasks that can be run simultaneously per core id
    private int[] coreSimultaneousTasks;
    // Number of tasks that can be run simultaneously per core id (maxTaskCount not considered)
    private int[] idealSimultaneousTasks;


    /**
     * Creates a new Worker instance.
     * 
     * @param name Worker name.
     * @param description Worker description.
     * @param worker Associated COMPSs worker.
     * @param limitOfTasks Limit of tasks.
     * @param sharedDisks Mounted shared disks.
     */
    @SuppressWarnings("unchecked")
    public Worker(String name, T description, COMPSsNode worker, int limitOfTasks, Map<String, String> sharedDisks) {
        super(worker, sharedDisks);

        int coreCount = CoreManager.getCoreCount();
        this.coreSimultaneousTasks = new int[coreCount];
        this.idealSimultaneousTasks = new int[coreCount];
        this.executableCores = new LinkedList<>();
        this.implSimultaneousTasks = new int[coreCount][];
        this.executableImpls = new LinkedList[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            executableImpls[coreId] = new LinkedList<>();
            implSimultaneousTasks[coreId] = new int[CoreManager.getNumberCoreImplementations(coreId)];
        }

        this.description = description;
    }

    /**
     * Creates a new Worker instance.
     *
     * @param name Worker name.
     * @param description Worker description.
     * @param config Worker configuration.
     * @param sharedDisks Mounted shared disks.
     */
    @SuppressWarnings("unchecked")
    public Worker(String name, T description, Configuration config, Map<String, String> sharedDisks) {
        super(name, config, sharedDisks);

        int coreCount = CoreManager.getCoreCount();
        this.coreSimultaneousTasks = new int[coreCount];
        this.idealSimultaneousTasks = new int[coreCount];
        this.executableCores = new LinkedList<>();
        this.implSimultaneousTasks = new int[coreCount][];
        this.executableImpls = new LinkedList[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            executableImpls[coreId] = new LinkedList<>();
            implSimultaneousTasks[coreId] = new int[CoreManager.getNumberCoreImplementations(coreId)];
        }

        this.description = description;
    }

    /**
     * Clones the given worker.
     * 
     * @param w Worker to clone.
     */
    public Worker(Worker<T> w) {
        super(w);
        this.coreSimultaneousTasks = w.coreSimultaneousTasks;
        this.idealSimultaneousTasks = w.idealSimultaneousTasks;
        this.executableCores = w.executableCores;
        this.implSimultaneousTasks = w.implSimultaneousTasks;
        this.executableImpls = w.executableImpls;

        this.description = w.description;

    }

    /**
     * Returns the worker description.
     * 
     * @return The worker description.
     */
    public T getDescription() {
        return this.description;
    }

    /**
     * Resets the number of used tasks.
     */
    public void resetUsedTaskCounts() {
        // Nothing to do.
    }

    /*-------------------------------------------------------------------------
     * ************************************************************************
     * ************************************************************************
     * ********* EXECUTABLE CORES AND IMPLEMENTATIONS MANAGEMENT **************
     * ************************************************************************
     * ************************************************************************
     * -----------------------------------------------------------------------*/

    /**
     * Updates the registered core elements.
     * 
     * @param updatedCoreIds New core elements.
     */
    @SuppressWarnings("unchecked")
    public void updatedCoreElements(List<Integer> updatedCoreIds) {
        if (DEBUG) {
            LOGGER.debug("Update coreElements on Worker " + this.getName());
        }
        int coreCount = CoreManager.getCoreCount();
        boolean[] coresToUpdate = new boolean[coreCount];
        for (int coreId : updatedCoreIds) {
            coresToUpdate[coreId] = true;
        }
        boolean[] wasExecutable = new boolean[coreCount];
        for (int coreId : this.executableCores) {
            wasExecutable[coreId] = true;
        }
        this.executableCores.clear();
        List<Implementation>[] executableImpls = new LinkedList[coreCount];
        int[][] implSimultaneousTasks = new int[coreCount][];
        int[] coreSimultaneousTasks = new int[coreCount];
        int[] idealSimultaneousTasks = new int[coreCount];

        for (int coreId = 0; coreId < coreCount; coreId++) {
            if (!coresToUpdate[coreId]) {
                executableImpls[coreId] = this.executableImpls[coreId];
                implSimultaneousTasks[coreId] = this.implSimultaneousTasks[coreId];
                coreSimultaneousTasks[coreId] = this.coreSimultaneousTasks[coreId];
                idealSimultaneousTasks[coreId] = this.idealSimultaneousTasks[coreId];
                if (wasExecutable[coreId]) {
                    this.executableCores.add(coreId);
                }
            } else {
                boolean executableCore = false;
                List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
                implSimultaneousTasks[coreId] = new int[impls.size()];
                executableImpls[coreId] = new LinkedList<>();
                for (Implementation impl : impls) {
                    if (impl.isLocalProcessing() && this != Comm.getAppHost()) {
                        continue;
                    }
                    if (canRun(impl)) {
                        int simultaneousCapacity = simultaneousCapacity(impl);
                        idealSimultaneousTasks[coreId] = Math.max(idealSimultaneousTasks[coreId], simultaneousCapacity);
                        implSimultaneousTasks[coreId][impl.getImplementationId()] = simultaneousCapacity;
                        if (implSimultaneousTasks[coreId][impl.getImplementationId()] > 0) {
                            executableImpls[coreId].add(impl);
                            executableCore = true;
                        }
                    }
                }
                if (executableCore) {
                    this.executableCores.add(coreId);
                    coreSimultaneousTasks[coreId] = limitIdealSimultaneousTasks(idealSimultaneousTasks[coreId]);
                }
            }
        }

        this.executableImpls = executableImpls;
        this.implSimultaneousTasks = implSimultaneousTasks;
        this.coreSimultaneousTasks = coreSimultaneousTasks;
        this.idealSimultaneousTasks = idealSimultaneousTasks;
    }

    /**
     * Updates the internal features.
     */
    @SuppressWarnings("unchecked")
    public void updatedFeatures() {
        int coreCount = CoreManager.getCoreCount();
        this.executableCores.clear();
        this.executableImpls = new LinkedList[coreCount];
        this.implSimultaneousTasks = new int[coreCount][];
        this.coreSimultaneousTasks = new int[coreCount];
        this.idealSimultaneousTasks = new int[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            boolean executableCore = false;
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            this.implSimultaneousTasks[coreId] = new int[impls.size()];
            this.executableImpls[coreId] = new LinkedList<>();
            for (Implementation impl : impls) {
                if (impl.isLocalProcessing() && this != Comm.getAppHost()) {
                    continue;
                }
                if (canRun(impl)) {
                    int simultaneousCapacity = simultaneousCapacity(impl);
                    this.idealSimultaneousTasks[coreId] =
                        Math.max(this.idealSimultaneousTasks[coreId], simultaneousCapacity);
                    this.implSimultaneousTasks[coreId][impl.getImplementationId()] = simultaneousCapacity;
                    if (simultaneousCapacity > 0) {
                        this.executableImpls[coreId].add(impl);
                        executableCore = true;
                    }
                }
            }
            if (executableCore) {
                this.coreSimultaneousTasks[coreId] = limitIdealSimultaneousTasks(this.idealSimultaneousTasks[coreId]);
                this.executableCores.add(coreId);
            }
        }
    }

    /**
     * Returns the maximum simultaneous tasks.
     * 
     * @param ideal Ideal simultaneous tasks.
     * @return Maximum simultaneous tasks.
     */
    protected int limitIdealSimultaneousTasks(int ideal) {
        return ideal;
    }

    /**
     * Returns a list of the executable cores by the current worker.
     * 
     * @return A list of the executable cores by the current worker.
     */
    public List<Integer> getExecutableCores() {
        return this.executableCores;
    }

    /**
     * Returns a list of the executable implementations by the current worker.
     * 
     * @return A list of the executable implementations by the current worker.
     */
    public List<Implementation>[] getExecutableImpls() {
        return this.executableImpls;
    }

    /**
     * Returns a list of the executable implementations of the core with id {@code coreId} by the current worker.
     * 
     * @param coreId Core Id.
     * @return A list of the executable implementations of the core with id {@code coreId} by the current worker.
     */
    public List<Implementation> getExecutableImpls(int coreId) {
        return this.executableImpls[coreId];
    }

    /**
     * Returns the ideal simultaneous tasks per core.
     * 
     * @return The ideal simultaneous tasks per core.
     */
    public int[] getIdealSimultaneousTasks() {
        return this.idealSimultaneousTasks;
    }

    /**
     * Returns the simultaneous tasks per core.
     * 
     * @return The simultaneous tasks per core.
     */
    public int[] getSimultaneousTasks() {
        return this.coreSimultaneousTasks;
    }

    /**
     * Returns the number of simultaneous executions of the given implementation {@code impl} that the current worker
     * can run.
     * 
     * @param impl Implementation to run simultaneously.
     * @return Number of simultaneous executions of the given implementation {@code impl} that the current worker can
     *         run.
     */
    public Integer simultaneousCapacity(Implementation impl) {
        return fitCount(impl);
    }

    /**
     * Dumps the resource links with the given prefix.
     * 
     * @param prefix Indentation prefix.
     * @return String containing a dump of the resource links.
     */
    public String getResourceLinks(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("NAME = ").append(getName()).append("\n");
        sb.append(prefix).append("EXEC_CORES = ").append(this.executableCores).append("\n");
        sb.append(prefix).append("CORE_SIMTASKS = [").append("\n");
        for (int i = 0; i < this.coreSimultaneousTasks.length; ++i) {
            sb.append(prefix).append("\t").append("CORE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("COREID = ").append(i).append("\n");
            sb.append(prefix).append("\t").append("\t").append("SIM_TASKS = ").append(this.coreSimultaneousTasks[i])
                .append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("]").append("\n");
        sb.append(prefix).append("IMPL_SIMTASKS = [").append("\n");
        for (int i = 0; i < this.implSimultaneousTasks.length; ++i) {
            for (int j = 0; j < this.implSimultaneousTasks[i].length; ++j) {
                sb.append(prefix).append("\t").append("IMPLEMENTATION = [").append("\n");
                sb.append(prefix).append("\t").append("\t").append("COREID = ").append(i).append("\n");
                sb.append(prefix).append("\t").append("\t").append("IMPLID = ").append(j).append("\n");
                sb.append(prefix).append("\t").append("\t").append("SIM_TASKS = ")
                    .append(this.implSimultaneousTasks[i][j]).append("\n");
                sb.append(prefix).append("\t").append("]").append("\n");
            }
        }
        sb.append(prefix).append("]").append("\n");

        return sb.toString();
    }

    /*-------------------------------------------------------------------------
     * ************************************************************************
     * ************************************************************************
     * ********************* AVAILABLE FEATURES MANAGEMENT ********************
     * ************************************************************************
     * ************************************************************************
     * -----------------------------------------------------------------------*/

    /**
     * Returns a list of runnable cores by the current worker.
     * 
     * @return List of runnable cores by the current worker.
     */
    public List<Integer> getRunnableCores() {
        List<Integer> cores = new LinkedList<>();
        int coreCount = CoreManager.getCoreCount();
        for (int coreId = 0; coreId < coreCount; coreId++) {
            if (!getRunnableImplementations(coreId).isEmpty()) {
                cores.add(coreId);
            }
        }
        return cores;
    }

    /**
     * Returns a list of runnable implementations per core by the current worker.
     * 
     * @return A list of runnable implementations per core by the current worker.
     */
    @SuppressWarnings("unchecked")
    public List<Implementation>[] getRunnableImplementations() {
        int coreCount = CoreManager.getCoreCount();
        List<Implementation>[] runnable = new LinkedList[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            runnable[coreId] = getRunnableImplementations(coreId);
        }
        return runnable;
    }

    /**
     * Returns a list of the runnable implementations of the core with id {@code coreId} by the current worker.
     * 
     * @param coreId Core Id.
     * @return A list of the runnable implementations of the core with id {@code coreId} by the current worker.
     */
    @SuppressWarnings("unchecked")
    public List<Implementation> getRunnableImplementations(int coreId) {
        List<Implementation> runnable = new LinkedList<>();
        for (Implementation impl : this.executableImpls[coreId]) {
            if (canRunNow((T) impl.getRequirements())) {
                runnable.add(impl);
            }
        }
        return runnable;
    }

    /**
     * Returns whether the current worker can run something or not.
     * 
     * @return {@literal true} if the current worker can run something, {@literal false} otherwise.
     */
    public final boolean canRunSomething() {
        if (this.isLost()) {
            return false;
        }
        int coreCount = CoreManager.getCoreCount();
        for (int coreId = 0; coreId < coreCount; coreId++) {
            if (!getRunnableImplementations(coreId).isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether the current worker can run the core element with id {@code coreId} or not.
     * 
     * @param coreId Core Id.
     * @return {@literal true} if the current worker can run the given core element, {@literal false} otherwise.
     */
    public boolean canRun(int coreId) {
        if (this.isLost()) {
            return false;
        }
        return this.idealSimultaneousTasks[coreId] > 0;
    }

    /**
     * Returns whether the current worker can run the given implementation or not.
     * 
     * @param implementation Implementation to run.
     * @return {@literal true} if the worker can run the current implementation, {@literal false} otherwise.
     */
    public abstract boolean canRun(Implementation implementation);

    /**
     * Returns the implementations inside {@code candidates} that can run now.
     * 
     * @param candidates Candidate implementations to run.
     * @return A list of implementations inside {@code candidates} that can run now.
     */
    @SuppressWarnings("unchecked")
    public List<Implementation> canRunNow(LinkedList<Implementation> candidates) {
        List<Implementation> runnable = new LinkedList<>();
        for (Implementation impl : candidates) {
            if (canRunNow((T) impl.getRequirements())) {
                runnable.add(impl);
            }
        }
        return runnable;
    }

    /**
     * Returns whether the given consumption can be executed now or not.
     * 
     * @param consumption Consumption to execute.
     * @return {@literal true} if the consumption can run now, {@literal false} otherwise.
     */
    public boolean canRunNow(T consumption) {
        if (this.isLost()) {
            return false;
        }
        // Available slots
        return this.hasAvailable(consumption);
    }

    /**
     * Start the execution of a given consumption (and reserve it).
     * 
     * @param consumption Consumption that must be started.
     * @return The real resource consumption reserved.
     */
    public T runTask(T consumption) {
        // There are free task-slots
        return reserveResource(consumption);
    }

    /**
     * Ends the execution of the given consumption (and releases it).
     * 
     * @param consumption Consumption that must be ended.
     */
    public void endTask(T consumption) {
        if (DEBUG) {
            LOGGER.debug("End task received. Releasing resource " + getName());
        }
        releaseResource(consumption);
    }

    /**
     * Returns the monitoring data.
     * 
     * @param prefix Indentation prefix.
     * @return The monitoring data.
     */
    public abstract String getMonitoringData(String prefix);

    /*
     * Internal private methods depending on the resourceType
     */

    /**
     * Returns the number of simultaneous executions of the given implementation.
     * 
     * @param impl Implementation.
     * @return The number of simultaneous executions of the given implementation.
     */
    public abstract Integer fitCount(Implementation impl);

    /**
     * Returns whether the current worker can host the given consumption or not.
     * 
     * @param consumption Consumption to host.
     * @return {@literal true} if the current worker can host the given consumption, {@literal false} otherwise.
     */
    public abstract boolean hasAvailable(T consumption);

    /**
     * Returns whether the current worker has available slots or not.
     * 
     * @return {@literal true} if the current worker has available slots, {@code literal} false otherwise.
     */
    public abstract boolean hasAvailableSlots();

    /**
     * Reserves the given consumption.
     * 
     * @param consumption Consumption to reserve.
     * @return Final consumption reserved.
     */
    public abstract T reserveResource(T consumption);

    /**
     * Releases the given consumption.
     * 
     * @param consumption Consumption to release.
     */
    public abstract void releaseResource(T consumption);

    /**
     * Releases all the resources inside the current worker.
     */
    public abstract void releaseAllResources();

    /**
     * Announces the creation of the current worker.
     * 
     * @throws AnnounceException When an internal error occurs in the worker announcement.
     */
    public void announceCreation() throws AnnounceException {
        COMPSsWorker w = (COMPSsWorker) this.getNode();
        w.announceCreation();
    }

    /**
     * Announces the destruction of the current worker.
     * 
     * @throws AnnounceException When an internal error occurs in the worker announcement.
     */
    public void announceDestruction() throws AnnounceException {
        COMPSsWorker w = (COMPSsWorker) this.getNode();
        w.announceDestruction();
    }

    /**
     * Returns an scheduling copy of the current worker.
     * 
     * @return An scheduling copy of the current worker.
     */
    public abstract Worker<T> getSchedulingCopy();

    @Override
    public String toString() {
        return "Worker " + this.name + " with the following description " + this.description;
    }

}
