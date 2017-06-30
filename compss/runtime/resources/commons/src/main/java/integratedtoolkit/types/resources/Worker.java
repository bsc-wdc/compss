package integratedtoolkit.types.resources;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.COMPSsNode;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.configuration.Configuration;
import integratedtoolkit.util.CoreManager;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class Worker<T extends WorkerResourceDescription> extends Resource {

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

    // Task count
    private int usedCPUTaskCount = 0;
    private final int maxCPUTaskCount;
    private int usedGPUTaskCount = 0;
    private final int maxGPUTaskCount;
    private int usedFPGATaskCount = 0;
    private final int maxFPGATaskCount;
    private int usedOthersTaskCount = 0;
    private final int maxOthersTaskCount;


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
        this.maxCPUTaskCount = limitOfTasks;
        this.maxGPUTaskCount = 0;
        this.maxFPGATaskCount = 0;
        this.maxOthersTaskCount = 0;
    }

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
        this.maxCPUTaskCount = config.getLimitOfTasks();
        this.maxGPUTaskCount = config.getLimitOfGPUTasks();
        this.maxFPGATaskCount = config.getLimitOfFPGATasks();
        this.maxOthersTaskCount = config.getLimitOfOTHERSTasks();
    }

    public Worker(Worker<T> w) {
        super(w);
        this.coreSimultaneousTasks = w.coreSimultaneousTasks;
        this.idealSimultaneousTasks = w.idealSimultaneousTasks;
        this.executableCores = w.executableCores;
        this.implSimultaneousTasks = w.implSimultaneousTasks;
        this.executableImpls = w.executableImpls;

        this.description = w.description;

        this.maxCPUTaskCount = w.maxCPUTaskCount;
        this.usedCPUTaskCount = w.usedCPUTaskCount;
        this.maxGPUTaskCount = w.maxGPUTaskCount;
        this.usedGPUTaskCount = w.usedGPUTaskCount;
        this.maxFPGATaskCount = w.maxFPGATaskCount;
        this.usedFPGATaskCount = w.usedFPGATaskCount;
        this.maxOthersTaskCount = w.maxOthersTaskCount;
        this.usedOthersTaskCount = w.usedOthersTaskCount;
    }

    public T getDescription() {
        return description;
    }

    public int getMaxCPUTaskCount() {
        return this.maxCPUTaskCount;
    }

    public int getUsedCPUTaskCount() {
        return this.usedCPUTaskCount;
    }

    public int getMaxGPUTaskCount() {
        return this.maxGPUTaskCount;
    }

    public int getUsedGPUTaskCount() {
        return this.usedGPUTaskCount;
    }

    public int getMaxFPGATaskCount() {
        return this.maxFPGATaskCount;
    }

    public int getUsedFPGATaskCount() {
        return this.usedFPGATaskCount;
    }

    public int getMaxOthersTaskCount() {
        return this.maxOthersTaskCount;
    }

    public int getUsedOthersTaskCount() {
        return this.usedOthersTaskCount;
    }

    private void decreaseUsedCPUTaskCount() {
        this.usedCPUTaskCount--;
    }

    private void increaseUsedCPUTaskCount() {
        this.usedCPUTaskCount++;
    }

    private void decreaseUsedGPUTaskCount() {
        this.usedGPUTaskCount--;
    }

    private void increaseUsedGPUTaskCount() {
        this.usedGPUTaskCount++;
    }

    private void decreaseUsedFPGATaskCount() {
        this.usedFPGATaskCount--;
    }

    private void increaseUsedFPGATaskCount() {
        this.usedFPGATaskCount++;
    }

    private void decreaseUsedOthersTaskCount() {
        this.usedOthersTaskCount--;
    }

    private void increaseUsedOthersTaskCount() {
        this.usedOthersTaskCount++;
    }

    public void resetUsedTaskCounts() {
        usedCPUTaskCount = 0;
        usedGPUTaskCount = 0;
        usedFPGATaskCount = 0;
        usedOthersTaskCount = 0;
    }

    /*-------------------------------------------------------------------------
     * ************************************************************************
     * ************************************************************************
     * ********* EXECUTABLE CORES AND IMPLEMENTATIONS MANAGEMENT **************
     * ************************************************************************
     * ************************************************************************
     * -----------------------------------------------------------------------*/
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
                    executableCores.add(coreId);
                }
            } else {
                boolean executableCore = false;
                List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
                implSimultaneousTasks[coreId] = new int[impls.size()];
                executableImpls[coreId] = new LinkedList<>();
                for (Implementation impl : impls) {
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
                    executableCores.add(coreId);
                }
            }
        }

        this.executableImpls = executableImpls;
        this.implSimultaneousTasks = implSimultaneousTasks;
        this.coreSimultaneousTasks = coreSimultaneousTasks;
        this.idealSimultaneousTasks = idealSimultaneousTasks;
    }

    @SuppressWarnings("unchecked")
    public void updatedFeatures() {
        int coreCount = CoreManager.getCoreCount();
        executableCores.clear();
        executableImpls = new LinkedList[coreCount];
        implSimultaneousTasks = new int[coreCount][];
        coreSimultaneousTasks = new int[coreCount];
        idealSimultaneousTasks = new int[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            boolean executableCore = false;
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            implSimultaneousTasks[coreId] = new int[impls.size()];
            executableImpls[coreId] = new LinkedList<>();
            for (Implementation impl : impls) {
                if (canRun(impl)) {
                    int simultaneousCapacity = simultaneousCapacity(impl);
                    idealSimultaneousTasks[coreId] = Math.max(idealSimultaneousTasks[coreId], simultaneousCapacity);
                    implSimultaneousTasks[coreId][impl.getImplementationId()] = simultaneousCapacity;
                    if (simultaneousCapacity > 0) {
                        executableImpls[coreId].add(impl);
                        executableCore = true;
                    }
                }
            }
            if (executableCore) {
                coreSimultaneousTasks[coreId] = Math.min(this.getMaxCPUTaskCount(), idealSimultaneousTasks[coreId]);
                executableCores.add(coreId);
            }
        }
    }

    public List<Integer> getExecutableCores() {
        return executableCores;
    }

    public List<Implementation>[] getExecutableImpls() {
        return executableImpls;
    }

    public List<Implementation> getExecutableImpls(int coreId) {
        return executableImpls[coreId];
    }

    public int[] getIdealSimultaneousTasks() {
        return this.idealSimultaneousTasks;
    }

    public int[] getSimultaneousTasks() {
        return coreSimultaneousTasks;
    }

    public Integer simultaneousCapacity(Implementation impl) {
        return Math.min(fitCount(impl), this.getMaxCPUTaskCount());
    }

    public String getResourceLinks(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("NAME = ").append(getName()).append("\n");
        sb.append(prefix).append("EXEC_CORES = ").append(executableCores).append("\n");
        sb.append(prefix).append("CORE_SIMTASKS = [").append("\n");
        for (int i = 0; i < coreSimultaneousTasks.length; ++i) {
            sb.append(prefix).append("\t").append("CORE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("COREID = ").append(i).append("\n");
            sb.append(prefix).append("\t").append("\t").append("SIM_TASKS = ").append(coreSimultaneousTasks[i]).append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("]").append("\n");
        sb.append(prefix).append("IMPL_SIMTASKS = [").append("\n");
        for (int i = 0; i < implSimultaneousTasks.length; ++i) {
            for (int j = 0; j < implSimultaneousTasks[i].length; ++j) {
                sb.append(prefix).append("\t").append("IMPLEMENTATION = [").append("\n");
                sb.append(prefix).append("\t").append("\t").append("COREID = ").append(i).append("\n");
                sb.append(prefix).append("\t").append("\t").append("IMPLID = ").append(j).append("\n");
                sb.append(prefix).append("\t").append("\t").append("SIM_TASKS = ").append(implSimultaneousTasks[i][j]).append("\n");
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

    @SuppressWarnings("unchecked")
    public List<Implementation>[] getRunnableImplementations() {
        int coreCount = CoreManager.getCoreCount();
        List<Implementation>[] runnable = new LinkedList[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            runnable[coreId] = getRunnableImplementations(coreId);
        }
        return runnable;
    }

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

    public boolean canRun(int coreId) {
        return this.idealSimultaneousTasks[coreId] > 0;
    }

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

    public boolean canRunNow(T consumption) {
        // Available slots
        boolean canRun = this.getUsedCPUTaskCount() < this.getMaxCPUTaskCount();
        canRun = canRun && ((this.getUsedGPUTaskCount() < this.getMaxGPUTaskCount()) || !this.usesGPU(consumption));
        canRun = canRun && ((this.getUsedFPGATaskCount() < this.getMaxFPGATaskCount()) || !this.usesFPGA(consumption));
        canRun = canRun && ((this.getUsedOthersTaskCount() < this.getMaxOthersTaskCount()) || !this.usesOthers(consumption));
        canRun = canRun && this.hasAvailable(consumption);
        return canRun;
    }

    public void endTask(T consumption) {
        if (DEBUG) {
            LOGGER.debug("End task received. Releasing resource " + getName());
        }

        this.decreaseUsedCPUTaskCount();
        if (this.usesGPU(consumption)) {
            this.decreaseUsedGPUTaskCount();
        }
        if (this.usesFPGA(consumption)) {
            this.decreaseUsedFPGATaskCount();
        }
        if (this.usesOthers(consumption)) {
            this.decreaseUsedOthersTaskCount();
        }
        releaseResource(consumption);
    }

    public T runTask(T consumption) {
        if (this.usedCPUTaskCount < this.maxCPUTaskCount) {
            // There are free task-slots
            T reserved = reserveResource(consumption);
            if (reserved != null) {
                // Consumption can be hosted
                this.increaseUsedCPUTaskCount();
                if (this.usesGPU(consumption)) {
                    this.increaseUsedGPUTaskCount();
                }
                if (this.usesFPGA(consumption)) {
                    this.increaseUsedFPGATaskCount();
                }
                if (this.usesOthers(consumption)) {
                    this.increaseUsedOthersTaskCount();
                }
                return reserved;
            } else {
                // Consumption cannot be hosted
                return null;
            }
        }

        // Consumption cannot be hosted
        return null;
    }

    public abstract String getMonitoringData(String prefix);

    public abstract boolean canRun(Implementation implementation);

    // Internal private methods depending on the resourceType
    public abstract Integer fitCount(Implementation impl);

    public abstract boolean hasAvailable(T consumption);

    public abstract boolean usesGPU(T consumption);

    public abstract boolean usesFPGA(T consumption);

    public abstract boolean usesOthers(T consumption);

    public abstract T reserveResource(T consumption);

    public abstract void releaseResource(T consumption);

    public abstract void releaseAllResources();

    public void announceCreation() throws Exception {
        COMPSsWorker w = (COMPSsWorker) this.getNode();
        w.announceCreation();
    }

    public void announceDestruction() throws Exception {
        COMPSsWorker w = (COMPSsWorker) this.getNode();
        w.announceDestruction();
    }

    public abstract Worker<T> getSchedulingCopy();

    @Override
    public String toString() {
        return "Worker " + description + " with usedTaskCount = " + usedCPUTaskCount + " and maxTaskCount = " + maxCPUTaskCount
                + " with the following description " + description;
    }

}
