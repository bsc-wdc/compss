package integratedtoolkit.util;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.MethodWorker;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;

import java.util.HashMap;

import org.apache.log4j.Logger;


public class WorkerStarter implements Runnable {

    // Log and debug
    protected static final Logger logger = Logger.getLogger(Loggers.COMM);
    public static final boolean debug = logger.isDebugEnabled();

    private static int[] onStartCoreCounts = new int[CoreManager.getCoreCount()];

    private final String name;
    private final MethodResourceDescription rd;
    private final HashMap<String, String> disks;
    private final MethodConfiguration configuration;
    
    private final int taskCount;
    private final int[] expectedCoreCount;
    

    public WorkerStarter(String name, MethodResourceDescription rd, HashMap<String, String> disks, MethodConfiguration config) {
        this.name = name;
        this.rd = rd;
        this.configuration = config;
        if (disks == null) {
        	this.disks = new HashMap<String, String>();
        } else {
        	this.disks = disks;
        }
        
        // Compute task count
        int limitOfTasks = config.getLimitOfTasks();
        int computingUnits = rd.getTotalComputingUnits();
        if (limitOfTasks < 0 && computingUnits < 0) {
        	this.taskCount = 0;
        } else {
        	this.taskCount = Math.max(limitOfTasks, computingUnits);
        }
        
        // Compute expected core count
        expectedCoreCount = computeExpectedCoreCount(taskCount);
        synchronized (onStartCoreCounts) {
            for (int coreId = 0; coreId < expectedCoreCount.length; coreId++) {
                onStartCoreCounts[coreId] += expectedCoreCount[coreId];
            }
        }
    }

    public void run() {
        Thread.currentThread().setName(name + " starter");
        Worker<?> newResource;
        try {
            newResource = new MethodWorker(name, rd, configuration, taskCount);
        } catch (Exception e) {
            logger.error("Error starting resource", e);
            ErrorManager.warn("Exception creating worker. Check runtime.log for more details", e);
            return;
        }
        for (java.util.Map.Entry<String, String> disk : disks.entrySet()) {
            newResource.addSharedDisk(disk.getKey(), disk.getValue());
        }
        synchronized (onStartCoreCounts) {
            for (int coreId = 0; coreId < expectedCoreCount.length; coreId++) {
                onStartCoreCounts[coreId] -= expectedCoreCount[coreId];
            }
            ResourceManager.addStaticWorker(newResource);
        }
    }

    public static int[] getExpectedCoreCount() {
        synchronized (onStartCoreCounts) {
            return onStartCoreCounts;
        }
    }

    public int[] computeExpectedCoreCount(int taskCount) {
        int coreCount = CoreManager.getCoreCount();
        int[] coreSimultaneousTasks = new int[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            Implementation<?>[] impls = CoreManager.getCoreImplementations(coreId);
            int ideal = 0;
            for (Implementation<?> impl : impls) {
                if (rd.canHost(impl)) {
                    ideal = Math.max(rd.canHostSimultaneously((MethodResourceDescription) impl.getRequirements()), ideal);
                }
                coreSimultaneousTasks[coreId] = Math.min(taskCount, ideal);
            }
        }
        return coreSimultaneousTasks;
    }

}
