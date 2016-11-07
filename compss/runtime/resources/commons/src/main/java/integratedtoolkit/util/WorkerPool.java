package integratedtoolkit.util;

import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.Worker;
import java.util.Collection;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeSet;


public class WorkerPool {

    // Resource Sets:
    // Static Resources (read from xml)
    private HashMap<String, Worker<?>> staticSet;
    // Critical Resources (can't be destroyed by periodical resource policy)
    private HashMap<String, CloudMethodWorker> criticalSet;
    // Non Critical Resources (can be destroyed by periodical resource policy)
    private HashMap<String, CloudMethodWorker> nonCriticalSet;

    // TreeSet : Priority on criticalSet based on cost
    private TreeSet<CloudMethodWorker> criticalOrder;


    public WorkerPool() {
        staticSet = new HashMap<>();
        criticalSet = new HashMap<>();
        nonCriticalSet = new HashMap<>();
        criticalOrder = new TreeSet<>();
    }

    // Adds a new Resource on the Physical list
    public void addStaticResource(Worker<?> newResource) {
        staticSet.put(newResource.getName(), newResource);
    }

    // Adds a new Resource on the Critical list
    public void addDynamicResource(CloudMethodWorker newResource) {
        criticalSet.put(newResource.getName(), newResource);
        criticalOrder.add(newResource);
    }

    public void coreElementUpdates(LinkedList<Integer> newCores) {
        for (Worker<?> r : staticSet.values()) {
            r.updatedCoreElements(newCores);
        }
        for (Worker<?> r : criticalSet.values()) {
            r.updatedCoreElements(newCores);
        }
        for (Worker<?> r : nonCriticalSet.values()) {
            r.updatedCoreElements(newCores);
        }
    }

    public Collection<Worker<?>> getStaticResources() {
        return staticSet.values();
    }

    public Worker<?> getStaticResource(String resourceName) {
        return staticSet.get(resourceName);
    }

    public CloudMethodWorker getDynamicResource(String resourceName) {
        CloudMethodWorker resource = null;
        resource = criticalSet.get(resourceName);
        if (resource == null) {
            resource = nonCriticalSet.get(resourceName);
        }
        return resource;
    }

    public LinkedList<CloudMethodWorker> getDynamicResources() {
        LinkedList<CloudMethodWorker> resources = new LinkedList<>();
        resources.addAll(criticalSet.values());
        resources.addAll(nonCriticalSet.values());

        return resources;
    }

    // returns all the resource information
    public Worker<?> getResource(String resourceName) {
        Worker<?> resource = null;
        resource = staticSet.get(resourceName);
        if (resource == null) {
            resource = criticalSet.get(resourceName);
        }
        if (resource == null) {
            resource = nonCriticalSet.get(resourceName);
        }

        return resource;
    }

    // Deletes a resource from the pool
    public void delete(Worker<?> resource) {
        String resourceName = resource.getName();
        // Remove resource from sets
        if (nonCriticalSet.remove(resourceName) == null) {
            if (criticalSet.remove(resourceName) == null) {
                staticSet.remove(resourceName);
            }
        }
    }

    // Returns a list with all coreIds that can be executed by the resource res
    public List<Integer> getExecutableCores(String res) {
        Worker<?> resource = getResource(res);
        if (resource == null) {
            return new LinkedList<Integer>();
        }
        return resource.getExecutableCores();
    }

    // Selects a subset of the critical set able to execute all the cores
    public void defineCriticalSet() {
        synchronized (this) {
            int coreCount = CoreManager.getCoreCount();
            boolean[] runnable = new boolean[coreCount];
            for (int coreId = 0; coreId < coreCount; coreId++) {
                runnable[coreId] = false;
            }

            String resourceName;
            for (Worker<?> res : staticSet.values()) {
                LinkedList<Integer> cores = res.getExecutableCores();
                for (int i = 0; i < cores.size(); i++) {
                    runnable[cores.get(i)] = true;
                }
            }
            for (CloudMethodWorker resource : criticalOrder) {
                resourceName = resource.getName();
                LinkedList<Integer> executableCores = resource.getExecutableCores();
                boolean needed = false;
                for (int i = 0; i < executableCores.size() && !needed; i++) {
                    needed = needed || !runnable[executableCores.get(i)];
                }
                if (needed) {
                    for (int i = 0; i < executableCores.size(); i++) {
                        runnable[executableCores.get(i)] = true;
                    }
                } else {
                    criticalSet.remove(resourceName);
                    criticalOrder.remove(resource);
                    nonCriticalSet.put(resourceName, resource);
                }
            }
        }
    }

    public Collection<CloudMethodWorker> getNonCriticalResources() {
        return nonCriticalSet.values();
    }

    public Collection<CloudMethodWorker> getCriticalResources() {
        return criticalSet.values();
    }

    // Returns the name of all the resources able to execute coreId
    public LinkedList<Worker<?>> findAllResources() {
        LinkedList<Worker<?>> workers = new LinkedList<>();
        if (staticSet != null && !staticSet.isEmpty()) {
            Object[] arrayStaticSet = staticSet.values().toArray();
            for (int i = 0; i < arrayStaticSet.length; i++) {
                workers.add((Worker<?>) arrayStaticSet[i]);
            }
        }

        if (criticalSet != null && !criticalSet.isEmpty()) {
            Object[] arrayCriticalSet = criticalSet.values().toArray();
            for (int i = 0; i < arrayCriticalSet.length; i++) {
                workers.add((Worker<?>) arrayCriticalSet[i]);
            }
        }

        if (nonCriticalSet != null && !nonCriticalSet.isEmpty()) {
            Object[] arrayNonCriticalSet = nonCriticalSet.values().toArray();
            for (int i = 0; i < arrayNonCriticalSet.length; i++) {
                workers.add((Worker<?>) arrayNonCriticalSet[i]);
            }
        }

        return workers;
    }

    public boolean isCriticalRemovalSafe(int[][] slotReductionImpls) {
        int coreCount = CoreManager.getCoreCount();
        // Compute cores from impl
        int[] slotReductionCores = new int[coreCount];
        for (int coreId = 0; coreId < coreCount; ++coreId) {
            for (int implId = 0; implId < CoreManager.getCoreImplementations(coreId).length; ++implId) {
                if (slotReductionImpls[coreId][implId] > slotReductionCores[coreId]) {
                    slotReductionCores[coreId] = slotReductionImpls[coreId][implId];
                }
            }
        }

        int[] slots = new int[coreCount];
        for (Worker<?> r : criticalSet.values()) {
            int[] resSlots = r.getSimultaneousTasks();
            for (int coreId = 0; coreId < coreCount; coreId++) {
                slots[coreId] += resSlots[coreId];
            }
        }

        for (int coreId = 0; coreId < coreCount; coreId++) {
            if (slotReductionCores[coreId] > 0 && slotReductionCores[coreId] >= slots[coreId]) {
                return false;
            }
        }
        return true;
    }

    public String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        // Resources
        sb.append(prefix).append("RESOURCES = [").append("\n");
        for (Worker<?> r : staticSet.values()) {
            sb.append(prefix).append("\t").append("RESOURCE = [").append("\n");
            sb.append(r.getResourceLinks(prefix + "\t\t")); // Adds resource information
            sb.append(prefix).append("\t").append("\t").append("SET = Static").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        for (Worker<?> r : criticalSet.values()) {
            sb.append(prefix).append("\t").append("RESOURCE = [").append("\n");
            sb.append(r.getResourceLinks(prefix + "\t\t")); // Adds resource information
            sb.append(prefix).append("\t").append("\t").append("SET = Critical").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        for (Worker<?> r : nonCriticalSet.values()) {
            sb.append(prefix).append("\t").append("RESOURCE = [").append("\n");
            sb.append(r.getResourceLinks(prefix + "\t\t")); // Adds resource information
            sb.append(prefix).append("\t").append("\t").append("SET = Non-Critical").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("]").append("\n");

        /*
         * //Cores sb.append(prefix).append("CORES = [").append("\n"); for (int i = 0; i < CoreManager.getCoreCount();
         * i++) { sb.append(prefix).append("\t").append("CORE = [").append("\n");
         * sb.append(prefix).append("\t").append("\t").append("ID = ").append(i).append("\n");
         * sb.append(prefix).append("\t"
         * ).append("\t").append("MAXTASKCOUNT = ").append(coreMaxTaskCount[i]).append("\n");
         * sb.append(prefix).append("\t").append("\t").append("TORESOURCE = [").append("\n"); for (Worker<?> r :
         * coreToResource[i]) {
         * sb.append(prefix).append("\t").append("\t").append("\t").append("RESOURCE = [").append("\n");
         * sb.append(prefix
         * ).append("\t").append("\t").append("\t").append("\t").append("NAME = ").append(r.getName()).append("\n");
         * sb.append (prefix).append("\t").append("\t").append("\t").append("\t").append("SIMTASKS = ").append(r.
         * getSimultaneousTasks ()[i]).append("\n");
         * sb.append(prefix).append("\t").append("\t").append("\t").append("]").append("\n"); }
         * sb.append(prefix).append("\t").append("\t").append("]").append("\n");
         * sb.append(prefix).append("\t").append("]").append("\n"); } sb.append(prefix).append("]").append("\n");
         */
        return sb.toString();
    }

}
