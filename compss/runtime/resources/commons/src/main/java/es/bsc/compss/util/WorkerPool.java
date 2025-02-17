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
package es.bsc.compss.util;

import es.bsc.compss.types.resources.DynamicMethodWorker;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


public class WorkerPool {

    // Resource Sets:
    // Static Resources (read from xml)
    private final Map<String, Worker<? extends WorkerResourceDescription>> staticSet;
    // Critical Resources (can't be destroyed by periodical resource policy)
    private final Map<String, DynamicMethodWorker> criticalSet;
    // Non Critical Resources (can be destroyed by periodical resource policy)
    private final Map<String, DynamicMethodWorker> nonCriticalSet;

    // TreeSet : Priority on criticalSet based on cost
    private final Set<DynamicMethodWorker> criticalOrder;


    /**
     * Creates a new worker pool instance.
     */
    public WorkerPool() {
        this.staticSet = new HashMap<>();
        this.criticalSet = new HashMap<>();
        this.nonCriticalSet = new HashMap<>();
        this.criticalOrder = new TreeSet<>();
    }

    /**
     * Adds a new Resource on the Physical set.
     * 
     * @param newResource Resource to add to the physical set.
     */
    public void addStaticResource(Worker<? extends WorkerResourceDescription> newResource) {
        this.staticSet.put(newResource.getName(), newResource);
    }

    /**
     * Adds a new Resource on the Critical list.
     * 
     * @param newResource Resource to add to the critical set.
     */
    public void addDynamicResource(DynamicMethodWorker newResource) {
        this.criticalSet.put(newResource.getName(), newResource);
        this.criticalOrder.add(newResource);
    }

    /**
     * Updates the registered core elements.
     * 
     * @param newCores New core elements.
     */
    public void coreElementUpdates(List<Integer> newCores) {
        for (Worker<? extends WorkerResourceDescription> r : this.staticSet.values()) {
            r.updatedCoreElements(newCores);
        }
        for (DynamicMethodWorker r : this.criticalSet.values()) {
            r.updatedCoreElements(newCores);
        }
        for (DynamicMethodWorker r : this.nonCriticalSet.values()) {
            r.updatedCoreElements(newCores);
        }
    }

    /**
     * Returns the static resources.
     * 
     * @return The static resources.
     */
    public Collection<Worker<? extends WorkerResourceDescription>> getStaticResources() {
        return this.staticSet.values();
    }

    /**
     * Returns the resource object of the static resource with the given name.
     * 
     * @param resourceName Resource name.
     * @return Resource object.
     */
    public Worker<? extends WorkerResourceDescription> getStaticResource(String resourceName) {
        return this.staticSet.get(resourceName);
    }

    /**
     * Returns the list of dynamic resources.
     * 
     * @return A list of all the dynamic resources.
     */
    public List<DynamicMethodWorker> getDynamicResources() {
        List<DynamicMethodWorker> resources = new LinkedList<>();
        resources.addAll(this.criticalSet.values());
        resources.addAll(this.nonCriticalSet.values());

        return resources;
    }

    /**
     * Returns the resource object of the dynamic resource with the given name.
     * 
     * @param resourceName Resource name.
     * @return Resource object.
     */
    public DynamicMethodWorker getDynamicResource(String resourceName) {
        DynamicMethodWorker resource = null;
        resource = this.criticalSet.get(resourceName);
        if (resource == null) {
            resource = this.nonCriticalSet.get(resourceName);
        }
        return resource;
    }

    /**
     * Returns all the resource information.
     *
     * @param resourceName Resource name.
     * @return Resource information.
     */
    public Worker<? extends WorkerResourceDescription> getResource(String resourceName) {
        Worker<? extends WorkerResourceDescription> resource = null;
        resource = this.staticSet.get(resourceName);
        if (resource == null) {
            resource = this.criticalSet.get(resourceName);
        }
        if (resource == null) {
            resource = this.nonCriticalSet.get(resourceName);
        }

        return resource;
    }

    /**
     * Deletes a resource from the pool.
     *
     * @param resource Resource to delete.
     */
    public void delete(Worker<? extends WorkerResourceDescription> resource) {
        String resourceName = resource.getName();
        DynamicMethodWorker worker;
        // Remove resource from sets
        if (this.nonCriticalSet.remove(resourceName) == null) {
            if ((worker = this.criticalSet.remove(resourceName)) == null) {
                this.staticSet.remove(resourceName);
            } else {
                this.criticalOrder.remove(worker);
            }

        }
    }

    /**
     * Returns the executable cores of the given resource name.
     * 
     * @param resourceName Resource Name.
     * @return List with all coreIds that can be executed by the resource res
     */
    public List<Integer> getExecutableCores(String resourceName) {
        Worker<? extends WorkerResourceDescription> resource = getResource(resourceName);
        if (resource == null) {
            return new LinkedList<>();
        }
        return resource.getExecutableCores();
    }

    /**
     * Selects a subset of the critical set able to execute all the cores.
     */
    public void defineCriticalSet() {
        synchronized (this) {
            int coreCount = CoreManager.getCoreCount();
            boolean[] runnable = new boolean[coreCount];
            for (int coreId = 0; coreId < coreCount; coreId++) {
                runnable[coreId] = false;
            }

            String resourceName;
            for (Worker<? extends WorkerResourceDescription> res : this.staticSet.values()) {
                List<Integer> cores = res.getExecutableCores();
                for (int i = 0; i < cores.size(); i++) {
                    runnable[cores.get(i)] = true;
                }
            }

            LinkedList<DynamicMethodWorker> criticalOrderRemovals = new LinkedList<>();
            for (DynamicMethodWorker resource : this.criticalOrder) {
                resourceName = resource.getName();
                List<Integer> executableCores = resource.getExecutableCores();
                boolean needed = false;
                for (int i = 0; i < executableCores.size() && !needed; i++) {
                    needed = needed || !runnable[executableCores.get(i)];
                }
                if (needed) {
                    for (int i = 0; i < executableCores.size(); i++) {
                        runnable[executableCores.get(i)] = true;
                    }
                } else {
                    this.criticalSet.remove(resourceName);
                    criticalOrderRemovals.add(resource);
                    this.nonCriticalSet.put(resourceName, resource);
                }
            }
            for (DynamicMethodWorker resource : criticalOrderRemovals) {
                this.criticalOrder.remove(resource);
            }
        }
    }

    public Collection<DynamicMethodWorker> getNonCriticalResources() {
        return this.nonCriticalSet.values();
    }

    public Collection<DynamicMethodWorker> getCriticalResources() {
        return this.criticalSet.values();
    }

    /**
     * Returns a list with all the available resources.
     * 
     * @return A list with all the available resources.
     */
    @SuppressWarnings("unchecked")
    public List<Worker<? extends WorkerResourceDescription>> findAllResources() {
        List<Worker<? extends WorkerResourceDescription>> workers = new LinkedList<>();

        if (this.staticSet != null && !this.staticSet.isEmpty()) {
            Object[] arrayStaticSet = this.staticSet.values().toArray();
            for (int i = 0; i < arrayStaticSet.length; i++) {
                workers.add((Worker<? extends WorkerResourceDescription>) arrayStaticSet[i]);
            }
        }

        if (this.criticalSet != null && !this.criticalSet.isEmpty()) {
            Object[] arrayCriticalSet = this.criticalSet.values().toArray();
            for (int i = 0; i < arrayCriticalSet.length; i++) {
                workers.add((Worker<? extends WorkerResourceDescription>) arrayCriticalSet[i]);
            }
        }

        if (this.nonCriticalSet != null && !this.nonCriticalSet.isEmpty()) {
            Object[] arrayNonCriticalSet = this.nonCriticalSet.values().toArray();
            for (int i = 0; i < arrayNonCriticalSet.length; i++) {
                workers.add((Worker<? extends WorkerResourceDescription>) arrayNonCriticalSet[i]);
            }
        }

        return workers;
    }

    /**
     * Returns whether the given reduction can be removed without affecting the critical set or not.
     * 
     * @param slotReductionImpls Reduction to perform.
     * @return {@literal true} if the given reduction can be removed without affecting the critical set,
     *         {@literal false} otherwise.
     */
    public boolean isCriticalRemovalSafe(int[][] slotReductionImpls) {
        int coreCount = CoreManager.getCoreCount();
        // Compute cores from impl
        int[] slotReductionCores = new int[coreCount];
        for (int coreId = 0; coreId < coreCount; ++coreId) {
            for (int implId = 0; implId < CoreManager.getNumberCoreImplementations(coreId); ++implId) {
                if (slotReductionImpls[coreId][implId] > slotReductionCores[coreId]) {
                    slotReductionCores[coreId] = slotReductionImpls[coreId][implId];
                }
            }
        }

        int[] slots = new int[coreCount];
        for (DynamicMethodWorker r : this.criticalSet.values()) {
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

    /**
     * Dumps the current resources state.
     * 
     * @param prefix String prefix.
     * @return String containing the dump of the current resources state.
     */
    public String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        // Resources
        sb.append(prefix).append("RESOURCES = [").append("\n");
        for (Worker<? extends WorkerResourceDescription> r : staticSet.values()) {
            sb.append(prefix).append("\t").append("RESOURCE = [").append("\n");
            sb.append(r.getResourceLinks(prefix + "\t\t")); // Adds resource information
            sb.append(prefix).append("\t").append("\t").append("SET = Static").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        for (DynamicMethodWorker r : criticalSet.values()) {
            sb.append(prefix).append("\t").append("RESOURCE = [").append("\n");
            sb.append(r.getResourceLinks(prefix + "\t\t")); // Adds resource information
            sb.append(prefix).append("\t").append("\t").append("SET = Critical").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        for (DynamicMethodWorker r : nonCriticalSet.values()) {
            sb.append(prefix).append("\t").append("RESOURCE = [").append("\n");
            sb.append(r.getResourceLinks(prefix + "\t\t")); // Adds resource information
            sb.append(prefix).append("\t").append("\t").append("SET = Non-Critical").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("]").append("\n");

        return sb.toString();
    }

}
