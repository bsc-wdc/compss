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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.CoreManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SchedulingInformation {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    // List of active resources per core
    private static final List<List<ResourceScheduler<? extends WorkerResourceDescription>>> CORE_TO_WORKERS;

    // Execution Information
    private final List<AllocatableAction> constrainingPredecessors;
    // Resource execution information
    private final ResourceScheduler<? extends WorkerResourceDescription> enforcedTargetResource;

    private Map<Resource, Double> perResourceScore;

    static {
        CORE_TO_WORKERS = new ArrayList<>();
    }


    /**
     * Clears all the information stored statically.
     */
    public static void clear() {
        CORE_TO_WORKERS.clear();
    }

    /**
     * Updates the coreCount information.
     * 
     * @param newCoreCount New coreCount value.
     */
    public static void updateCoreCount(int newCoreCount) {
        for (int currentCoreCount = CORE_TO_WORKERS.size(); currentCoreCount < newCoreCount; ++currentCoreCount) {
            // Add empty workers list to new core entry
            CORE_TO_WORKERS.add(new LinkedList<>());
        }
    }

    /**
     * Registers the modifications on the given worker.
     * 
     * @param ui Modified ResourceScheduler.
     */
    public static <T extends WorkerResourceDescription> void changesOnWorker(ResourceScheduler<T> ui) {
        // Remove the previous description of the worker
        for (List<ResourceScheduler<? extends WorkerResourceDescription>> coreToWorker : CORE_TO_WORKERS) {
            coreToWorker.remove(ui);
        }

        // Update registered coreElements
        SchedulingInformation.updateCoreCount(CoreManager.getCoreCount());

        // Add the new description of the worker
        List<Integer> executableCores = ui.getExecutableCores();
        for (int coreId : executableCores) {
            List<ResourceScheduler<? extends WorkerResourceDescription>> workersList = CORE_TO_WORKERS.get(coreId);
            workersList.add(ui);
        }
    }

    /**
     * Returns the executors of a given core element.
     * 
     * @param coreId Core Id.
     * @return ResourceSchedulers capable of executing the given core Id.
     */
    public static List<ResourceScheduler<? extends WorkerResourceDescription>> getCoreElementExecutors(int coreId) {
        List<ResourceScheduler<? extends WorkerResourceDescription>> res = new LinkedList<>();
        if (!CORE_TO_WORKERS.isEmpty()) {
            for (ResourceScheduler<? extends WorkerResourceDescription> rs : CORE_TO_WORKERS.get(coreId)) {
                res.add(rs);
            }
        }
        return res;
    }

    /**
     * Creates an empty Scheduling Information.
     */
    public SchedulingInformation() {
        this.constrainingPredecessors = new LinkedList<>();
        this.perResourceScore = new HashMap<>();
        this.enforcedTargetResource = null;
    }

    /**
     * Creates a new Scheduling Information instance.
     * 
     * @param enforcedTargetResource Enforced resource.
     */
    public <T extends WorkerResourceDescription> SchedulingInformation(ResourceScheduler<T> enforcedTargetResource) {
        this.constrainingPredecessors = new LinkedList<>();
        this.enforcedTargetResource = enforcedTargetResource;
        this.perResourceScore = new HashMap<>();
    }

    /**
     * Adds a new resource constraint.
     * 
     * @param predecessor Constraining AllocatableAction.
     */
    public final void addResourceConstraint(AllocatableAction predecessor) {
        this.constrainingPredecessors.add(predecessor);
    }

    /**
     * Returns the constraining predecessors.
     * 
     * @return A list of constraining predecessor AllocatableActions.
     */
    public final List<AllocatableAction> getConstrainingPredecessors() {
        return this.constrainingPredecessors;
    }

    /**
     * Returns whether the action is executable or not.
     * 
     * @return {@code true} if the action is executable, {@code false} otherwise.
     */
    public boolean isExecutable() {
        return true;
    }

    /**
     * Returns the enforced resource.
     * 
     * @return The enforced resource.
     */
    public final ResourceScheduler<? extends WorkerResourceDescription> getEnforcedTargetResource() {
        return this.enforcedTargetResource;
    }

    /**
     * Returns the score for a given resource where to run the action.
     * 
     * @param r candidate resource to execute the action
     * @return The score for the worker.
     */
    public double getPreregisteredScore(Resource r) {
        Double score = perResourceScore.get(r);
        if (score == null) {
            LOGGER.warn("The resource " + r.toString() + " is not registered");
            score = 0.00;
            perResourceScore.put(r, score);
        }
        return score;
    }

    /**
     * Increases the pre-registered score for a set of workers.
     * 
     * @param resources List of resources whose score should be increased
     * @param rise augmented score value
     */
    public void increasePreregisteredScores(Collection<Resource> resources, double rise) {
        for (Resource r : resources) {
            Double score = this.getPreregisteredScore(r) + rise;
            perResourceScore.put(r, score);
        }
    }

}
