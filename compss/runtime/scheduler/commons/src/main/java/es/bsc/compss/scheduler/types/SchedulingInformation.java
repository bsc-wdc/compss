/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.CoreManager;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class SchedulingInformation {

    // List of active resources per core
    private static final List<List<ResourceScheduler<? extends WorkerResourceDescription>>> CORE_TO_WORKERS = new ArrayList<>();


    public static void updateCoreCount(int newCoreCount) {
        for (int currentCoreCount = CORE_TO_WORKERS.size(); currentCoreCount < newCoreCount; ++currentCoreCount) {
            // Add empty workers list to new core entry
            CORE_TO_WORKERS.add(new LinkedList<>());
        }
    }

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
            CORE_TO_WORKERS.get(coreId).add(ui);
        }
    }

    public static List<ResourceScheduler<? extends WorkerResourceDescription>> getCoreElementExecutors(int coreId) {
        List<ResourceScheduler<? extends WorkerResourceDescription>> res = new LinkedList<>();
        for (ResourceScheduler<? extends WorkerResourceDescription> rs : CORE_TO_WORKERS.get(coreId)) {
            res.add(rs);
        }
        return res;
    }


    // Execution Information
    private final List<AllocatableAction> constrainingPredecessors;
    // Resource execution information
    private final ResourceScheduler<? extends WorkerResourceDescription> enforcedTargetResource;


    public <T extends WorkerResourceDescription> SchedulingInformation(ResourceScheduler<T> enforcedTargetResource) {
        constrainingPredecessors = new LinkedList<>();
        this.enforcedTargetResource = enforcedTargetResource;
    }

    public final void addResourceConstraint(AllocatableAction predecessor) {
        constrainingPredecessors.add(predecessor);
    }

    public final List<AllocatableAction> getConstrainingPredecessors() {
        return constrainingPredecessors;
    }

    public boolean isExecutable() {
        return true;
    }

    public final ResourceScheduler<? extends WorkerResourceDescription> getEnforcedTargetResource() {
        return enforcedTargetResource;
    }

}
