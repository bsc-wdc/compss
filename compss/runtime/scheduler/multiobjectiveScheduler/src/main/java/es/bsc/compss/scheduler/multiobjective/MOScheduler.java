/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.multiobjective;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.multiobjective.config.MOConfiguration;
import es.bsc.compss.scheduler.multiobjective.types.MOProfile;
import es.bsc.compss.scheduler.multiobjective.types.MOScore;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.ResourceOptimizer;
import es.bsc.compss.util.SchedulingOptimizer;

import java.util.List;

import org.json.JSONObject;


public class MOScheduler extends TaskScheduler {

    public MOScheduler() {
        MOConfiguration.load();
    }

    @Override
    public MOProfile generateProfile(JSONObject json) {
        return new MOProfile(json);
    }

    @Override
    public <T extends WorkerResourceDescription> MOResourceScheduler<T> generateSchedulerForResource(Worker<T> w,
            JSONObject res, JSONObject impls) {
        // LOGGER.debug("[LoadBalancingScheduler] Generate scheduler for resource " + w.getName());
        return new MOResourceScheduler<>(w, res, impls);
    }

    @Override
    public <T extends WorkerResourceDescription> MOSchedulingInformation generateSchedulingInformation(
            ResourceScheduler<T> enforcedTargetResource) {
        return new MOSchedulingInformation(enforcedTargetResource);
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        return getActionScore(action);
    }

    public static MOScore getActionScore(AllocatableAction action) {
        long actionScore = MOScore.getActionScore(action);
        long dataTime = MOScore.getDataPredecessorTime(action.getDataPredecessors());
        return new MOScore(actionScore, dataTime, 0, 0, 0, 0);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        /*
         * Collection<ResourceScheduler<? extends WorkerResourceDescription>> workers = this.getWorkers();
         * System.out.println("End Profiles:"); for (ResourceScheduler<?> worker : workers) { System.out.println("\t" +
         * * worker.getName()); for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) { for
         * (Implementation impl : CoreManager.getCoreImplementations(coreId)) { System.out.println("\t\t" +
         * CoreManager.getSignature(coreId, impl.getImplementationId())); MOProfile profile = (MOProfile)
         * worker.getProfile(impl); System.out.println("\t\t\tTime " + profile.getAverageExecutionTime() + " ms");
         * System.out.println("\t\t\tPower " + profile.getPower() + " W"); System.out.println("\t\t\tCost " +
         * profile.getPrice() + " €"); } } }
         */
    }

    @Override
    public ResourceOptimizer generateResourceOptimizer() {
        return new MOResourceOptimizer(this);
    }

    @Override
    public SchedulingOptimizer generateSchedulingOptimizer() {
        return new MOScheduleOptimizer(this);
    }

    /**
     * Notifies to the scheduler that some actions have become free of data dependencies or resource dependencies.
     *
     * @param <T>
     * @param dataFreeActions IN, list of actions free of data dependencies
     * @param resourceFreeActions IN, list of actions free of resource dependencies
     * @param blockedCandidates OUT, list of blocked candidates
     * @param resource Resource where the previous task was executed
     */
    @Override
    public <T extends WorkerResourceDescription> void handleDependencyFreeActions(
            List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
            List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {
        LOGGER.debug("[MOScheduler] Treating dependency free actions on resource " + resource.getName());
        for (AllocatableAction freeAction : dataFreeActions) {
            tryToLaunch(freeAction);
        }
        for (AllocatableAction freeAction : resourceFreeActions) {
            tryToLaunch(freeAction);
        }
    }

}
