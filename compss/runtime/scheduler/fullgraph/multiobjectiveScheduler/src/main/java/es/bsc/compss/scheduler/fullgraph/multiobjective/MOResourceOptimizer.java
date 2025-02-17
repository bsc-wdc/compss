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
package es.bsc.compss.scheduler.fullgraph.multiobjective;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.fullgraph.multiobjective.config.MOConfiguration;
import es.bsc.compss.scheduler.fullgraph.multiobjective.types.MOProfile;
import es.bsc.compss.scheduler.fullgraph.multiobjective.types.MOScore;
import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.scheduler.types.WorkloadState;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.types.resources.updates.ResourceUpdate;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.util.ResourceOptimizer;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;


public class MOResourceOptimizer extends ResourceOptimizer {

    private final long initialTimeStamp = System.currentTimeMillis();
    private StringBuilder log = new StringBuilder(
        "-------------------------\n" + "    CHECK SCALABILITY    \n" + "-------------------------\n");


    /**
     * Creates a new MOResourceOptimizer instance.
     * 
     * @param ts Associated MOScheduler.
     */
    public MOResourceOptimizer(MOScheduler ts) {
        super(ts);
    }

    @Override
    protected CloudTypeProfile generateCloudTypeProfile(JSONObject citdJSON, JSONObject implsJSON) {
        return new MOCloudTypeProfile(citdJSON, implsJSON);
    }

    @Override
    protected void applyPolicies(WorkloadState workload) {
        double timeBoundary = MOConfiguration.getTimeBoundary();
        double energyBoundary = MOConfiguration.getEnergyBoundary();
        double costBoundary = MOConfiguration.getMonetaryBoundary();
        double powerBoundary = MOConfiguration.getPowerBoundary();
        double priceBoundary = MOConfiguration.getPriceBoundary();

        double[] elapsedTime = new double[1];
        double[] elapsedEnergy = new double[1];
        double[] elapsedCost = new double[1];
        double[] elapsedPower = new double[1];
        double[] elapsedPrice = new double[1];

        Collection<ResourceScheduler<? extends WorkerResourceDescription>> workers = this.ts.getWorkers();
        List<ResourceCreationRequest> creations = ResourceManager.getPendingCreationRequests();
        Resource<?>[] allResources = new Resource[workers.size() + creations.size()];
        int[] load = new int[workload.getCoreCount()];

        HashMap<CloudInstanceTypeDescription, Integer> pendingCreations = new HashMap<>();
        HashMap<CloudInstanceTypeDescription, Integer> pendingReductions = new HashMap<>();

        final ConfigurationCost actualCost = getContext(allResources, load, workers, creations, pendingCreations,
            pendingReductions, elapsedTime, elapsedEnergy, elapsedCost, elapsedPower, elapsedPrice);

        addToLog(
            "Boundaries\n" + "\tTime: " + timeBoundary + "s\n" + "\tEnergy: " + energyBoundary + "Wh\n" + "\tCost: "
                + costBoundary + "€\n" + "\tPower: " + powerBoundary + "W\n" + "\tPrice: " + priceBoundary + "€/h\n");

        addToLog("Elapsed\n" + "\tTime: " + elapsedTime[0] + "s\n" + "\tEnergy: " + elapsedEnergy[0] + "Wh\n"
            + "\tCost: " + elapsedCost[0] + "€\n" + "\tPower: " + elapsedPower[0] + "W\n" + "\tPrice: "
            + elapsedPrice[0] + "€/h\n");

        double timeBudget = timeBoundary - elapsedTime[0];
        double energyBudget = energyBoundary - elapsedEnergy[0];
        double costBudget = costBoundary - elapsedCost[0];
        double powerBudget = powerBoundary - elapsedPower[0];
        double priceBudget = priceBoundary - elapsedPrice[0];
        addToLog("Budget\n" + "\tTime: " + timeBudget + "s\n" + "\tEnergy: " + energyBudget + "Wh - "
            + (energyBudget * 3600) + "J\n" + "\tCost: " + costBudget + "€\n" + "\tPower: " + powerBudget + "W\n"
            + "\tPrice: " + priceBudget + "€/h\n");

        addToLog("Current Resources\n");
        addToLog("Workload Info:\n");
        for (int coreId = 0; coreId < workload.getCoreCount(); coreId++) {
            addToLog("\tCore " + coreId + ": " + load[coreId] + "\n");
        }

        Action actualAction = new Action(actualCost);
        addToLog("Actual Cost:\n");
        addToLog(actualAction.toString());

        ConfigurationCost simCost = simulate(load, allResources, 0, 0, 0);
        addToLog("Actual Simulated:\n");
        Action currentSim = new Action(simCost);
        addToLog(currentSim.toString());

        LinkedList<Action> actions = generatePossibleActions(allResources, load);
        Action action =
            this.selectBestAction(currentSim, actions, timeBudget, energyBudget, costBudget, powerBudget, priceBudget);
        addToLog("Action to perform: " + action.title + "\n");
        printLog();
        action.perform();
    }

    private LinkedList<Action> generatePossibleActions(Resource<?>[] allResources, int[] load) {
        LinkedList<Action> actions = new LinkedList<>();

        // Generate all possible resource acquisitions
        if (ResourceManager.getCurrentVMCount() < ResourceManager.getMaxCloudVMs()) {
            generatePossibleResourceAcquisitions(actions, allResources, load);
        }

        // Generate all possible resource releases
        if (ResourceManager.getCurrentVMCount() > ResourceManager.getMinCloudVMs()) {
            generatePossibleResourceReleases(actions, allResources, load);
        }
        return actions;
    }

    private void generatePossibleResourceAcquisitions(LinkedList<Action> actions, Resource<?>[] allResources,
        int[] load) {
        for (CloudProvider cp : ResourceManager.getAvailableCloudProviders()) {
            if (!cp.canHostMoreInstances()) {
                continue;
            }
            for (CloudInstanceTypeDescription citd : cp.getAllTypes()) {
                for (CloudImageDescription cid : cp.getAllImages()) {
                    Resource<?>[] resources = new Resource[allResources.length + 1];
                    System.arraycopy(allResources, 0, resources, 0, allResources.length);
                    resources[allResources.length] = createResourceForComponent(citd, cid);
                    ConfigurationCost cc = simulate(load, resources, 0, 0, 0);
                    Action a = new ActionAdd(cp, citd, cid, cc);
                    addToLog(a.toString());
                    actions.add(a);
                }
            }

        }
    }

    private void generatePossibleResourceReleases(LinkedList<Action> actions, Resource<?>[] allResources, int[] load) {
        for (int i = 0; i < allResources.length; i++) {
            Resource<?> excludedWorker = allResources[i];
            Worker<?> w = excludedWorker.getResource();
            // If worker is null, worker is being created. It cannot be destroyed yet.
            if (w == null || !(w.getDescription() instanceof CloudMethodResourceDescription)) {
                continue;
            }
            CloudMethodResourceDescription description = (CloudMethodResourceDescription) w.getDescription();
            if (!(excludedWorker.hasPendingModifications())) {
                CloudImageDescription image = description.getImage();
                for (CloudInstanceTypeDescription typeReduction : description.getPossibleReductions()) {
                    CloudMethodResourceDescription reductionDescription =
                        new CloudMethodResourceDescription(typeReduction, image);
                    CloudMethodResourceDescription reducedDescription = new CloudMethodResourceDescription(description);
                    reducedDescription.reduce(reductionDescription);
                    ConfigurationCost cc;
                    if (reducedDescription.getTypeComposition().isEmpty()) {
                        Resource<?>[] resources = new Resource[allResources.length - 1];
                        System.arraycopy(allResources, 0, resources, 0, i);
                        System.arraycopy(allResources, i + 1, resources, i, resources.length - i);
                        long time = excludedWorker.startTime;
                        double energy = excludedWorker.idlePower * time + excludedWorker.startEnergy;
                        double cost = excludedWorker.startCost;
                        cc = simulate(load, resources, time, energy, cost);
                    } else {
                        allResources[i] = reduceResourceForComponent(excludedWorker, reducedDescription);
                        long time = excludedWorker.startTime;
                        double energy = excludedWorker.idlePower * time + excludedWorker.startEnergy;
                        double cost = excludedWorker.startCost;
                        cc = simulate(load, allResources, time, energy, cost);
                        allResources[i] = excludedWorker;
                    }
                    Action a = new ActionRemove(excludedWorker, typeReduction, cc);
                    addToLog(a.toString());
                    actions.add(a);
                }
            }
        }
    }

    private Action selectBestAction(Action currentAction, LinkedList<Action> candidates, double timeBudget,
        double energyBudget, double costBudget, double powerBudget, double priceBudget) {

        addToLog("SELECTING BEST ACTION ACCORDING TO " + MOConfiguration.getSchedulerOptimization() + "\n");
        Action bestAction = currentAction;
        for (Action action : candidates) {

            boolean improves = false;
            if (action.cost.power > powerBudget || action.cost.price > priceBudget) {
                addToLog("\t\t Surpasses the power (" + action.cost.power + ">" + powerBudget + ") or price budget ("
                    + action.cost.price + ">" + priceBudget + ")");
            } else {
                addToLog("\tChecking " + action.title + "\n");
                switch (MOConfiguration.getSchedulerOptimization()) {
                    case TIME:
                        improves = doesImproveTime(action, bestAction, energyBudget, costBudget);
                        break;
                    case COST:
                        improves = doesImproveCost(action, bestAction, energyBudget, timeBudget);
                        break;
                    case ENERGY:
                        improves = doesImproveEnergy(action, bestAction, timeBudget, costBudget);
                        break;
                    default:
                        // UNKNOWN: DO NOTHING!!!
                }
            }
            if (improves) {
                addToLog("\t\t" + action.title + " becomes the preferred option\n");
                bestAction = action;
            } else {
                addToLog("\t\t" + action.title + " does not improve " + bestAction.title + "\n");
            }
        }
        return bestAction;
    }

    private static <T extends Comparable<T>> boolean isAcceptable(T candidate, T reference, T budget) {
        if (reference.compareTo(budget) > 0) {
            return candidate.compareTo(reference) <= 0;
        } else {
            return candidate.compareTo(budget) <= 0;
        }
    }

    private boolean doesImproveTime(Action candidate, Action reference, double energyBudget, double costBudget) {
        ConfigurationCost cCost = candidate.cost;
        ConfigurationCost rCost = reference.cost;
        if (cCost.time < rCost.time) {
            if (!isAcceptable(cCost.energy, rCost.energy, energyBudget)) {
                addToLog("\t\t Surpasses the energy budget\n");
            }
            if (!isAcceptable(cCost.cost, rCost.cost, costBudget)) {
                addToLog("\t\t Surpasses the cost budget\n");
            }
            return isAcceptable(cCost.energy, rCost.energy, energyBudget)
                && isAcceptable(cCost.cost, rCost.cost, costBudget);
        } else if (cCost.time == rCost.time) {
            if (cCost.energy < rCost.energy) {
                return isAcceptable(cCost.cost, rCost.cost, costBudget);
            } else if (cCost.energy == rCost.energy) {
                return cCost.cost < rCost.cost;
            } else {
                addToLog("\t\t Energy's higher than the currently selected option\n");
            }
        } else {
            if (rCost.energy > energyBudget && cCost.energy < rCost.energy) {
                if (isAcceptable(cCost.cost, rCost.cost, costBudget)) {
                    addToLog("\t\t Time's higher than the currently selected option."
                        + " But energy is closer to the boundary\n");
                }
                return isAcceptable(cCost.cost, rCost.cost, costBudget);
            }

            if (rCost.cost > costBudget && cCost.cost < rCost.cost) {
                if (isAcceptable(cCost.energy, rCost.energy, costBudget)) {
                    addToLog("\t\t Time's higher than the currently selected option."
                        + " But cost is closer to the boundary\n");
                }
                return isAcceptable(cCost.energy, rCost.energy, costBudget);
            }
            addToLog("\t\t Time's higher than the currently selected option\n");

        }
        return false;
    }

    private boolean doesImproveCost(Action candidate, Action reference, double energyBudget, double timeBudget) {
        ConfigurationCost cCost = candidate.cost;
        ConfigurationCost rCost = reference.cost;
        if (cCost.cost < rCost.cost) {
            if (!isAcceptable(cCost.energy, rCost.energy, energyBudget)) {
                addToLog("\t\t Surpasses the energy budget " + cCost.energy + " > " + energyBudget + "\n");
            }
            if (!isAcceptable(cCost.time, rCost.time, timeBudget)) {
                addToLog("\t\t Surpasses the time budget " + cCost.time + " > " + timeBudget + "\n");
            }
            return isAcceptable(cCost.energy, rCost.energy, energyBudget)
                && isAcceptable(cCost.time, rCost.time, timeBudget);
        } else if (cCost.cost == rCost.cost) {
            if (cCost.time < rCost.time) {
                return isAcceptable(cCost.energy, rCost.energy, energyBudget);
            } else if (cCost.time == rCost.time) {
                return cCost.energy < rCost.energy;
            } else {
                addToLog("\t\t Time's higher than the currently selected option\n");
            }
        } else {
            if (rCost.time > timeBudget && cCost.time < rCost.time) {
                if (isAcceptable(cCost.energy, rCost.energy, energyBudget)) {
                    addToLog("\t\t Cost's higher than the currently selected option."
                        + " But time is closer to the boundary\n");
                }
                return isAcceptable(cCost.cost, rCost.cost, energyBudget);
            }
            if (rCost.energy > energyBudget && cCost.energy < rCost.energy) {
                if (isAcceptable(cCost.time, rCost.time, timeBudget)) {
                    addToLog("\t\t Energy's higher than the currently selected option."
                        + " But energy is closer to the boundary\n");
                }
                return isAcceptable(cCost.time, rCost.time, timeBudget);
            }
            addToLog("\t\t Cost's higher than the currently selected option\n");
        }
        return false;
    }

    private boolean doesImproveEnergy(Action candidate, Action reference, double timeBudget, double costBudget) {
        ConfigurationCost cCost = candidate.cost;
        ConfigurationCost rCost = reference.cost;
        if (cCost.energy < rCost.energy) {
            if (!isAcceptable(cCost.time, rCost.time, timeBudget)) {
                addToLog("\t\t Surpasses the time budget\n");
            }
            if (!isAcceptable(cCost.cost, rCost.cost, costBudget)) {
                addToLog("\t\t Surpasses the cost budget\n");
            }
            return isAcceptable(cCost.time, rCost.time, timeBudget) && isAcceptable(cCost.cost, rCost.cost, costBudget);
        } else if (cCost.energy == rCost.energy) {
            if (cCost.time < rCost.time) {
                return isAcceptable(cCost.cost, rCost.cost, costBudget);
            } else if (cCost.time == rCost.time) {
                return cCost.cost < rCost.cost;
            } else {
                addToLog("\t\t Time's higher than the currently selected option\n");
            }
        } else {
            if (rCost.time > timeBudget && cCost.time < rCost.time) {
                if (isAcceptable(cCost.cost, rCost.cost, costBudget)) {
                    addToLog("\t\t Energy's higher than the currently selected option."
                        + " But time is closer to the boundary\n");
                }
                return isAcceptable(cCost.cost, rCost.cost, costBudget);
            }
            if (rCost.cost > costBudget && cCost.cost < rCost.cost) {
                if (isAcceptable(cCost.time, rCost.time, timeBudget)) {
                    addToLog("\t\t Energy's higher than the currently selected option."
                        + " But time is closer to the boundary\n");
                }
                return isAcceptable(cCost.time, rCost.time, timeBudget);
            }
            addToLog("\t\t Energy's higher than the currently selected option.\n");
        }
        return false;
    }

    // Get estimated cost (energy, price, time) of the current scheduling
    private <T extends WorkerResourceDescription> ConfigurationCost getContext(Resource<?>[] allResources, int[] load,
        Collection<ResourceScheduler<? extends WorkerResourceDescription>> workers,
        List<ResourceCreationRequest> creations, HashMap<CloudInstanceTypeDescription, Integer> pendingCreations,
        HashMap<CloudInstanceTypeDescription, Integer> pendingDestructions, double[] elapsedTime,
        double[] elapsedEnergy, double[] elapsedCost, double[] elapsedPower, double[] elapsedPrice) {

        elapsedTime[0] = 0;
        elapsedEnergy[0] = 0;
        elapsedCost[0] = 0;
        elapsedPower[0] = 0;
        elapsedPrice[0] = 0;

        double time = 0;
        double actionsCost = 0;
        double idlePrice = 0;
        double actionsEnergy = 0;
        double idlePower = 0;
        int resourceId = 0;
        for (ResourceScheduler<?> w : workers) {
            @SuppressWarnings("unchecked")
            MOResourceScheduler<T> aw = (MOResourceScheduler<T>) w;
            Resource<T> r = new Resource<>(aw);
            allResources[resourceId] = r;

            addToLog("\tName:" + aw.getName() + (r.hasPendingModifications() ? " (IS TO BE DELETED)" : "") + "\n");

            time = Math.max(time, aw.getLastGapExpectedStart());
            addToLog("\t\tTime:" + aw.getLastGapExpectedStart() + " ms -> total " + time + "\n");

            elapsedCost[0] += aw.getRunActionsCost();
            addToLog("\t\tExecuted Actions Cost:" + aw.getRunActionsCost() + " €.ms/h -> total " + elapsedCost[0]
                + "€.ms/h\n");

            actionsCost += aw.getScheduledActionsCost();
            addToLog("\t\tScheduled Actions Cost:" + aw.getScheduledActionsCost() + " €.ms/h -> total " + actionsCost
                + "€.ms/h\n");

            r.idlePrice = aw.getIdlePrice();
            idlePrice += r.idlePrice;
            addToLog("\t\tIdle Price:" + r.idlePrice + " €/h -> total " + idlePrice + "€/h\n");

            elapsedEnergy[0] += aw.getRunActionsEnergy();
            addToLog("\t\tExecuted Actions Energy:" + aw.getRunActionsEnergy() + " mJ -> total " + elapsedEnergy[0]
                + "mJ\n");

            actionsEnergy += aw.getScheduledActionsEnergy();
            addToLog("\t\tScheduled Actions Energy:" + aw.getScheduledActionsEnergy() + " mJ -> total " + actionsEnergy
                + "mJ\n");

            r.idlePower = aw.getIdlePower();
            idlePower += r.idlePower;
            addToLog("\t\tIdle Power:" + r.idlePower + " W -> total " + idlePower + "W\n");

            r.startTime = aw.getExpectedEndTimeRunning();
            r.startCost = aw.getRunningActionsCost();
            r.startEnergy = aw.getRunningActionsEnergy();

            int[][] implsCount = aw.getImplementationCounts();
            int[][] runningCounts = aw.getRunningImplementationCounts();
            addToLog("\t\tCore Information:\n");
            StringBuilder[] coreInfo = new StringBuilder[CoreManager.getCoreCount()];
            Implementation[] impls = new Implementation[CoreManager.getCoreCount()];
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                coreInfo[coreId] = new StringBuilder("\t\t\tCore " + coreId + "\n");
                int favId = 0;
                int favCount = implsCount[coreId][0];
                load[coreId] += implsCount[coreId][0] - runningCounts[coreId][0];
                coreInfo[coreId].append("\t\t\t\tImplementation 0: " + implsCount[coreId][0] + ", "
                    + runningCounts[coreId][0] + " of'em already running\n");
                for (int implId = 1; implId < CoreManager.getCoreImplementations(coreId).size(); implId++) {
                    coreInfo[coreId].append("\t\t\t\tImplementation " + implId + ": " + implsCount[coreId][implId]
                        + ", " + runningCounts[coreId][implId] + " of'em already running\n");
                    load[coreId] += implsCount[coreId][implId] - runningCounts[coreId][implId];
                    if (implsCount[coreId][implId] > favCount) {
                        favId = implId;
                    }
                }
                if (favCount > 0) {
                    impls[coreId] = CoreManager.getCoreImplementations(coreId).get(favId);
                } else {
                    List<Implementation> coreImpls = CoreManager.getCoreImplementations(coreId);
                    MOProfile[] profiles = new MOProfile[coreImpls.size()];
                    for (int i = 0; i < profiles.length; i++) {
                        profiles[i] = (MOProfile) aw.getProfile(coreImpls.get(i));
                    }
                    impls[coreId] = getBestImplementation(coreImpls, profiles);
                }
                coreInfo[coreId].append("\t\t\t\tFavorite Implementation " + favId + "\n");
            }

            r.profiles = new MOProfile[implsCount.length];
            r.capacity = new int[implsCount.length];
            for (int coreId = 0; coreId < implsCount.length; coreId++) {
                r.profiles[coreId] = (MOProfile) aw.getProfile(impls[coreId]);
                coreInfo[coreId].append("\t\t\t\tProfile " + r.profiles[coreId] + "\n");
                r.capacity[coreId] = aw.getSimultaneousCapacity(impls[coreId]);
                coreInfo[coreId].append("\t\t\t\tCapacity " + r.capacity[coreId] + "\n");
                addToLog(coreInfo[coreId].toString());
            }

            if (r.hasPendingModifications()) {
                for (ResourceUpdate<T> ru : r.getPendingModifications()) {
                    Map<CloudInstanceTypeDescription, int[]> modificationComposition =
                        ((CloudMethodResourceDescription) ru.getModification()).getTypeComposition();
                    for (Map.Entry<CloudInstanceTypeDescription, int[]> entry : modificationComposition.entrySet()) {
                        CloudInstanceTypeDescription componentType = entry.getKey();
                        int count = entry.getValue()[0];
                        Integer pendingDestruction = pendingDestructions.get(componentType);
                        if (pendingDestruction == null) {
                            pendingDestruction = 0;
                        }
                        pendingDestruction += count;
                        pendingDestructions.put(componentType, pendingDestruction);
                    }
                }
            }
            resourceId++;
        }
        // Convert time to secs
        elapsedTime[0] = (double) (System.currentTimeMillis() - initialTimeStamp) / 1000;
        // Convert energy from mJ to Wh
        elapsedEnergy[0] = elapsedEnergy[0] / 3_600_000;
        // Convert energy from €ms/h to €
        elapsedCost[0] = elapsedCost[0] / 3_600_000;
        for (ResourceCreationRequest rcr : creations) {
            for (Map.Entry<CloudInstanceTypeDescription, int[]> entry : rcr.getRequested().getTypeComposition()
                .entrySet()) {
                CloudInstanceTypeDescription componentType = entry.getKey();
                int count = entry.getValue()[0];
                addToLog("\tName: REQUESTED " + componentType.getName() + "\n");

                Integer pendingCreation = pendingCreations.get(componentType);
                if (pendingCreation == null) {
                    pendingCreation = 0;
                }
                pendingCreation += count;
                pendingCreations.put(componentType, pendingCreation);
            }

            Resource<?> r = createResourceForCreationRequest(rcr);
            allResources[resourceId] = r;

            addToLog("\t\tTime: 0 ms -> total " + time + "\n");
            addToLog("\t\tactions Cost: 0 € -> total " + actionsCost + "€\n");
            idlePrice += r.idlePrice;
            addToLog("\t\tIdle Price:" + r.idlePrice + " € -> total " + idlePrice + "€\n");
            addToLog("\t\tactions Energy:0 mJ -> total " + actionsEnergy + "mJ\n");
            idlePower += r.idlePower;
            addToLog("\t\tIdle Power:" + r.idlePower + " W -> total " + idlePower + "W\n");

            // r.startTime = 0;
            r.startCost = 0;
            r.startEnergy = 0;

            addToLog("\t\tCore Information:\n");
            StringBuilder[] coreInfo = new StringBuilder[CoreManager.getCoreCount()];
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                coreInfo[coreId] = new StringBuilder("\t\t\tCore " + coreId + "\n");
                load[coreId] += 0;
                coreInfo[coreId].append("\t\t\t\tImplementation 0: 0, 0 of'em already running\n");
                for (int implId = 1; implId < CoreManager.getCoreImplementations(coreId).size(); implId++) {
                    coreInfo[coreId].append("\t\t\t\tImplementation " + implId + ": 0, 0 of'em already running\n");
                }
                coreInfo[coreId].append("\t\t\t\tFavorite Implementation 0\n");
            }

            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                coreInfo[coreId].append("\t\t\t\tProfile " + r.profiles[coreId] + "\n");
                coreInfo[coreId].append("\t\t\t\tCapacity " + r.capacity[coreId] + "\n");
                addToLog(coreInfo[coreId].toString());
            }

            resourceId++;
        }

        return new ConfigurationCost(time, idlePower, actionsEnergy, idlePrice, actionsCost);
    }

    private ConfigurationCost simulate(int[] counts, Resource<?>[] resources, long minTime, double minEnergy,
        double minCost) {
        // addToLog("Simulation\n");
        int[] workingCounts = new int[counts.length];
        System.arraycopy(counts, 0, workingCounts, 0, counts.length);
        LinkedList<Resource<?>> executable = new LinkedList<>();
        for (Resource<?> r : resources) {
            r.clear();
            if (r.canExecute()) {
                executable.add(r);
            }
        }

        SortedList sl = new SortedList(executable.size());
        for (Resource<?> r : executable) {
            sl.initialAdd(r);
        }

        for (int coreId = 0; coreId < workingCounts.length; coreId++) {
            while (workingCounts[coreId] > 0) {
                // Pressumes that all CE runs in every resource
                Resource<?> r = sl.peek();
                r.time += r.profiles[coreId].getAverageExecutionTime();
                r.counts[coreId] += Math.min(r.capacity[coreId], workingCounts[coreId]);
                workingCounts[coreId] -= r.capacity[coreId];
                sl.add(r);
            }
        }

        // Summary Execution
        long time = minTime;
        double idlePower = 0;
        double actionsEnergy = 0;
        double idlePrice = 0;
        double actionsCost = 0;
        for (Resource<?> r : resources) {
            double rActionsEnergy = r.startEnergy;
            double rActionsCost = r.startCost;
            // addToLog("\t" + (r.worker != null ? r.getName() : " NEW") + "\n");
            time = Math.max(time, r.time);
            idlePower += r.idlePower;
            idlePrice += r.idlePrice;

            for (int coreId = 0; coreId < r.counts.length; coreId++) {
                rActionsEnergy +=
                    r.counts[coreId] * r.profiles[coreId].getPower() * r.profiles[coreId].getAverageExecutionTime();
                rActionsCost +=
                    r.counts[coreId] * r.profiles[coreId].getPrice() * r.profiles[coreId].getAverageExecutionTime();
            }
            actionsEnergy += rActionsEnergy;
            actionsCost += rActionsCost;
            // addToLog("\t\t Time: " + time + "ms\n");
            // addToLog("\t\tactions Cost:" + rActionsCost + " € -> total " + actionsCost + "€\n");
            // addToLog("\t\tIdle Price:" + r.idlePrice + " €/h -> total " + idlePrice + "€/h\n");
            // addToLog("\t\tactions Energy:" + rActionsEnergy + " mJ -> total " + actionsEnergy + "mJ\n");
            // addToLog("\t\tIdle Power:" + r.idlePower + " W -> total " + idlePower + "W\n");

        }
        return new ConfigurationCost(time, idlePower, actionsEnergy + minEnergy, idlePrice, actionsCost + minCost);
    }

    private Resource<?> reduceResourceForComponent(Resource<?> excludedWorker,
        CloudMethodResourceDescription reduction) {
        Resource<?> clone = new Resource<>(excludedWorker.worker);
        clone.idlePower = excludedWorker.idlePower;
        clone.idlePrice = excludedWorker.idlePrice;
        clone.capacity = new int[excludedWorker.capacity.length];
        System.arraycopy(excludedWorker.capacity, 0, clone.capacity, 0, excludedWorker.capacity.length);
        clone.startTime = excludedWorker.startTime;
        clone.startEnergy = excludedWorker.startEnergy;
        clone.startCost = excludedWorker.startCost;
        clone.time = excludedWorker.time;
        clone.counts = excludedWorker.counts;
        Map<CloudInstanceTypeDescription, int[]> composition = reduction.getTypeComposition();
        for (Map.Entry<CloudInstanceTypeDescription, int[]> component : composition.entrySet()) {
            CloudInstanceTypeDescription type = component.getKey();
            int count = component.getValue()[0];
            MethodResourceDescription rd = type.getResourceDescription();
            MOCloudTypeProfile moCloudTypeProf = (MOCloudTypeProfile) getCloudTypeProfile(type);
            clone.idlePower -= moCloudTypeProf.getIdlePower() * count;
            clone.idlePrice -= moCloudTypeProf.getIdlePrice() * count;
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
                MOProfile[] profiles = new MOProfile[impls.size()];
                for (int i = 0; i < impls.size(); i++) {
                    profiles[i] =
                        (MOProfile) moCloudTypeProf.getImplProfiles(coreId, impls.get(i).getImplementationId());
                }
                Implementation impl = getBestImplementation(impls, profiles);
                clone.capacity[coreId] -=
                    rd.canHostSimultaneously((MethodResourceDescription) impl.getRequirements()) * count;
            }
        }
        return clone;
    }

    private Resource<?> createResourceForCreationRequest(ResourceCreationRequest rcr) {
        CloudMethodResourceDescription cmrd = rcr.getRequested();
        Resource<?> r = new Resource<>(null);
        Map<CloudInstanceTypeDescription, int[]> composition = cmrd.getTypeComposition();
        r.capacity = new int[CoreManager.getCoreCount()];
        r.profiles = new MOProfile[CoreManager.getCoreCount()];

        for (Map.Entry<CloudInstanceTypeDescription, int[]> component : composition.entrySet()) {
            CloudInstanceTypeDescription type = component.getKey();
            int count = component.getValue()[0];
            MethodResourceDescription rd = type.getResourceDescription();
            MOCloudTypeProfile moCloudTypeProf = (MOCloudTypeProfile) getCloudTypeProfile(type);
            r.idlePower += moCloudTypeProf.getIdlePower() * count;
            r.idlePrice += moCloudTypeProf.getIdlePrice() * count;

            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
                MOProfile[] profiles = new MOProfile[impls.size()];
                for (int i = 0; i < impls.size(); i++) {
                    profiles[i] =
                        (MOProfile) moCloudTypeProf.getImplProfiles(coreId, impls.get(i).getImplementationId());
                }
                Implementation impl = getBestImplementation(impls, profiles);
                r.capacity[coreId] +=
                    rd.canHostSimultaneously((MethodResourceDescription) impl.getRequirements()) * count;
                MOProfile bestImplProf =
                    (MOProfile) moCloudTypeProf.getImplProfiles(coreId, impl.getImplementationId());
                if (r.profiles[coreId] == null) {
                    r.profiles[coreId] = bestImplProf;
                } else {
                    r.profiles[coreId].accumulate(bestImplProf);
                }
            }
        }
        r.startTime =
            (cmrd.getImage().getCreationTime() * 1000) - (System.currentTimeMillis() - rcr.getRequestedTime());
        if (r.startTime < 0) {
            r.startTime = 0;
        }
        r.clear();
        return r;
    }

    private Resource<?> createResourceForComponent(CloudInstanceTypeDescription citd, CloudImageDescription cid) {
        Resource<?> r = new Resource<>(null);
        MOCloudTypeProfile moCloudTypeProf = (MOCloudTypeProfile) getCloudTypeProfile(citd);
        r.idlePower = moCloudTypeProf.getIdlePower();
        r.idlePrice = moCloudTypeProf.getIdlePrice();
        MethodResourceDescription rd = citd.getResourceDescription();
        r.capacity = new int[CoreManager.getCoreCount()];
        r.profiles = new MOProfile[CoreManager.getCoreCount()];
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            MOProfile[] profiles = new MOProfile[impls.size()];
            for (int i = 0; i < impls.size(); i++) {
                profiles[i] = (MOProfile) moCloudTypeProf.getImplProfiles(coreId, impls.get(i).getImplementationId());
            }
            Implementation impl = getBestImplementation(impls, profiles);
            r.capacity[coreId] = rd.canHostSimultaneously((MethodResourceDescription) impl.getRequirements());
            r.profiles[coreId] = (MOProfile) moCloudTypeProf.getImplProfiles(coreId, impl.getImplementationId());
        }
        r.startTime = cid.getCreationTime() * 1000;
        r.clear();
        return r;
    }

    private Implementation getBestImplementation(List<Implementation> impls, MOProfile[] profiles) {
        Implementation impl = impls.get(0);
        MOScore bestScore = new MOScore(0, 0, 0, 0, profiles[0].getAverageExecutionTime(), profiles[0].getPower(),
            profiles[0].getPrice());
        for (int i = 1; i < impls.size(); i++) {
            Implementation candidate = impls.get(i);
            long length = profiles[i].getAverageExecutionTime();
            double power = profiles[i].getPower();
            double price = profiles[i].getPrice();
            MOScore score = new MOScore(0, 0, 0, 0, length, power * length, price);
            if (Score.isBetter(score, bestScore)) {
                bestScore = score;
                impl = candidate;
            }
        }
        return impl;
    }

    /*
     * LOG OPPERATIONS
     */

    private void addToLog(String s) {
        log.append(s);
    }

    private void printLog() {
        System.out.println(log.toString() + "\n" + "-----------------------------\n");
        log = new StringBuilder(
            "-------------------------\n" + "    CHECK SCALABILITY    \n" + "-------------------------\n");
    }

    /*
     * SUPPORTING PROFILE CLASSES
     */


    protected class MOCloudTypeProfile extends CloudTypeProfile {

        private final double idlePower;
        private final double idlePrice;


        public MOCloudTypeProfile(JSONObject typeJSON, JSONObject implsJSON) {
            super(typeJSON, implsJSON);

            double idlePower;
            double idlePrice;
            if (typeJSON != null) {
                try {
                    idlePower = typeJSON.getDouble("idlePower");
                } catch (JSONException je) {
                    idlePower = MOConfiguration.DEFAULT_IDLE_POWER;
                }

                try {
                    idlePrice = typeJSON.getDouble("idlePrice");
                } catch (JSONException je) {
                    idlePrice = MOConfiguration.DEFAULT_IDLE_PRICE;
                }
            } else {
                idlePower = MOConfiguration.DEFAULT_IDLE_POWER;
                idlePrice = MOConfiguration.DEFAULT_IDLE_PRICE;
            }
            this.idlePower = idlePower;
            this.idlePrice = idlePrice;
        }

        public double getIdlePower() {
            return this.idlePower;
        }

        public double getIdlePrice() {
            return this.idlePrice;
        }

        @Override
        protected Profile generateProfileForImplementation(Implementation impl, JSONObject jsonImpl) {
            return new MOProfile(jsonImpl);
        }

    }

    /*
     * SUPPORTING RESOURCE CLASSES
     */

    private class Resource<T extends WorkerResourceDescription> {

        private final MOResourceScheduler<T> worker;
        private double idlePower;
        private double idlePrice;
        private MOProfile[] profiles;
        private int[] capacity;
        private long startTime;
        private double startEnergy;
        private double startCost;
        private long time;
        private int[] counts;


        public Resource(MOResourceScheduler<T> worker) {
            this.worker = worker;
        }

        public void clear() {
            this.time = this.startTime;
            this.counts = new int[this.profiles.length];
        }

        private boolean canExecute() {
            if (this.worker != null) {
                return !this.worker.hasPendingModifications();
            }
            return true;
        }

        private boolean hasPendingModifications() {
            if (this.worker != null) {
                return this.worker.hasPendingModifications();
            }
            return true;
        }

        private List<ResourceUpdate<T>> getPendingModifications() {
            if (this.worker != null) {
                return this.worker.getPendingModifications();
            }
            return new LinkedList<>();
        }

        private Worker<?> getResource() {
            if (this.worker != null) {
                return this.worker.getResource();
            }
            return null;
        }

        private String getName() {
            if (this.worker != null) {
                return this.worker.getName();
            }
            return "TEMPORARY";
        }

    }

    /*
     * SUPPORTING LIST CLASSES
     */

    private class SortedList {

        private final Resource<?>[] values;


        public SortedList(int size) {
            this.values = new Resource[size];
        }

        public void initialAdd(Resource<?> r) {
            for (int i = 0; i < this.values.length - 1; i++) {
                if (this.values[i + 1] != null && r.time < this.values[i + 1].time) {
                    this.values[i] = r;
                    return;
                } else {
                    this.values[i] = this.values[i + 1];
                }
            }
            this.values[this.values.length - 1] = r;
        }

        public void add(Resource<?> r) {
            for (int i = 0; i < values.length - 1; i++) {
                if (r.time < values[i + 1].time) {
                    values[i] = r;
                    return;
                } else {
                    values[i] = values[i + 1];
                }
            }
            values[values.length - 1] = r;
        }

        public Resource<?> peek() {
            return values[0];
        }
    }

    /*
     * SUPPORTING ACTION CLASSES
     */

    private class Action {

        protected final String title;
        private final ConfigurationCost cost;


        public Action(ConfigurationCost cost) {
            this.title = "Current Configuration";
            this.cost = cost;
        }

        public Action(String title, ConfigurationCost cost) {
            this.title = title;
            this.cost = cost;
        }

        @Override
        public String toString() {
            return this.title + ":\n" + this.cost.toString();
        }

        public void perform() {
            // Nothing to do since subclasses will override it
        }
    }

    private class ActionAdd extends Action {

        private final CloudProvider provider;
        private final CloudInstanceTypeDescription instance;
        private final CloudImageDescription image;


        public ActionAdd(CloudProvider provider, CloudInstanceTypeDescription component, CloudImageDescription image,
            ConfigurationCost cost) {
            super("Add " + component, cost);

            this.provider = provider;
            this.image = image;
            this.instance = component;
        }

        @Override
        public void perform() {
            RUNTIME_LOGGER.debug("[MOResourceOptimizer] Performing Add action " + this);
            CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(this.instance, this.image);
            this.provider.requestResourceCreation(cmrd, null);
        }
    }

    private class ActionRemove extends Action {

        private final Resource<?> res;
        private final CloudInstanceTypeDescription citd;


        public ActionRemove(Resource<?> res, CloudInstanceTypeDescription reduction, ConfigurationCost cost) {
            super("Remove " + res.getName(), cost);

            this.citd = reduction;
            this.res = res;
        }

        @Override
        public void perform() {
            RUNTIME_LOGGER.debug("[MOResourceOptimizer] Performing Remove action " + this);
            CloudMethodWorker worker = (CloudMethodWorker) this.res.getResource();
            CloudMethodResourceDescription reduction =
                new CloudMethodResourceDescription(this.citd, worker.getDescription().getImage());
            ResourceManager.requestWorkerReduction(worker, reduction);
        }
    }

    private class ConfigurationCost {

        private final double time; // secs
        private final double energy; // Wh
        private final double cost; // €
        private final double power; // W
        private final double price; // €/h


        public ConfigurationCost(double time, double idlePower, double fixedEnergy, double idlePrice,
            double fixedCost) {
            this.time = (double) (time / 1_000);
            this.energy = (double) ((idlePower * time + fixedEnergy) / 3_600_000);
            this.cost = (double) (((idlePrice * time) + fixedCost) / 3_600_000);
            this.power = idlePower + (double) (fixedEnergy / time);
            this.price = idlePrice + (double) (fixedCost / time);
        }

        @Override
        public String toString() {
            return "Configuration Cost:\n" + "\tTime: " + this.time + "s\n" + "\tEnergy: " + this.energy + "Wh\n"
                + "\tCost: " + this.cost + "€\n" + "\tPower: " + this.power + "W\n" + "\tPrice: " + this.price
                + "€/h\n";
        }
    }

}
