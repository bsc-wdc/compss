package es.bsc.compss.scheduler.multiobjective;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.multiobjective.config.MOConfiguration;
import es.bsc.compss.scheduler.multiobjective.types.MOProfile;
import es.bsc.compss.scheduler.multiobjective.types.MOScore;
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
import es.bsc.compss.util.JSONStateManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.util.ResourceOptimizer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;

public class MOResourceOptimizer extends ResourceOptimizer {

    private static final long CREATION_TIME = 0l;
    private Map<CloudInstanceTypeDescription, MOProfile> defaultProfiles;

    public MOResourceOptimizer(MOScheduler ts) {
        super(ts);
        defaultProfiles = new HashMap<>();
        for (CloudProvider cp : ResourceManager.getAvailableCloudProviders()) {
            for (CloudInstanceTypeDescription citd : cp.getAllTypes()) {
                JSONObject JSONcitd = ts.getJSONForCloudInstanceTypeDescription(cp, citd);
                System.out.println(citd.getName() + " --> " + JSONcitd);
            }
        }
    }
    /*
    @Override
    protected void applyPolicies(WorkloadState workload) {
        try {

            long timeBoundary = MOConfiguration.getTimeBoundary();
            double energyBoundary = MOConfiguration.getEconomicBoundary();
            double costBoundary = MOConfiguration.getMonetaryBoundary();
            double powerBoundary = MOConfiguration.getPowerBoundary();
            double priceBoundary = MOConfiguration.getPriceBoundary();

            long[] elapsedTime = new long[1];
            double[] elapsedEnergy = new double[1];
            double[] elapsedCost = new double[1];
            double[] elapsedPower = new double[1];
            double[] elapsedPrice = new double[1];

            Collection<ResourceScheduler<? extends WorkerResourceDescription>> workers = ts.getWorkers();
            List<ResourceCreationRequest> creations = ResourceManager.getPendingCreationRequests();
            Resource[] allResources = new Resource[workers.size() + creations.size()];
            int[] load = new int[workload.getCoreCount()];

            HashMap<String, Integer> pendingCreations = new HashMap<>();
            HashMap<String, Integer> pendingReductions = new HashMap<>();
            ConfigurationCost actualCost = getContext(allResources, load, workers, creations, pendingCreations, pendingReductions, elapsedTime, elapsedEnergy, elapsedCost, elapsedPower, elapsedPrice);

            addToLog("Boundaries\n"
                    + "\tTime: " + timeBoundary + "s\n"
                    + "\tEnergy: " + energyBoundary + "Wh\n"
                    + "\tCost: " + costBoundary + "€\n"
                    + "\tPower: " + powerBoundary + "W\n"
                    + "\tPrice: " + priceBoundary + "€/h\n");

            addToLog("Elapsed\n"
                    + "\tTime: " + elapsedTime[0] + "s\n"
                    + "\tEnergy: " + elapsedEnergy[0] + "Wh\n"
                    + "\tCost: " + elapsedCost[0] + "€\n"
                    + "\tPower: " + elapsedPower[0] + "W\n"
                    + "\tPrice: " + elapsedPrice[0] + "€/h\n");

            long timeBudget = timeBoundary - elapsedTime[0];
            double energyBudget = energyBoundary - elapsedEnergy[0];
            double costBudget = costBoundary - elapsedCost[0];
            double powerBudget = powerBoundary - elapsedPower[0];
            double priceBudget = priceBoundary - elapsedPrice[0];
            addToLog("Budget\n"
                    + "\tTime: " + timeBudget + "s\n"
                    + "\tEnergy: " + energyBudget + "Wh - " + (energyBudget * 3600) + "J\n"
                    + "\tCost: " + costBudget + "€\n"
                    + "\tPower: " + powerBudget + "W\n"
                    + "\tPrice: " + priceBudget + "€/h\n");

            addToLog("Current Resources\n");
            addToLog("Workload Info:\n");
            for (int coreId = 0; coreId < workload.getCoreCount(); coreId++) {
                addToLog("\tCore " + coreId + ": " + load[coreId] + "\n");
            }

            Action actualAction = new Action(actualCost);
            addToLog(actualAction.toString());

            ConfigurationCost simCost = simulate(load, allResources, 0, 0, 0);
            Action currentSim = new Action(simCost);
            addToLog(currentSim.toString());

            LinkedList<Action> actions = generatePossibleActions(allResources, load);
            Action action = this.selectBestAction(currentSim, actions, timeBudget, energyBudget, costBudget, powerBudget, priceBudget);
            addToLog("Action to perform: " + action.title + "\n");
            printLog();
            action.perform();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private LinkedList<Action> generatePossibleActions(Resource[] allResources, int[] load) {
        LinkedList<Action> actions = new LinkedList<>();

        //Generate all possible resource acquisitions
        if (ResourceManager.getCurrentVMCount() < ResourceManager.getMaxCloudVMs()) {
            generatePossibleResourceAcquisitions(actions, allResources, load);
        }

        //Generate all possible resource releases
        if (ResourceManager.getCurrentVMCount() > ResourceManager.getMinCloudVMs()) {
            generatePossibleResourceReleases(actions, allResources, load);
        }
        return actions;
    }

    private void generatePossibleResourceAcquisitions(LinkedList<Action> actions, Resource[] allResources, int[] load) {
        for (CloudProvider cp : ResourceManager.getAvailableCloudProviders()) {
            if (!cp.canHostMoreInstances()) {
                continue;
            }
            for (CloudInstanceTypeDescription citd : cp.getAllTypes()) {
                for (CloudImageDescription cid : cp.getAllImages()) {
                    Resource[] resources = new Resource[allResources.length + 1];
                    System.arraycopy(allResources, 0, resources, 0, allResources.length);
                    resources[allResources.length] = createResourceForComponent(citd, cid);
                    ConfigurationCost cc = simulate(load, resources, 0, 0, 0);
                    Action a = new Action.Add(cp, citd, cid, cc);
                    addToLog(a.toString());
                    actions.add(a);
                }
            }

        }
    }

    private void generatePossibleResourceReleases(LinkedList<Action> actions, Resource[] allResources, int[] load) {
        for (int i = 0; i < allResources.length; i++) {
            Resource excludedWorker = allResources[i];
            Worker w = excludedWorker.getResource();
            if (!(w.getDescription() instanceof CloudMethodResourceDescription)) {
                continue;
            }
            CloudMethodResourceDescription description = (CloudMethodResourceDescription) w.getDescription();
            if (!(excludedWorker.hasPendingModifications())) {
                CloudImageDescription image = description.getImage();
                for (CloudInstanceTypeDescription typeReduction : description.getPossibleReductions()) {
                    CloudMethodResourceDescription reductionDescription = new CloudMethodResourceDescription(typeReduction, image);
                    CloudMethodResourceDescription reducedDescription = new CloudMethodResourceDescription(description);
                    reducedDescription.reduce(reductionDescription);
                    ConfigurationCost cc;
                    if (reducedDescription.getTypeComposition().isEmpty()) {
                        Resource[] resources = new Resource[allResources.length - 1];
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
                    Action a = new Action.Remove(excludedWorker, typeReduction, cc);
                    addToLog(a.toString());
                    actions.add(a);
                }
            }
        }
    }

    private Action selectBestAction(Action currentAction, LinkedList<Action> candidates, long timeBudget, double energyBudget, double costBudget, double powerBudget, double priceBudget) {
        addToLog("SELECTING BEST ACTION ACCORDING TO " + MOConfiguration.getSchedulerOptimization() + "\n");
        Action bestAction = currentAction;
        for (Action action : candidates) {

            boolean improves = false;
            if (action.cost.power > powerBudget || action.cost.price > priceBudget) {
                addToLog("\t\t Surpasses the power (" + action.cost.power + ">" + powerBudget + ") or price budget (" + action.cost.price + ">" + priceBudget + ")");
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
                    //UNKNOWN: DO NOTHING!!!
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

    private static <T extends Comparable> boolean isAcceptable(T candidate, T reference, T budget) {
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
            return isAcceptable(cCost.energy, rCost.energy, energyBudget) && isAcceptable(cCost.cost, rCost.cost, costBudget);
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
                    addToLog("\t\t Time's higher than the currently selected option. But energy is closer to the boundary\n");
                }
                return isAcceptable(cCost.cost, rCost.cost, costBudget);
            }

            if (rCost.cost > costBudget && cCost.cost < rCost.cost) {
                if (isAcceptable(cCost.energy, rCost.energy, costBudget)) {
                    addToLog("\t\t Time's higher than the currently selected option. But cost is closer to the boundary\n");
                }
                return isAcceptable(cCost.energy, rCost.energy, costBudget);
            }
            addToLog("\t\t Time's higher than the currently selected option\n");

        }
        return false;
    }

    private boolean doesImproveCost(Action candidate, Action reference, double energyBudget, long timeBudget) {
        ConfigurationCost cCost = candidate.cost;
        ConfigurationCost rCost = reference.cost;
        if (cCost.cost < rCost.cost) {
            if (!isAcceptable(cCost.energy, rCost.energy, energyBudget)) {
                addToLog("\t\t Surpasses the energy budget " + cCost.energy + " > " + energyBudget + "\n");
            }
            if (!isAcceptable(cCost.time, rCost.time, timeBudget)) {
                addToLog("\t\t Surpasses the time budget " + cCost.time + " > " + timeBudget + "\n");
            }
            return isAcceptable(cCost.energy, rCost.energy, energyBudget) && isAcceptable(cCost.time, rCost.time, timeBudget);
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
                    addToLog("\t\t Cost's higher than the currently selected option. But time is closer to the boundary\n");
                }
                return isAcceptable(cCost.cost, rCost.cost, energyBudget);
            }
            if (rCost.energy > energyBudget && cCost.energy < rCost.energy) {
                if (isAcceptable(cCost.time, rCost.time, timeBudget)) {
                    addToLog("\t\t Energy's higher than the currently selected option. But energy is closer to the boundary\n");
                }
                return isAcceptable(cCost.time, rCost.time, timeBudget);
            }
            addToLog("\t\t Cost's higher than the currently selected option\n");
        }
        return false;
    }

    private boolean doesImproveEnergy(Action candidate, Action reference, long timeBudget, double costBudget) {
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
                    addToLog("\t\t Energy's higher than the currently selected option. But time is closer to the boundary\n");
                }
                return isAcceptable(cCost.cost, rCost.cost, costBudget);
            }
            if (rCost.cost > costBudget && cCost.cost < rCost.cost) {
                if (isAcceptable(cCost.time, rCost.time, timeBudget)) {
                    addToLog("\t\t Energy's higher than the currently selected option. But time is closer to the boundary\n");
                }
                return isAcceptable(cCost.time, rCost.time, timeBudget);
            }
            addToLog("\t\t Energy's higher than the currently selected option.\n");
        }
        return false;
    }

    private ConfigurationCost getContext(Resource[] allResources, int[] load, Collection<ResourceScheduler<? extends WorkerResourceDescription>> workers,
            List<ResourceCreationRequest> creations, HashMap<String, Integer> pendingCreations, HashMap<String, Integer> pendingDestructions,
            long[] elapsedTime, double[] elapsedEnergy, double[] elapsedCost, double[] elapsedPower, double[] elapsedPrice
    ) {

        elapsedTime[0] = 0;
        elapsedEnergy[0] = 0;
        elapsedCost[0] = 0;
        elapsedPower[0] = 0;
        elapsedPrice[0] = 0;

        long time = 0;
        double actionsCost = 0;
        double idlePrice = 0;
        double actionsEnergy = 0;
        double idlePower = 0;
        int resourceId = 0;
        for (ResourceScheduler w : workers) {
            Resource r = new Resource();
            allResources[resourceId] = r;
            r.worker = (MOResourceScheduler) w;

            MOResourceScheduler aw = (MOResourceScheduler) w;
            addToLog("\tName:" + aw.getName() + (r.hasPendingModifications() ? " (IS TO BE DELETED)" : "") + "\n");

            time = Math.max(time, aw.getLastGapExpectedStart());
            addToLog("\t\tTime:" + aw.getLastGapExpectedStart() + " ms -> total " + time + "\n");

            elapsedCost[0] += aw.getRunActionsCost();
            addToLog("\t\tExecuted Actions Cost:" + aw.getRunActionsCost() + " € -> total " + elapsedCost[0] + "€\n");

            actionsCost += aw.getScheduledActionsCost();
            addToLog("\t\tScheduled Actions Cost:" + aw.getScheduledActionsCost() + " € -> total " + actionsCost + "€\n");

            r.idlePrice = aw.getIdlePrice();
            idlePrice += r.idlePrice;
            addToLog("\t\tIdle Price:" + r.idlePrice + " € -> total " + idlePrice + "€\n");

            elapsedEnergy[0] += aw.getRunActionsEnergy();
            addToLog("\t\tExecuted Actions Energy:" + aw.getRunActionsEnergy() + " mJ -> total " + elapsedEnergy[0] + "mJ\n");

            actionsEnergy += aw.getScheduledActionsEnergy();
            addToLog("\t\tScheduled Actions Energy:" + aw.getScheduledActionsEnergy() + " mJ -> total " + actionsEnergy + "mJ\n");

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
                coreInfo[coreId].append("\t\t\t\tImplementation 0: " + implsCount[coreId][0] + ", " + runningCounts[coreId][0] + " of'em already running\n");
                for (int implId = 1; implId < CoreManager.getCoreImplementations(coreId).size(); implId++) {
                    coreInfo[coreId].append("\t\t\t\tImplementation " + implId + ": " + implsCount[coreId][implId] + ", " + runningCounts[coreId][implId] + " of'em already running\n");
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

                String componentType = ((CloudMethodResourceDescription) r.getResource().getDescription()).getType();
                Integer pendingDestruction = pendingCreations.get(componentType);
                if (pendingDestruction == null) {
                    pendingDestruction = 0;
                }
                pendingDestruction++;
                pendingDestructions.put(componentType, pendingDestruction);
            }
            resourceId++;
        }

        for (ResourceCreationRequest crc : creations) {
            String componentType = crc.getRequested().getType();
            addToLog("\tName: REQUESTED " + componentType + "\n");
            Integer pendingCreation = pendingCreations.get(componentType);
            if (pendingCreation == null) {
                pendingCreation = 0;
            }
            pendingCreation++;
            pendingCreations.put(componentType, pendingCreation);
            Resource r = createResourceForComponent(crc.getRequested());
            allResources[resourceId] = r;

            addToLog("\t\tTime: 0 ms -> total " + time + "\n");
            addToLog("\t\tactions Cost: 0 € -> total " + actionsCost + "€\n");
            idlePrice += r.idlePrice;
            addToLog("\t\tIdle Price:" + r.idlePrice + " € -> total " + idlePrice + "€\n");
            addToLog("\t\tactions Energy:0 mJ -> total " + actionsEnergy + "mJ\n");
            idlePower += r.idlePower;
            addToLog("\t\tIdle Power:" + r.idlePower + " W -> total " + idlePower + "W\n");

            r.startTime = 0;
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

    StringBuilder log = new StringBuilder(
            "-------------------------\n"
            + "    CHECK SCALABILITY    \n"
            + "-------------------------\n");

    private void addToLog(String s) {
        log.append(s);
    }

    private void printLog() {
        System.out.println(log.toString() + "\n"
                + "-----------------------------\n");
        log = new StringBuilder(
                "-------------------------\n"
                + "    CHECK SCALABILITY    \n"
                + "-------------------------\n");
    }

    private ConfigurationCost simulate(int[] counts, Resource[] resources, long minTime, double minEnergy, double minCost) {
//        addToLog("Simulation\n");
        int[] workingCounts = new int[counts.length];
        System.arraycopy(counts, 0, workingCounts, 0, counts.length);
        LinkedList<Resource> executable = new LinkedList();
        for (Resource r : resources) {
            r.clear();
            if (r.canExecute()) {
                executable.add(r);
            }
        }

        SortedList sl = new SortedList(executable.size());
        for (Resource r : executable) {
            sl.initialAdd(r);
        }

        for (int coreId = 0; coreId < workingCounts.length; coreId++) {
            while (workingCounts[coreId] > 0) {
                //Pressumes that all CE runs in every resource
                Resource r = sl.peek();
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
        for (Resource r : resources) {
            double rActionsEnergy = r.startEnergy;
            double rActionsCost = r.startCost;
//            addToLog("\t" + (r.worker != null ? r.getName() : " NEW") + "\n");
            time = Math.max(time, r.time);
            idlePower += r.idlePower;
            idlePrice += r.idlePrice;

            for (int coreId = 0; coreId < r.counts.length; coreId++) {
                rActionsEnergy += r.counts[coreId] * r.profiles[coreId].getPower() * r.profiles[coreId].getAverageExecutionTime();
                rActionsCost += r.counts[coreId] * r.profiles[coreId].getPrice();
            }
            actionsEnergy += rActionsEnergy;
            actionsCost += rActionsCost;
//            addToLog("\t\t Time: " + time + "ms\n");
//            addToLog("\t\tactions Cost:" + rActionsCost + " € -> total " + actionsCost + "€\n");
//            addToLog("\t\tIdle Price:" + r.idlePrice + " €/h -> total " + idlePrice + "€/h\n");
//            addToLog("\t\tactions Energy:" + rActionsEnergy + " mJ -> total " + actionsEnergy + "mJ\n");
//            addToLog("\t\tIdle Power:" + r.idlePower + " W -> total " + idlePower + "W\n");

        }
        return new ConfigurationCost(time, idlePower, actionsEnergy + minEnergy, idlePrice, actionsCost + minCost);
    }

    private class Resource {

        MOResourceScheduler worker;
        double idlePower;
        double idlePrice;
        MOProfile[] profiles;
        int[] capacity;
        long startTime;
        double startEnergy;
        double startCost;
        long time;
        int[] counts;

        public void clear() {
            time = startTime;
            counts = new int[profiles.length];
        }

        private boolean canExecute() {
            if (worker != null) {
                return !worker.hasPendingModifications();
            }
            return true;
        }

        private boolean hasPendingModifications() {
            if (worker != null) {
                return worker.hasPendingModifications();
            }
            return true;
        }

        private List<ResourceUpdate> getPendingModifications() {
            if (worker != null) {
                return worker.getPendingModifications();
            }
            return new LinkedList<>();
        }

        private Worker getResource() {
            if (worker != null) {
                return worker.getResource();
            }
            return null;
        }

        private String getName() {
            if (worker != null) {
                return worker.getName();
            }
            return "TEMPORARY";
        }

    }

    private Resource reduceResourceForComponent(Resource excludedWorker, CloudMethodResourceDescription reduction) {
        Resource clone = new Resource();
        clone.worker = excludedWorker.worker;
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

            clone.idlePower -= Ascetic.getPower(componentName) * count;
            clone.idlePrice -= Ascetic.getPrice(componentName) * count;
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
                MOProfile[] profiles = new MOProfile[impls.size()];
                for (int i = 0; i < impls.size(); i++) {
                    profiles[i] = getPredefinedProfile(type, impls.get(i));
                }
                Implementation impl = getBestImplementation(impls, profiles);
                clone.capacity[coreId] -= rd.canHostSimultaneously((MethodResourceDescription) impl.getRequirements()) * count;
            }
        }
        return clone;
    }

    private Resource createResourceForComponent(CloudMethodResourceDescription cmrd) {
        Resource r = new Resource();
        Map<CloudInstanceTypeDescription, int[]> composition = cmrd.getTypeComposition();
        r.capacity = new int[CoreManager.getCoreCount()];
        r.profiles = new MOProfile[CoreManager.getCoreCount()];

        for (Map.Entry<CloudInstanceTypeDescription, int[]> component : composition.entrySet()) {
            CloudInstanceTypeDescription type = component.getKey();
            int count = component.getValue()[0];
            MethodResourceDescription rd = type.getResourceDescription();

            r.idlePower += Ascetic.getPower(componentName) * count;
            r.idlePrice += Ascetic.getPrice(componentName) * count;

            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
                MOProfile[] profiles = new MOProfile[impls.size()];
                for (int i = 0; i < impls.size(); i++) {
                    profiles[i] = getPredefinedProfile(type, impls.get(i));
                }
                Implementation impl = getBestImplementation(impls, profiles);
                r.capacity[coreId] += rd.canHostSimultaneously((MethodResourceDescription) impl.getRequirements()) * count;
                r.profiles[coreId].accumulate(getPredefinedProfile(type, impl));
            }
        }

        r.startTime = CREATION_TIME;
        r.clear();
        return r;
    }

    private Resource createResourceForComponent(CloudInstanceTypeDescription citd, CloudImageDescription cid) {
        Resource r = new Resource();
        r.idlePower = Ascetic.getPower(componentName);
        r.idlePrice = Ascetic.getPrice(componentName);
        MethodResourceDescription rd = citd.getResourceDescription();
        r.capacity = new int[CoreManager.getCoreCount()];
        r.profiles = new MOProfile[CoreManager.getCoreCount()];
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            MOProfile[] profiles = new MOProfile[impls.size()];
            for (int i = 0; i < impls.size(); i++) {
                profiles[i] = getPredefinedProfile(citd, impls.get(i));
            }
            Implementation impl = getBestImplementation(impls, profiles);
            r.capacity[coreId] = rd.canHostSimultaneously((MethodResourceDescription) impl.getRequirements());
            r.profiles[coreId] = getPredefinedProfile(citd, impl);
        }
        r.startTime = CREATION_TIME;
        r.clear();
        return r;
    }

    private Implementation getBestImplementation(List<Implementation> impls, MOProfile[] profiles) {
        Implementation impl = impls.get(0);
        MOScore bestScore = new MOScore(0, 0, 0, profiles[0].getAverageExecutionTime(), profiles[0].getPower(), profiles[0].getPrice());
        for (int i = 1; i < impls.size(); i++) {
            Implementation candidate = impls.get(i);
            long length = profiles[i].getAverageExecutionTime();
            double power = profiles[i].getPower();
            double price = profiles[i].getPrice();
            MOScore score = new MOScore(0, 0, 0, length, power * length, price);
            if (Score.isBetter(score, bestScore)) {
                bestScore = score;
                impl = candidate;
            }
        }
        return impl;
    }

    private class SortedList {

        private final Resource[] values;

        public SortedList(int size) {
            this.values = new Resource[size];
        }

        public void initialAdd(Resource r) {
            for (int i = 0; i < values.length - 1; i++) {
                if (values[i + 1] != null && r.time < values[i + 1].time) {
                    values[i] = r;
                    return;
                } else {
                    values[i] = values[i + 1];
                }
            }
            values[values.length - 1] = r;
        }

        public void add(Resource r) {
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

        public Resource peek() {
            return values[0];
        }
    }

    private static class Action {

        String title;
        ConfigurationCost cost;

        public Action(ConfigurationCost cost) {
            title = "Current Configuration";
            this.cost = cost;
        }

        public String toString() {
            return title + ":\n" + cost.toString();
        }

        public void perform() {
        }

        public static class Add extends Action {

            private final CloudProvider provider;
            private final CloudInstanceTypeDescription instance;
            private final CloudImageDescription image;

            public Add(CloudProvider provider, CloudInstanceTypeDescription component, CloudImageDescription image, ConfigurationCost cost) {
                super(cost);
                this.title = "Add " + component;
                this.provider = provider;
                this.image = image;
                this.instance = component;
            }

            public void perform() {
                RUNTIME_LOGGER.debug("ASCETIC: Performing Add action " + this);
                CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(instance, image);
                provider.requestResourceCreation(cmrd);
            }
        }

        public static class Remove extends Action {

            private final Resource res;
            private final CloudInstanceTypeDescription citd;

            public Remove(Resource res, CloudInstanceTypeDescription reduction, ConfigurationCost cost) {
                super(cost);
                title = "Remove " + res.getName();
                this.citd = reduction;
                this.res = res;
            }

            public void perform() {
                CloudMethodWorker worker = (CloudMethodWorker) res.getResource();
                CloudMethodResourceDescription reduction = new CloudMethodResourceDescription(citd, worker.getDescription().getImage());
                ResourceManager.reduceCloudWorker(worker, reduction);
            }
        }
    }

    private static class ConfigurationCost {

        long time;
        double energy;
        double cost;
        double power;
        double price;

        ConfigurationCost(long time, double idlePower, double fixedEnergy, double idlePrice, double fixedCost) {
            this.time = time / 1000;
            this.energy = (idlePower * time + fixedEnergy) / 3_600_000;
            this.cost = (idlePrice * ((double) (time / 3_600_000))) + fixedCost;
            this.power = idlePower + (fixedEnergy / time);
            this.price = idlePrice + (double) ((fixedCost * 3_600_000) / time);
        }

        @Override
        public String toString() {
            return "\tTime: " + time + "s\n"
                    + "\tEnergy: " + energy + "Wh\n"
                    + "\tCost: " + cost + "€\n"
                    + "\tPower: " + power + "W\n"
                    + "\tPrice: " + price + "€/h\n";
        }
    }

    private MOProfile getPredefinedProfile(CloudInstanceTypeDescription citd, Implementation impl) {
        JSONObject json = new JSONObject("");
        return new MOProfile(json);
    }*/
}
