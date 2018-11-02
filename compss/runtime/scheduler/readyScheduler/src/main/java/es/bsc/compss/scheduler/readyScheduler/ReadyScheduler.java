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
package es.bsc.compss.scheduler.readyScheduler;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Representation of a Scheduler that considers only ready tasks
 *
 */
public abstract class ReadyScheduler extends TaskScheduler {

	// Logger
	protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

	// Tree set is an ordered set!!
	protected final HashMap<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> unassignedReadyActions;
	protected final HashSet<ResourceScheduler<?>> availableWorkers;
	protected final HashSet<ResourceScheduler<?>> workers;

	/**
	 * Constructs a new Ready Scheduler instance
	 *
	 */
	public ReadyScheduler() {
		super();
		this.unassignedReadyActions = new HashMap<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>();
		this.availableWorkers = new HashSet<ResourceScheduler<?>>();
		this.workers = new HashSet<ResourceScheduler<?>>();
	}

	/*
	 * *****************************************************************************
	 * ****************************
	 * *****************************************************************************
	 * **************************** ***************************** UPDATE STRUCTURES
	 * OPERATIONS **********************************************
	 * *****************************************************************************
	 * ****************************
	 * *****************************************************************************
	 * ****************************
	 */
	@Override
	public <T extends WorkerResourceDescription> void workerLoadUpdate(ResourceScheduler<T> resource) {

	}

	@Override
	protected <T extends WorkerResourceDescription> void workerDetected(ResourceScheduler<T> resource) {
		super.workerDetected(resource);
		this.availableWorkers.add(resource);
		this.workers.add(resource);
		if (unassignedReadyActions.size() > 0) {
			TreeSet<ObjectValue<AllocatableAction>> orderedActions = new TreeSet<ObjectValue<AllocatableAction>>();
			Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter = unassignedReadyActions
					.entrySet().iterator();
			TreeSet<ObjectValue<AllocatableAction>> actionList = (TreeSet<ObjectValue<AllocatableAction>>) iter.next()
					.getValue();
			for (ObjectValue<AllocatableAction> actionValue : actionList) {
				AllocatableAction action = actionValue.getObject();
				Score actionScore = generateActionScore(action);
				Score fullScore = action.schedulingScore(resource, actionScore);
				ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
				orderedActions.add(obj);
			}
			this.unassignedReadyActions.put(resource, orderedActions);
		} else {
			TreeSet<ObjectValue<AllocatableAction>> orderedActions = new TreeSet<ObjectValue<AllocatableAction>>();
			this.unassignedReadyActions.put(resource, orderedActions);
		}
	}

	@Override
	protected <T extends WorkerResourceDescription> void workerRemoved(ResourceScheduler<T> resource) {
		super.workerRemoved(resource);
		this.availableWorkers.remove(resource);
		this.workers.remove(resource);
		if (this.workers.size() == 0) {
			for (ObjectValue<AllocatableAction> actionValue : this.unassignedReadyActions.get(resource)) {
				AllocatableAction action = actionValue.getObject();
				addToBlocked(action);
			}
		}
		resource.setRemoved(true);
		this.unassignedReadyActions.remove(resource);
	}

	@Override
	public abstract Score generateActionScore(AllocatableAction action);

	/*
	 * *****************************************************************************
	 * ****************************
	 * *****************************************************************************
	 * **************************** ********************************* SCHEDULING
	 * OPERATIONS *************************************************
	 * *****************************************************************************
	 * ****************************
	 * *****************************************************************************
	 * ****************************
	 */
	@Override
	protected void scheduleAction(AllocatableAction action, Score actionScore) {
		try {
			action.tryToSchedule(actionScore, this.availableWorkers);
			ResourceScheduler<? extends WorkerResourceDescription> resource = action.getAssignedResource();
			if (!resource.canRunSomething()) {
				this.availableWorkers.remove(resource);
			}
			removeActionFromSchedulerStructures(action);
		} catch (UnassignedActionException | BlockedActionException ex) {
			addActionToSchedulerStructures(action);
		}
	}

	protected <T extends WorkerResourceDescription> void scheduleAction(AllocatableAction action,
			ResourceScheduler<T> targetWorker, Score actionScore)
			throws BlockedActionException, UnassignedActionException {
		// This if should be eraseble since we only handle dependency free actions
		if (!action.hasDataPredecessors()) {
			action.schedule(targetWorker, actionScore);
		}
	}

	@Override
	public List<AllocatableAction> getUnassignedActions() {
		LinkedList<AllocatableAction> unassigned = new LinkedList<AllocatableAction>();

		Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter = unassignedReadyActions
				.entrySet().iterator();
		TreeSet<ObjectValue<AllocatableAction>> actionList = (TreeSet<ObjectValue<AllocatableAction>>) iter.next()
				.getValue();
		for (ObjectValue<AllocatableAction> actionObject : actionList) {
			unassigned.add(actionObject.getObject());
		}
		return unassigned;
	}

	@Override
	public final <T extends WorkerResourceDescription> void handleDependencyFreeActions(
			List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
			List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {
		LOGGER.debug("[ReadyScheduler] Treating dependency free actions on resource " + resource.getName());
		this.availableWorkers.add(resource);
		purgeFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
		tryToLaunchFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
	}

	public <T extends WorkerResourceDescription> void purgeFreeActions(List<AllocatableAction> dataFreeActions,
			List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
			ResourceScheduler<T> resource) {

		// List<AllocatableAction> unassignedReadyActions =
		// this.unassignedReadyActions.getAllActions();
		// this.unassignedReadyActions.removeAllActions();
		// dataFreeActions.addAll(unassignedReadyActions);
	}

	private void addActionToSchedulerStructures(AllocatableAction action) {
		if (!this.unassignedReadyActions.isEmpty()) {
			Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter = unassignedReadyActions
					.entrySet().iterator();
			Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry = iter.next();
			TreeSet<ObjectValue<AllocatableAction>> actionList = (TreeSet<ObjectValue<AllocatableAction>>) currentEntry
					.getValue();
			ResourceScheduler<?> resource = currentEntry.getKey();
			Score actionScore = generateActionScore(action);
			Score fullScore = action.schedulingScore(resource, actionScore);
			ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
			if (!actionList.add(obj)) {
				return;
			}
			while (iter.hasNext()) {
				currentEntry = iter.next();
				resource = currentEntry.getKey();
				actionList = (TreeSet<ObjectValue<AllocatableAction>>) currentEntry.getValue();
				fullScore = action.schedulingScore(resource, actionScore);
				obj = new ObjectValue<>(action, fullScore);
				actionList.add(obj);
			}
		} else {
			addToBlocked(action);
		}
	}

	private void removeActionFromSchedulerStructures(AllocatableAction action) {
		Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter = unassignedReadyActions
				.entrySet().iterator();
		Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry = iter.next();
		ResourceScheduler<?> resource = currentEntry.getKey();
		Score actionScore = generateActionScore(action);
		Score fullScore = action.schedulingScore(resource, actionScore);
		ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
		removeObjectValueFromScheduler(obj);
	}

	private void removeObjectValueFromScheduler(ObjectValue<AllocatableAction> obj) {
		Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter = unassignedReadyActions
				.entrySet().iterator();
		Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry = iter.next();
		TreeSet<ObjectValue<AllocatableAction>> actionList = currentEntry.getValue();
		if (!actionList.remove(obj)) {
			return;
		}
		while (iter.hasNext()) {
			currentEntry = iter.next();
			actionList = currentEntry.getValue();
			actionList.remove(obj);
		}
	}

	/**
	 * The action is not registered in any data structure
	 *
	 * @param action
	 */
	@Override
	protected void lostAllocatableAction(AllocatableAction action) {
		addActionToSchedulerStructures(action);
	}

	protected <T extends WorkerResourceDescription> void tryToLaunchFreeActions(List<AllocatableAction> dataFreeActions,
			List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
			ResourceScheduler<T> resource) {

		// Try to launch all the data free actions
		for (AllocatableAction freeAction : dataFreeActions) {
			addActionToSchedulerStructures(freeAction);
		}
		dataFreeActions = new LinkedList<AllocatableAction>();

		// Resource free actions should always be empty in this scheduler
		for (AllocatableAction freeAction : resourceFreeActions) {
			addActionToSchedulerStructures(freeAction);
		}
		resourceFreeActions = new LinkedList<AllocatableAction>();

		// Only in case there are actions that have entered the scheduler without having
		// available resources -> They were in the blocked list
		for (AllocatableAction freeAction : blockedCandidates) {
			addActionToSchedulerStructures(freeAction);
		}
		blockedCandidates = new LinkedList<AllocatableAction>();

		Iterator<ObjectValue<AllocatableAction>> executableActionsIterator = this.unassignedReadyActions.get(resource)
				.descendingIterator();
		HashSet<ObjectValue<AllocatableAction>> objectValueToErase = new HashSet<ObjectValue<AllocatableAction>>();
		while (executableActionsIterator.hasNext() && !this.availableWorkers.isEmpty()) {
			ObjectValue<AllocatableAction> obj = executableActionsIterator.next();
			AllocatableAction freeAction = obj.getObject();
			try {
				try {
					freeAction.tryToSchedule(obj.getScore(), this.availableWorkers);
					ResourceScheduler<? extends WorkerResourceDescription> assignedResource = freeAction
							.getAssignedResource();
					if (!resource.canRunSomething()) {
						this.availableWorkers.remove(assignedResource);
					}
					objectValueToErase.add(obj);
				} catch (UnassignedActionException e) {
					// This should never happen
					addActionToSchedulerStructures(freeAction);
				}
				tryToLaunch(freeAction);
			} catch (BlockedActionException e) {
				objectValueToErase.add(obj);
				addToBlocked(freeAction);
			}
		}
		for (ObjectValue<AllocatableAction> obj : objectValueToErase) {
			removeObjectValueFromScheduler(obj);
		}
	}
}
