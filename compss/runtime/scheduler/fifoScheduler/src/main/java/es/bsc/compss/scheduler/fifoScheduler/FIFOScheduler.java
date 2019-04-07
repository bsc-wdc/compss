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
package es.bsc.compss.scheduler.fifoScheduler;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONObject;

/**
 * Representation of a Scheduler that considers only ready tasks and sorts them
 * in FIFO mode
 *
 */
public class FIFOScheduler extends TaskScheduler {

	protected LinkedList<AllocatableAction> unassignedReadyActionsList;
	protected HashSet<AllocatableAction> unassignedReadyActionsSet; 
	protected final HashSet<ResourceScheduler<?>> availableWorkers;
	protected int amountOfWorkers;

	/**
	 * Constructs a new Ready Scheduler instance
	 *
	 */
	public FIFOScheduler() {
		super();
		this.unassignedReadyActionsList = new LinkedList<AllocatableAction>();
		this.unassignedReadyActionsSet = new HashSet<AllocatableAction>();
		this.availableWorkers = new HashSet<ResourceScheduler<?>>();
		this.amountOfWorkers = 0;
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
	public <T extends WorkerResourceDescription> FIFOResourceScheduler<T> generateSchedulerForResource(Worker<T> w,
			JSONObject resJSON, JSONObject implJSON) {
		// LOGGER.debug("[FIFOScheduler] Generate scheduler for resource " +
		// w.getName());
		return new FIFOResourceScheduler<>(w, resJSON, implJSON);
	}

	@Override
	protected <T extends WorkerResourceDescription> void workerDetected(ResourceScheduler<T> resource) {
		super.workerDetected(resource);
		this.availableWorkers.add(resource);
		this.amountOfWorkers += 1;
	}

	@Override
	protected <T extends WorkerResourceDescription> void workerRemoved(ResourceScheduler<T> resource) {
		super.workerRemoved(resource);
		this.availableWorkers.remove(resource);
		this.amountOfWorkers -= 1;
		if (this.amountOfWorkers == 0) {
			for (AllocatableAction action : this.unassignedReadyActionsList) {
				addToBlocked(action);
			}
			this.unassignedReadyActionsList.clear();
			this.unassignedReadyActionsSet.clear();
		}
	}

	@Override
	public Score generateActionScore(AllocatableAction action) {
		// LOGGER.debug("[FIFOScheduler] Generate Action Score for " + action);
		return new Score(action.getPriority(), 0, 0, 0);
	}

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
	protected void scheduleAction(AllocatableAction action, Score actionScore) throws BlockedActionException {
		if (!action.hasDataPredecessors()) {
			try {
				if (DEBUG) {
					LOGGER.debug("[ReadyScheduler] Trying to schedule action " + action);
				}
				action.tryToSchedule(actionScore, this.availableWorkers);
				ResourceScheduler<? extends WorkerResourceDescription> resource = action.getAssignedResource();
				if (!resource.canRunSomething()) {
					this.availableWorkers.remove(resource);
				}
				removeActionFromSchedulerStructures(action);
				if (DEBUG) {
					LOGGER.debug("[ReadyScheduler] Remove action " + action + " from scheduler structures");
				}
			} catch (UnassignedActionException ex) {
				if (DEBUG) {
					LOGGER.debug("[ReadyScheduler] Introducing action " + action
							+ " into the scheduler from try to schedule action");
				}
				addActionToSchedulerStructures(action);
			}
		}
	}

	@Override
	public List<AllocatableAction> getUnassignedActions() {
		return this.unassignedReadyActionsList;
	}

	@Override
	public final <T extends WorkerResourceDescription> void handleDependencyFreeActions(
			List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
			List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {
		if (DEBUG) {
			LOGGER.debug("[ReadyScheduler] Treating dependency free actions on resource " + resource.getName());
		}
		this.availableWorkers.add(resource);
		purgeFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
		tryToLaunchFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
	}

	public <T extends WorkerResourceDescription> void purgeFreeActions(List<AllocatableAction> dataFreeActions,
			List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
			ResourceScheduler<T> resource) {
	}

	private void addActionToSchedulerStructures(AllocatableAction action) {
		if (this.amountOfWorkers == 0) {
			if (DEBUG) {
				LOGGER.debug(
						"[ReadyScheduler] Cannot add action " + action + " because there are not available resources");
			}
			addToBlocked(action);
		}else {
			if(this.unassignedReadyActionsSet.add(action)) {
				if (DEBUG) {
					LOGGER.debug("[ReadyScheduler] Add action to scheduler structures " + action);
				}
				this.unassignedReadyActionsList.addLast(action);
			}
		}
	}

	private void removeActionFromSchedulerStructures(AllocatableAction action) {
		if(this.unassignedReadyActionsSet.remove(action)) {
			this.unassignedReadyActionsList.remove(action);
		}
	}

	protected <T extends WorkerResourceDescription> void tryToLaunchFreeActions(List<AllocatableAction> dataFreeActions,
			List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
			ResourceScheduler<T> resource) {
		if (DEBUG) {
			LOGGER.debug("[ReadyScheduler] Try to launch free actions on resource " + resource.getName() + " with "
					+ this.unassignedReadyActionsList.size() + " candidates in this worker");
		}

		// Actions that have been freeded by the action that just finished
		for (AllocatableAction freeAction : dataFreeActions) {
			if (DEBUG) {
				LOGGER.debug(
						"[ReadyScheduler] Introducing action " + freeAction + " into the scheduler from data free");
			}
			addActionToSchedulerStructures(freeAction);
		}
		dataFreeActions = new LinkedList<AllocatableAction>();

		// Resource free actions should always be empty in this scheduler
		for (AllocatableAction freeAction : resourceFreeActions) {
			if (DEBUG) {
				LOGGER.debug(
						"[ReadyScheduler] Introducing action " + freeAction + " into the scheduler from resource free");
			}
			addActionToSchedulerStructures(freeAction);
		}
		resourceFreeActions = new LinkedList<AllocatableAction>();

		// Only in case there are actions that have entered the scheduler without having
		// available resources -> They were in the blocked list
		for (AllocatableAction freeAction : blockedCandidates) {
			if (DEBUG) {
				LOGGER.debug("[ReadyScheduler] Introducing action " + freeAction + " into the scheduler from blocked");
			}
			addActionToSchedulerStructures(freeAction);
		}
		blockedCandidates = new LinkedList<AllocatableAction>();

		Iterator<AllocatableAction> executableActionsIterator = this.unassignedReadyActionsList.iterator();
		while (executableActionsIterator.hasNext() && !this.availableWorkers.isEmpty()) {
			AllocatableAction freeAction = executableActionsIterator.next();
			try {
				freeAction.tryToSchedule(generateActionScore(freeAction), this.availableWorkers);
				ResourceScheduler<? extends WorkerResourceDescription> assignedResource = freeAction
						.getAssignedResource();
				tryToLaunch(freeAction);
				if (!assignedResource.canRunSomething()) {
					this.availableWorkers.remove(assignedResource);
				}
				this.unassignedReadyActionsSet.remove(freeAction);
				executableActionsIterator.remove();
			} catch (BlockedActionException e) {
				this.unassignedReadyActionsSet.remove(freeAction);
				executableActionsIterator.remove();
				addToBlocked(freeAction);
			} catch (UnassignedActionException e) {
				// Nothing to be done here since the action was already in the scheduler
				// structures. If there is an exception, the freeAction will not be added
				// to the objectValueToErase list.
				// Hence, this is not an ignored Exception but an expected behavior.
			}
		}
	}
}
