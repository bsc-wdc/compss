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
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Representation of a Scheduler that considers only ready tasks
 *
 */
public abstract class ReadyScheduler extends TaskScheduler {

	// Tree set is an ordered set!!
	protected HashMap<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> unassignedReadyActions;
	protected final HashSet<ResourceScheduler<?>> availableWorkers;
	protected final HashMap<ResourceScheduler<?>, Future<?>> resourceTokens;
	protected int amountOfWorkers;
	ThreadPoolExecutor schedulerExecutor;

	/**
	 * Constructs a new Ready Scheduler instance
	 *
	 */
	public ReadyScheduler() {
		super();
		this.unassignedReadyActions = new HashMap<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>();
		this.resourceTokens = new HashMap<ResourceScheduler<?>, Future<?>>();
		this.availableWorkers = new HashSet<ResourceScheduler<?>>();
		this.amountOfWorkers = 0;
		this.schedulerExecutor = new ThreadPoolExecutor(15, 40, 180, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
		this.schedulerExecutor.allowCoreThreadTimeOut(true);
	}

	/*
	 * *****************************************************************************
	 * *****************************************************************************
	 * ************************ UPDATE STRUCTURES OPERATIONS ***********************
	 * *****************************************************************************
	 * *****************************************************************************
	 * *****************************************************************************
	 */

	@Override
	public <T extends WorkerResourceDescription> void workerLoadUpdate(ResourceScheduler<T> resource) {

	}

	@Override
	protected <T extends WorkerResourceDescription> void workerDetected(ResourceScheduler<T> resource) {
		super.workerDetected(resource);
		this.availableWorkers.add(resource);
		this.resourceTokens.put(resource, null);
		this.amountOfWorkers += 1;
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
		this.resourceTokens.remove(resource);
		this.amountOfWorkers -= 1;
		if (this.amountOfWorkers == 0) {
			for (ObjectValue<AllocatableAction> actionValue : this.unassignedReadyActions.get(resource)) {
				AllocatableAction action = actionValue.getObject();
				addToBlocked(action);
			}
		}
		this.unassignedReadyActions.remove(resource);
	}

	@Override
	public abstract Score generateActionScore(AllocatableAction action);

	/*
	 * *****************************************************************************
	 * *****************************************************************************
	 * *************************** SCHEDULING OPERATIONS ***************************
	 * *****************************************************************************
	 * *****************************************************************************
	 * *****************************************************************************
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

	protected <T extends WorkerResourceDescription> void scheduleAction(AllocatableAction action,
			ResourceScheduler<T> targetWorker, Score actionScore)
			throws BlockedActionException, UnassignedActionException {
		// This if should be eraseble since we only handle dependency free actions
		if (!action.hasDataPredecessors()) {
			action.schedule(targetWorker, actionScore);
			removeActionFromSchedulerStructures(action);
		}
	}

	@Override
	public List<AllocatableAction> getUnassignedActions() {
		LinkedList<AllocatableAction> unassigned = new LinkedList<AllocatableAction>();
		Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter = unassignedReadyActions
				.entrySet().iterator();
		TreeSet<ObjectValue<AllocatableAction>> actionList = (TreeSet<ObjectValue<AllocatableAction>>) iter.next()
				.getValue();
		ResourceScheduler<?> resource = ((Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>)iter).getKey();
		Future<?> resourceToken = this.resourceTokens.get(resource);
		if (resourceToken != null) {
			try {
				resourceToken.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
				LOGGER.fatal("Unexpected thread interruption");
				ErrorManager.fatal("Unexpected thread interruption");
			}
		}
		for (ObjectValue<AllocatableAction> actionObject : actionList) {
			unassigned.add(actionObject.getObject());
		}
		return unassigned;
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


	private void addActionToResource(Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry, AllocatableAction action) {
		ResourceScheduler<?> resource = currentEntry.getKey();
		TreeSet<ObjectValue<AllocatableAction>> actionList = (TreeSet<ObjectValue<AllocatableAction>>) currentEntry.getValue();
		Score fullScore = action.schedulingScore(resource, generateActionScore(action));
		if (fullScore != null) {
			ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
			actionList.add(obj);
		}
	}
	
	private void removeActionFromResource(Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry, AllocatableAction action) {
		currentEntry.getKey();
		TreeSet<ObjectValue<AllocatableAction>> actionList = currentEntry.getValue();
		Score fullScore = action.schedulingScore(currentEntry.getKey(), generateActionScore(action));
		if (fullScore != null) {
			ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
			actionList.remove(obj);
		}
	}
	
	private Runnable createAddRunnable(final Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry, final AllocatableAction action, final Future<?> token) {
		Runnable addRunnable = new Runnable() {
			public void run() {
				if (token != null) {
					try {
						token.get();
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
						LOGGER.fatal("Unexpected thread interruption");
						ErrorManager.fatal("Unexpected thread interruption");
					}
				}
				addActionToResource(currentEntry, action);
			}
		};
		return addRunnable;
	}
	
	private Runnable createRemoveRunnable(final Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry, final AllocatableAction action, final Future<?> token) {
		Runnable removeRunnable = new Runnable() {
			public void run() {
				if (token != null) {
					try {
						token.get();
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
						LOGGER.fatal("Unexpected thread interruption");
						ErrorManager.fatal("Unexpected thread interruption");
					}
				}
				removeActionFromResource(currentEntry, action);
			}
		};
		return removeRunnable;
	}	

	private void addActionToSchedulerStructures(AllocatableAction action) {
		if (!this.unassignedReadyActions.isEmpty()) {
			if (DEBUG) {
				LOGGER.debug("[ReadyScheduler] Add action to scheduler structures " + action);
			}
			Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter = unassignedReadyActions
					.entrySet().iterator();
			Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry = iter.next();
			TreeSet<ObjectValue<AllocatableAction>> actionList = (TreeSet<ObjectValue<AllocatableAction>>) currentEntry
					.getValue();
			
			ResourceScheduler<?> resource = currentEntry.getKey();
			Score actionScore = generateActionScore(action);
			Score fullScore = action.schedulingScore(resource, actionScore);
			//if (fullScore != null) {
				ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
				if (!actionList.add(obj)) {
					return;
				}
			//}
			while (iter.hasNext()) {
				currentEntry = iter.next();
				/*
				resource = currentEntry.getKey();
				actionList = (TreeSet<ObjectValue<AllocatableAction>>) currentEntry.getValue();
				fullScore = action.schedulingScore(resource, actionScore);
				obj = new ObjectValue<>(action, fullScore);
				actionList.add(obj);
				action.setLastScore(resource, fullScore);
				*/
				resource = currentEntry.getKey();
				Future<?> lastToken = this.resourceTokens.get(resource);
				this.resourceTokens.put(resource, schedulerExecutor.submit(createAddRunnable(currentEntry, action, lastToken)));
				//schedulerExecutor.execute(createAddRunnable(currentEntry, action));
				//addActionToResource(currentEntry, action);
			}
			/*
			for (Future<?> currentExecution: futureList) {
				try {
					currentExecution.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
					LOGGER.fatal("Unexpected thread interruption");
					ErrorManager.fatal("Unexpected thread interruption");
				}
			}
			*/
			/*
			schedulerExecutor.shutdown();
			//while(!schedulerExecutor.isTerminated()) {
			try {
				schedulerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
			  // I'm not gonna interrupt this thread
			}
			*/
			//}
			
		} else {
			if (DEBUG) {
				LOGGER.debug(
						"[ReadyScheduler] Cannot add action " + action + " because there are not available resources");
			}
			addToBlocked(action);
		}
	}

	private void removeActionFromSchedulerStructures(AllocatableAction action) {
		if (!this.unassignedReadyActions.isEmpty()) {
			if (DEBUG) {
				LOGGER.debug("[ReadyScheduler] Remove action from scheduler structures " + action);
			}
			Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter = unassignedReadyActions
					.entrySet().iterator();
			Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry = iter.next();
			ResourceScheduler<?> resource = currentEntry.getKey();
			TreeSet<ObjectValue<AllocatableAction>> actionList = currentEntry.getValue();
			Score actionScore = generateActionScore(action);
			Score fullScore = action.schedulingScore(resource, actionScore);
			//Score lastScore = action.getLastScore(resource);
			ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
			if (!actionList.remove(obj)) {
				return;
			}
			//this.schedulerExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
			while (iter.hasNext()) {
				currentEntry = iter.next();
				/*
				currentEntry.getKey();
				actionList = currentEntry.getValue();
				lastScore = action.getLastScore(resource);
				obj = new ObjectValue<>(action, lastScore);
				actionList.remove(obj);
				*/
				resource = currentEntry.getKey();
				Future<?> lastToken = this.resourceTokens.get(resource);
				this.resourceTokens.put(resource, schedulerExecutor.submit(createRemoveRunnable(currentEntry, action, lastToken)));
				//futureList.add(schedulerExecutor.submit(createRemoveRunnable(currentEntry, action)));
				//schedulerExecutor.execute(createRemoveRunnable(currentEntry, action));
				//removeActionFromResource(currentEntry, action);
			}
			/*
			for (Future<?> currentExecution: futureList) {
				try {
					currentExecution.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
					LOGGER.fatal("Unexpected thread interruption");
					ErrorManager.fatal("Unexpected thread interruption");
				}
			}
			*/
			//schedulerExecutor.shutdown();
			//while(!schedulerExecutor.isTerminated()) {
			//try {
			//	schedulerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			//} catch (InterruptedException e) {
			  // I'm not gonna interrupt this thread
			//}
			//}
		}
	}

	protected <T extends WorkerResourceDescription> void tryToLaunchFreeActions(List<AllocatableAction> dataFreeActions,
			List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
			ResourceScheduler<T> resource) {
		if (DEBUG) {
			LOGGER.debug("[ReadyScheduler] Try to launch free actions on resource " + resource.getName() + " with "
					+ this.unassignedReadyActions.get(resource).size() + " candidates in this worker");
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
		
		Future<?> lastToken = this.resourceTokens.get(resource);
		if (lastToken != null) {
			try {
				lastToken.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
				LOGGER.fatal("Unexpected thread interruption");
				ErrorManager.fatal("Unexpected thread interruption");
			}
		}
		this.resourceTokens.put(resource, null);

		Iterator<ObjectValue<AllocatableAction>> executableActionsIterator = this.unassignedReadyActions.get(resource)
				.iterator();
		HashSet<ObjectValue<AllocatableAction>> objectValueToErase = new HashSet<ObjectValue<AllocatableAction>>();
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.TRY_TO_LAUNCH.getId(), Tracer.Event.TRY_TO_LAUNCH.getType());
        }
		while (executableActionsIterator.hasNext() && !this.availableWorkers.isEmpty()) {
			ObjectValue<AllocatableAction> obj = executableActionsIterator.next();
			AllocatableAction freeAction = obj.getObject();
			try {
				freeAction.tryToSchedule(obj.getScore(), this.availableWorkers);
				ResourceScheduler<? extends WorkerResourceDescription> assignedResource = freeAction
						.getAssignedResource();
				tryToLaunch(freeAction);
				if (!assignedResource.canRunSomething()) {
					this.availableWorkers.remove(assignedResource);
				}
				objectValueToErase.add(obj);
			} catch (BlockedActionException e) {
				objectValueToErase.add(obj);
				addToBlocked(freeAction);
			} catch (UnassignedActionException e) {
				// Nothing to be done here since the action was already in the scheduler
				// structures. If there is an exception, the freeAction will not be added
				// to the objectValueToErase list.
				// Hence, this is not an ignored Exception but an expected behavior.
			}
		}
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.Event.TRY_TO_LAUNCH.getType());
        }
		
		for (ObjectValue<AllocatableAction> obj : objectValueToErase) {
			AllocatableAction action = obj.getObject();
			removeActionFromSchedulerStructures(action);
		}
	}
}
