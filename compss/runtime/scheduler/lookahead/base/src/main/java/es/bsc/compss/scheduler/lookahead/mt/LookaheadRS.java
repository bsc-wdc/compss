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
package es.bsc.compss.scheduler.lookahead.mt;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Future;

import org.json.JSONObject;


/**
 * Implementation for the LookaheadRS.
 *
 * @param <T> Worker Resource Description.
 */
public abstract class LookaheadRS<T extends WorkerResourceDescription> extends ResourceScheduler<T> {

    protected final Set<ObjectValue<AllocatableAction>> unassignedActions;
    protected final Map<AllocatableAction, ObjectValue<AllocatableAction>> addedActions;
    private Future<?> token;


    /**
     * New Ready Resource Scheduler instance.
     *
     * @param w Associated worker.
     * @param resJSON Worker JSON description.
     * @param implJSON Implementation JSON description.
     */
    public LookaheadRS(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
        super(w, resJSON, implJSON);
        unassignedActions = new TreeSet<>();
        addedActions = new HashMap<>();
        token = null;
    }

    public Set<ObjectValue<AllocatableAction>> getUnassignedActions() {
        return unassignedActions;
    }

    public Map<AllocatableAction, ObjectValue<AllocatableAction>> getAddedActions() {
        return addedActions;
    }

    public Future<?> getToken() {
        return token;
    }

    public void setToken(Future<?> token) {
        this.token = token;
    }

    /**
     * Adds an action to the RS structures.
     * 
     * @param action Action to add
     * @param actionScore current score for the action
     * @return {@literal true}, if the action has been properly added to the RS structure; {@literal false}, otherwise
     */
    public boolean addAction(AllocatableAction action, Score actionScore) {
        Score fullScore = action.schedulingScore(this, actionScore);
        ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
        boolean added = false;
        synchronized (unassignedActions) {
            added = unassignedActions.add(obj);
        }
        if (added) {
            addedActions.put(action, obj);
        }
        return added;
    }

    /**
     * Removes an action from the RS structures.
     * 
     * @param action Action to remove
     * @return {@literal true}, if the action has been properly removed from the RS structure; {@literal false},
     *         otherwise
     */
    public boolean removeAction(AllocatableAction action) {
        ObjectValue<AllocatableAction> obj = addedActions.remove(action);
        boolean removed = false;
        if (obj != null) {
            synchronized (unassignedActions) {
                removed = unassignedActions.remove(obj);// NOSONAR
            }
        }
        return removed;
    }

    /**
     * Updates an action on the RS structures.
     * 
     * @param action Action to update
     * @param actionScore new score for the action
     */
    public void updateAction(AllocatableAction action, Score actionScore) {
        ObjectValue<AllocatableAction> obj = addedActions.remove(action);
        if (obj != null) {
            boolean removed = false;
            synchronized (unassignedActions) {
                removed = unassignedActions.remove(obj);// NOSONAR
            }
            if (removed) {
                Score fullScore = action.schedulingScore(this, actionScore);
                obj = new ObjectValue<>(action, fullScore);
                boolean added = false;
                synchronized (unassignedActions) {
                    added = unassignedActions.add(obj);
                }
                if (added) {
                    addedActions.put(action, obj);
                }
            }
        }
    }

    @Override
    public abstract Score generateBlockedScore(AllocatableAction action);

    @Override
    public abstract Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore);

    @Override
    public abstract Score generateImplementationScore(AllocatableAction action, TaskDescription params,
        Implementation impl, Score resourceScore);

    @Override
    public abstract String toString();

    /**
     * Clear unassigned and added actions.
     */
    @Override
    public final void clear() {
        unassignedActions.clear();
        addedActions.clear();
        token = null;
        super.clear();
    }

}
