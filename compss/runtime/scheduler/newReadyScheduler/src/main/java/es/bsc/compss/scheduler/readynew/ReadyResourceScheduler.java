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
package es.bsc.compss.scheduler.readynew;

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

import org.json.JSONObject;


/**
 * Implementation for the ReadyResourceScheduler.
 *
 * @param <T> Worker Resource Description.
 */
public abstract class ReadyResourceScheduler<T extends WorkerResourceDescription> extends ResourceScheduler<T> {

    protected Set<ObjectValue<AllocatableAction>> unassignedActions;
    protected Map<AllocatableAction, ObjectValue<AllocatableAction>> addedActions;


    /**
     * New Ready Resource Scheduler instance.
     *
     * @param w Associated worker.
     * @param resJSON Worker JSON description.
     * @param implJSON Implementation JSON description.
     */
    public ReadyResourceScheduler(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
        super(w, resJSON, implJSON);
        resetUnassignedActions();
    }

    public Set<ObjectValue<AllocatableAction>> getUnassignedActions() {
        return unassignedActions;
    }

    public Map<AllocatableAction, ObjectValue<AllocatableAction>> getAddedActions() {
        return addedActions;
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
    public void resetUnassignedActions() {
        unassignedActions = new TreeSet<>();
        addedActions = new HashMap<>();

    }

}
