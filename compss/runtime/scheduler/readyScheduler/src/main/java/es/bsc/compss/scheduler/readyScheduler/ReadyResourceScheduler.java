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
package es.bsc.compss.scheduler.readyScheduler;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import org.json.JSONObject;


/**
 * Implementation for the ReadyResourceScheduler
 *
 * @param <T>
 */
public abstract class ReadyResourceScheduler<T extends WorkerResourceDescription> extends ResourceScheduler<T> {

    /**
     * New ready resource scheduler instance
     *
     * @param w
     * @param resJSON
     * @param implJSON
     */
    public ReadyResourceScheduler(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
        super(w, resJSON, implJSON);
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

}
