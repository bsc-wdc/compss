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
    public abstract Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl,
            Score resourceScore);

    @Override
    public abstract String toString();

}
