package integratedtoolkit.scheduler.readyScheduler;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import org.json.JSONObject;

/**
 * Implementation for the ReadyResourceScheduler
 *
 * @param <T>
 */
public abstract class ReadyResourceScheduler< T extends WorkerResourceDescription> extends ResourceScheduler<T> {

    /**
     * New ready resource scheduler instance
     *
     * @param w
     * @param json
     */
    public ReadyResourceScheduler(Worker<T> w, JSONObject json) {
        super(w, json);
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
