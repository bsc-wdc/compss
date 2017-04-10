package integratedtoolkit.scheduler.types;

import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Worker;


public abstract class ReadyScore extends Score {

    public ReadyScore(double actionScore, double res, double waiting, double impl) {
        super(actionScore, res, waiting, impl);
    }

    public ReadyScore(ReadyScore clone) {
        super(clone);
    }

    @Override
    public abstract double calculateResourceScore(TaskDescription params, Worker<?, ?> w);

}
