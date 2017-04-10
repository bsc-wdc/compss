package integratedtoolkit.scheduler.types;

import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Worker;


public class LIFOScore extends ReadyScore {

    /**
     * New LIFOScore
     * 
     * @param actionScore
     * @param waiting
     * @param res
     * @param impl
     */
    public LIFOScore(double actionScore, double res, double waiting, double impl) {
        super(actionScore, res, waiting, impl);
    }

    /**
     * Clone a LIFOScore
     * 
     * @param clone
     */
    public LIFOScore(LIFOScore clone) {
        super(clone);
    }

    @Override
    public String toString() {
        return "[LIFOScore = [action:" + actionScore + ", resource:" + resourceScore + ", load:" + waitingScore + ", implementation:"
                + implementationScore + "]" + "]";

    }

    @Override
    public double calculateResourceScore(TaskDescription params, Worker<?, ?> w) {
        return (double) params.getId();
    }

}
