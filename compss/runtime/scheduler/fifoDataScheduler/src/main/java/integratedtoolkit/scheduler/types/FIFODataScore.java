package integratedtoolkit.scheduler.types;

import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Worker;


public class FIFODataScore extends ReadyScore {

    /**
     * Creates a new FIFOScore from the given values
     * 
     * @param actionScore
     * @param waiting
     * @param res
     * @param impl
     */
    public FIFODataScore(double actionScore, double res, double waiting, double impl) {
        super(actionScore, res, waiting, impl);
    }

    /**
     * Creates a copy of @clone FIFOScore
     * 
     * @param clone
     */
    public FIFODataScore(FIFODataScore clone) {
        super(clone);
    }

    /**
     * Calculates the score for a given worker @w and a given parameters @params
     * 
     * @param params
     * @param w
     * @return
     */
    @Override
    public double calculateResourceScore(TaskDescription params, Worker<?, ?> w) {
        return (double) -params.getId();
    }

    @Override
    public String toString() {
        return "[FIFODataScore = [action:" + actionScore + ", resource:" + resourceScore + ", load:" + waitingScore + ", implementation:"
                + implementationScore + "]";
    }

}
