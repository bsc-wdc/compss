package integratedtoolkit.scheduler.types;

import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Worker;


public class FIFOScore extends ReadyScore {

    /**
     * Creates a new FIFOScore from the given values
     * 
     * @param actionScore
     * @param waiting
     * @param res
     * @param impl
     */
    public FIFOScore(double actionScore, double res, double waiting, double impl) {
        super(actionScore, res, waiting, impl);
    }

    /**
     * Creates a copy of @clone FIFOScore
     * 
     * @param clone
     */
    public FIFOScore(FIFOScore clone) {
        super(clone);
    }

    @Override
    public double calculateResourceScore(TaskDescription params, Worker<?, ?> w) {
        return (double) -params.getId();
    }

    @Override
    public String toString() {
        return "[FIFOScore = [action:" + this.actionScore + ", resource:" + this.resourceScore + ", load:" + this.waitingScore
                + ", implementation:" + this.implementationScore + "]";
    }

}
