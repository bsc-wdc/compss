package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.types.Score;


public class FIFOScore extends Score {

    /**
     * Creates a new FIFOScore from the given values
     * 
     * @param actionScore
     * @param waiting
     * @param res
     * @param impl
     */
    public FIFOScore(double actionScore, double waiting, double res, double impl) {
        super(actionScore, waiting, res, impl);
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
    public String toString() {
        return "[FIFOScore = [action:" + this.actionScore + ", resource:" + this.resourceScore + ", load:" + this.waitingScore
                + ", implementation:" + this.implementationScore + "]";
    }

}
