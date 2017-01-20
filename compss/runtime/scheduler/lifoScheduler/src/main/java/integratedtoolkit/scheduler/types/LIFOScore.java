package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.types.Score;


public class LIFOScore extends Score {

    /**
     * New LIFOScore
     * 
     * @param actionScore
     * @param waiting
     * @param res
     * @param impl
     */
    public LIFOScore(double actionScore, double waiting, double res, double impl) {
        super(actionScore, waiting, res, impl);
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

}
