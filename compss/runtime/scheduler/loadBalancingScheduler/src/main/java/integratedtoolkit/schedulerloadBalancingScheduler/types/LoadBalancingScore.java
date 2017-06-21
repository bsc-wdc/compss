package integratedtoolkit.schedulerloadBalancingScheduler.types;

import integratedtoolkit.scheduler.types.Score;

public class LoadBalancingScore extends Score {

    /**
     * Creates a new ResourceEmptyScore with the given values
     *
     * @param actionScore
     * @param waiting
     * @param res
     * @param impl
     */
    public LoadBalancingScore(long actionScore, long res, long waiting, long impl) {
        super(actionScore, res, waiting, impl);
    }

    /**
     * Creates a copy of the @clone ResourceEmptyScore
     *
     * @param clone
     */
    public LoadBalancingScore(LoadBalancingScore clone) {
        super(clone);
    }

    @Override
    public boolean isBetter(Score reference) {
        LoadBalancingScore other = (LoadBalancingScore) reference;
        if (this.actionScore != other.actionScore) {
            return this.actionScore > other.actionScore;
        }
        if (this.resourceScore != other.resourceScore) {
            return this.resourceScore > other.resourceScore;
        }
        if (this.implementationScore != other.implementationScore) {
            return this.implementationScore > other.implementationScore;
        }
        return this.waitingScore > other.waitingScore;
    }

    @Override
    public String toString() {
        return "[LoadBalancingScore = [action:" + actionScore + ", resource:" + resourceScore + ", load:" + waitingScore
                + ", implementation:" + implementationScore + "]" + "]";
    }

}
