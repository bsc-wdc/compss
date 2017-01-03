package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Worker;


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

    /**
     * Calculates the score for a given worker @w and a given parameters @params
     * 
     * @param params
     * @param w
     * @return
     */
    public static double calculateScore(TaskDescription params, Worker<?> w) {
        return (1.0 / (double) params.getId());
    }

    @Override
    public boolean isBetter(Score other) {
        if (actionScore != other.actionScore) {
            return actionScore > other.actionScore;
        }
        return implementationScore > other.implementationScore;
    }

    @Override
    public String toString() {
        return "[FIFOScore = [action:" + actionScore + ", resource:" + resourceScore + ", load:" + waitingScore + ", implementation:"
                + implementationScore + "]";
    }

}
