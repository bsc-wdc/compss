package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Worker;


public class LIFOScore extends Score {

    public LIFOScore(double actionScore, double waiting, double res, double impl) {
        super(actionScore, waiting, res, impl);
    }

    public LIFOScore(LIFOScore clone) {
        super(clone);
    }

    @Override
    public boolean isBetter(Score other) {
        if (actionScore != other.actionScore) {
            return actionScore > other.actionScore;
        }
        return implementationScore > other.implementationScore;
    }

    public static double calculateScore(TaskDescription params, Worker<?> w) {
        return params.getId();
    }

    @Override
    public String toString() {
        /*
         * return "[FIFOScore = [action:" + actionScore + ", resource:" + resourceScore + ", load:" + waitingScore +
         * ", implementation:" + implementationScore + "]" + "]";
         */
        return "[action:" + actionScore + ", resource:" + resourceScore + ", load:" + waitingScore + ", implementation:"
                + implementationScore + "]";
    }

}
