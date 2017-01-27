package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Resource;


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
    
    @Override
    public boolean isBetter(Score other){
        if (this.actionScore != other.actionScore) {
            return this.actionScore > other.actionScore;
        }
        return this.implementationScore > other.implementationScore;
    }
    
    public static double calculateScore(TaskDescription params, Resource w) {
        return params.getId();
    }

}
