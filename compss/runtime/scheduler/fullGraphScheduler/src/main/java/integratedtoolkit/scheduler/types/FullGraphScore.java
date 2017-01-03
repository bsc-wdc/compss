package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.LinkedList;


public class FullGraphScore<P extends Profile, T extends WorkerResourceDescription> extends Score {

    /*
     * ActionScore -> task Priority expectedDataAvailable -> expected time when data dependencies will be ready (take
     * into account transfers) resourceScore -> Expected ResourceAvailability implementationScore -> ExecutionTime
     */
    private final double expectedDataAvailable;
    private double expectedStart;


    public FullGraphScore(double actionScore, double dataAvailability, double waiting, double res, double impl) {
        super(actionScore, waiting, res, impl);
        expectedDataAvailable = dataAvailability;
        expectedStart = Math.max(resourceScore, expectedDataAvailable);
    }

    public FullGraphScore(FullGraphScore<P, T> actionScore, double transferTime, double waiting, double resourceTime, double impl) {
        super(actionScore.getActionScore(), waiting, resourceTime, impl);
        expectedDataAvailable = actionScore.expectedDataAvailable + transferTime;
        expectedStart = Math.max(resourceScore, expectedDataAvailable);
    }

    @Override
    public boolean isBetter(Score other) {
        FullGraphScore<P, T> otherDS = (FullGraphScore<P, T>) other;
        if (actionScore != other.actionScore) {
            return actionScore > other.actionScore;
        }
        double ownEnd = expectedStart + implementationScore;
        double otherEnd = otherDS.expectedStart + other.implementationScore;
        return ownEnd < otherEnd;
    }

    public static long getActionScore(AllocatableAction action) {
        return action.getPriority();
    }

    public long getDataPredecessorTime(LinkedList<AllocatableAction<P, T>> predecessors) {
        long dataTime = 0;
        for (AllocatableAction<P, T> pred : predecessors) {
            dataTime = Math.max(dataTime, ((FullGraphSchedulingInformation<P, T>) pred.getSchedulingInfo()).getExpectedEnd());
        }
        return dataTime;
    }

    public double getActionScore() {
        return actionScore;
    }

    public double getExpectedDataAvailable() {
        return expectedDataAvailable;
    }

    public double getResourceScore() {
        return resourceScore;
    }

    public double getExpectedStart() {
        return expectedStart;
    }

    public double getImplementationScore() {
        return implementationScore;
    }

    @Override
    public String toString() {
        return "action " + actionScore + " availableData " + expectedDataAvailable + " resource " + resourceScore + " expectedStart "
                + expectedStart + " implementation" + implementationScore;
    }

}
