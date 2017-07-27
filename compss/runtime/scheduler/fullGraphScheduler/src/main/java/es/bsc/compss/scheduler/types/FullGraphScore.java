package es.bsc.compss.scheduler.types;

import es.bsc.es.bsc.compss.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import es.bsc.es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.es.bsc.compss.scheduler.types.Profile;
import es.bsc.es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.LinkedList;


public class FullGraphScore<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> extends Score {

    /*
     * ActionScore -> task Priority expectedDataAvailable -> expected time when data dependencies will be ready (take
     * into account transfers) resourceScore -> Expected ResourceAvailability implementationScore -> ExecutionTime
     */
    private final double expectedDataAvailable;
    private double expectedStart;


    public FullGraphScore(double actionScore, double dataAvailability, double waiting, double res, double impl) {
        super(actionScore, res, waiting, impl);
        expectedDataAvailable = dataAvailability;
        expectedStart = Math.max(resourceScore, expectedDataAvailable);
    }

    public FullGraphScore(FullGraphScore<P, T, I> actionScore, double transferTime, double waiting, double resourceTime, double impl) {
        super(actionScore.getActionScore(), resourceTime, waiting, impl);
        expectedDataAvailable = actionScore.expectedDataAvailable + transferTime;
        expectedStart = Math.max(resourceScore, expectedDataAvailable);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isBetter(Score other) {
        FullGraphScore<P, T, I> otherDS = (FullGraphScore<P, T, I>) other;
        if (this.actionScore != other.actionScore) {
            return this.actionScore > other.actionScore;
        }
        double ownEnd = this.expectedStart + this.implementationScore;
        double otherEnd = otherDS.expectedStart + other.implementationScore;
        return ownEnd < otherEnd;
    }

    public static <P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> long getActionScore(
            AllocatableAction<P, T, I> action) {
        return action.getPriority();
    }

    public long getDataPredecessorTime(LinkedList<AllocatableAction<P, T, I>> predecessors) {
        long dataTime = 0;
        for (AllocatableAction<P, T, I> pred : predecessors) {
            dataTime = Math.max(dataTime, ((FullGraphSchedulingInformation<P, T, I>) pred.getSchedulingInfo()).getExpectedEnd());
        }
        return dataTime;
    }

    public double getExpectedDataAvailable() {
        return this.expectedDataAvailable;
    }

    public double getExpectedStart() {
        return this.expectedStart;
    }

    @Override
    public String toString() {
        return "[FGScore = [action: " + this.actionScore + ", availableData: " + this.expectedDataAvailable + ", resource: "
                + this.resourceScore + ", expectedStart: " + this.expectedStart + ", implementation:" + this.implementationScore + "]";
    }

}
