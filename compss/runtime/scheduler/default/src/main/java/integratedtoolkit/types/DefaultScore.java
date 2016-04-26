package integratedtoolkit.types;

import integratedtoolkit.scheduler.defaultscheduler.DefaultSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import java.util.LinkedList;

public class DefaultScore extends Score {

    /*
     * ActionScore -> task Priority
     * expectedDataAvailable -> expected time when data dependencies will be ready (take into account transfers)
     * resourceScore -> Expected ResourceAvailability
     * implementationScore -> ExecutionTime
     */
    private final long expectedDataAvailable;
    private long expectedStart;

    public DefaultScore(long actionScore, long dataAvailability, long res, long impl) {
        super(actionScore, res, impl);
        expectedDataAvailable = dataAvailability;
        expectedStart = Math.max(resourceScore, expectedDataAvailable);
    }

    public DefaultScore(DefaultScore actionScore, long transferTime, long resourceTime, long impl) {
        super(actionScore, resourceTime, impl);
        expectedDataAvailable = actionScore.expectedDataAvailable + transferTime;
        expectedStart = Math.max(resourceScore, expectedDataAvailable);
    }

    @Override
    public boolean isBetter(Score other) {
        DefaultScore otherDS = (DefaultScore) other;
        if (actionScore != other.actionScore) {
            return actionScore > other.actionScore;
        }
        long ownEnd = expectedStart + implementationScore;
        long otherEnd = otherDS.expectedStart + other.implementationScore;
        return ownEnd < otherEnd;
    }

    public static long getActionScore(TaskParams params) {
        return params.hasPriority() ? 1l : 0l;
    }

    public static long getDataPredecessorTime(LinkedList<AllocatableAction> predecessors) {
        long dataTime = 0;
        for (AllocatableAction pred : predecessors) {
            dataTime = Math.max(dataTime, ((DefaultSchedulingInformation) pred.getSchedulingInfo()).getExpectedEnd());
        }
        return dataTime;
    }

    public long getActionScore() {
        return actionScore;
    }

    public long getExpectedDataAvailable() {
        return expectedDataAvailable;
    }

    public long getResourceScore() {
        return resourceScore;
    }

    public long getExpectedStart() {
        return expectedStart;
    }

    public long getImplementationScore() {
        return implementationScore;
    }

    public String toString() {
        return "action " + actionScore + " availableData " + expectedDataAvailable + " resource " + resourceScore + " expectedStart " + expectedStart + " implementation" + implementationScore;
    }
}
