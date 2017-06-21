package integratedtoolkit.scheduler.multiobjective.types;

import integratedtoolkit.scheduler.multiobjective.MOSchedulingInformation;
import integratedtoolkit.scheduler.multiobjective.config.MOConfiguration;
import integratedtoolkit.scheduler.multiobjective.config.MOConfiguration.OptimizationParameter;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Score;

import java.util.LinkedList;

public class MOScore extends Score {

    private static final OptimizationParameter OPTIMIZATION_PARAM = MOConfiguration.getSchedulerOptimization();
    /*
     * actionScore -> task Priority
     * resourceScore -> Expected Resource Availability
     * expectedDataAvailable -> expected time when data dependencies will be ready (take into account transfers)
     * waitingScore --> Expected time when the execution would start
     * implementationScore -> ExecutionTime
     * expectedCost -> Expected monetary cost of the execution
     * expectedEnergy -> Expected energy consumption of the execution
     */
    private final long expectedDataAvailable;
    private final double expectedCost;
    private final double expectedEnergy;

    public MOScore(long taskPriority, long dataAvailability, long resourceAvailability, long execTime, double energy, double cost) {
        super(taskPriority, resourceAvailability, Math.max(resourceAvailability, dataAvailability), execTime);
        expectedDataAvailable = dataAvailability;
        expectedCost = cost;
        expectedEnergy = energy;
    }

    @Override
    public boolean isBetter(Score other) {
        MOScore otherDS = (MOScore) other;
        if (actionScore != otherDS.actionScore) {
            return actionScore > otherDS.actionScore;
        }

        double diffCost = expectedCost - otherDS.expectedCost;
        double diffEnergy = expectedEnergy - otherDS.expectedEnergy;
        long ownEnd = waitingScore + implementationScore;
        long otherEnd = otherDS.waitingScore + otherDS.implementationScore;
        long diffEnd = ownEnd - otherEnd;
        switch (OPTIMIZATION_PARAM) {
            case COST:
                if (diffCost == 0) {
                    if (diffEnd == 0) {
                        return diffEnergy < 0;
                    } else {
                        return diffEnd < 0;
                    }
                } else {
                    return diffCost < 0;
                }
            case ENERGY:
                if (diffEnergy == 0) {
                    if (diffEnd == 0) {
                        return diffCost < 0;
                    } else {
                        return diffEnd < 0;
                    }
                } else {
                    return diffEnergy < 0;
                }
            default:
                if (diffEnd == 0) {
                    if (diffEnergy == 0) {
                        return diffCost < 0;
                    } else {
                        return diffEnergy < 0;
                    }
                } else {
                    return diffEnd < 0;
                }
        }
    }

    public static long getActionScore(AllocatableAction action) {
        return action.getPriority();
    }

    public long getDataPredecessorTime(LinkedList<AllocatableAction> predecessors) {
        long dataTime = 0;
        for (AllocatableAction pred : predecessors) {
            dataTime = Math.max(dataTime, ((MOSchedulingInformation) pred.getSchedulingInfo()).getExpectedEnd());
        }
        return dataTime;
    }

    public long getExpectedDataAvailable() {
        return expectedDataAvailable;
    }

    public long getExpectedStart() {
        return this.waitingScore;
    }

    @Override
    public String toString() {
        return "[MOScore = ["
                + "Action Priority:" + this.actionScore + ", "
                + "Resource Availability:" + this.resourceScore + ", "
                + "Data Availability:" + this.expectedDataAvailable + ", "
                + "Expected Start Timestamp:" + this.waitingScore + ", "
                + "Expected Execution Time:" + this.implementationScore + ", "
                + "Expected Execution Consumption:" + this.expectedEnergy + ", "
                + "Expected Execution Cost:" + this.expectedCost
                + "]" + "]";
    }
}
