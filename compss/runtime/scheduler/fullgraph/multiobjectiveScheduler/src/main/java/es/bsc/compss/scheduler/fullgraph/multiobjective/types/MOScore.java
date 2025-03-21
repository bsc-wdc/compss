/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.scheduler.fullgraph.multiobjective.types;

import es.bsc.compss.scheduler.fullgraph.multiobjective.MOSchedulingInformation;
import es.bsc.compss.scheduler.fullgraph.multiobjective.config.MOConfiguration;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;

import java.util.List;


public class MOScore extends Score {

    /*
     * actionScore -> task Priority resourceScore -> Expected Resource Availability expectedDataAvailable -> expected
     * time when data dependencies will be ready (take into account transfers) waitingScore --> Expected time when the
     * execution would start implementationScore -> ExecutionTime expectedCost -> Expected monetary cost of the
     * execution expectedEnergy -> Expected energy consumption of the execution
     */
    private final long expectedDataAvailable;
    private final double expectedCost;
    private final double expectedEnergy;


    /**
     * Creates a new score instance.
     *
     * @param priority The priority of the action.
     * @param multiNodeGroupId The MultiNodeGroup Id of the action.
     * @param resourceScore The score of the resource (e.g., number of data in that resource)
     * @param dataAvailability Data availability score.
     * @param execTime Implementation's execution time.
     * @param energy Energy cost.
     * @param cost Money cost.
     */
    public MOScore(long priority, long multiNodeGroupId, long resourceScore, long dataAvailability, long execTime,
        double energy, double cost) {

        super(priority, multiNodeGroupId, resourceScore, Math.max(resourceScore, dataAvailability), execTime);

        this.expectedDataAvailable = dataAvailability;
        this.expectedCost = cost;
        this.expectedEnergy = energy;
    }

    @Override
    public boolean isBetterCustomValues(Score other) {
        MOScore otherDS = (MOScore) other;
        double diffCost = this.expectedCost - otherDS.expectedCost;
        double diffEnergy = this.expectedEnergy - otherDS.expectedEnergy;
        long ownEnd = this.waitingScore + this.implementationScore;
        long otherEnd = otherDS.waitingScore + otherDS.implementationScore;
        long diffEnd = ownEnd - otherEnd;
        switch (MOConfiguration.getSchedulerOptimization()) {
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

    /**
     * Returns the action score.
     * 
     * @param action Action
     * @return Action score.
     */
    public static long getActionScore(AllocatableAction action) {
        return action.getPriority();
    }

    /**
     * Returns the latest time of the data predecessors.
     * 
     * @param predecessors Action predecessors.
     * @return Latest time of the action predecessors.
     */
    public static long getDataPredecessorTime(List<AllocatableAction> predecessors) {
        long dataTime = 0;
        for (AllocatableAction pred : predecessors) {
            dataTime = Math.max(dataTime, ((MOSchedulingInformation) pred.getSchedulingInfo()).getExpectedEnd());
        }
        return dataTime;
    }

    /**
     * Returns the expected data available time.
     * 
     * @return The expected data available time.
     */
    public long getExpectedDataAvailable() {
        return this.expectedDataAvailable;
    }

    /**
     * Returns the expected start time.
     * 
     * @return The expected start time.
     */
    public long getExpectedStart() {
        return this.waitingScore;
    }

    @Override
    public String toString() {
        return "[MOScore = [" + "Priority: " + this.priority + ", " + "MultiNodeGroupId: " + this.actionGroupPriority
            + ", " + "Resource: " + this.resourceScore + ", " + "Data Availability:" + this.expectedDataAvailable + ", "
            + "Expected Start Timestamp:" + this.waitingScore + ", " + "Expected Execution Time:"
            + this.implementationScore + ", " + "Expected Execution Consumption:" + this.expectedEnergy + ", "
            + "Expected Execution Cost:" + this.expectedCost + "]" + "]";
    }

}
