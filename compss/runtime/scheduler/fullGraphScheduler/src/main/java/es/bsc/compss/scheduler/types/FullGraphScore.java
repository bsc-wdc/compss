/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;

import java.util.List;


public class FullGraphScore extends Score {

    /*
     * ActionScore -> task Priority expectedDataAvailable -> expected time when data dependencies will be ready (take
     * into account transfers) resourceScore -> Expected ResourceAvailability implementationScore -> ExecutionTime
     */
    private final double expectedDataAvailable;
    private double expectedStart;


    public FullGraphScore(long actionScore, double dataAvailability, long waiting, long res, long impl) {
        super(actionScore, res, waiting, impl);
        this.expectedDataAvailable = dataAvailability;
        this.expectedStart = Math.max(this.resourceScore, this.expectedDataAvailable);
    }

    public FullGraphScore(FullGraphScore actionScore, double transferTime, long waiting, long resourceTime, long impl) {
        super(actionScore.getActionScore(), resourceTime, waiting, impl);
        this.expectedDataAvailable = actionScore.expectedDataAvailable + transferTime;
        this.expectedStart = Math.max(this.resourceScore, this.expectedDataAvailable);
    }

    @Override
    public boolean isBetter(Score other) {
        FullGraphScore otherDS = (FullGraphScore) other;
        if (this.actionScore != other.actionScore) {
            return this.actionScore > other.actionScore;
        }
        double ownEnd = this.expectedStart + this.implementationScore;
        double otherEnd = otherDS.expectedStart + other.implementationScore;
        return ownEnd < otherEnd;
    }

    public static long getActionScore(AllocatableAction action) {
        return action.getPriority();
    }

    public long getDataPredecessorTime(List<AllocatableAction> predecessors) {
        long dataTime = 0;
        for (AllocatableAction pred : predecessors) {
            dataTime = Math.max(dataTime, ((FullGraphSchedulingInformation) pred.getSchedulingInfo()).getExpectedEnd());
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
        return "[FGScore = [action: " + this.actionScore + ", availableData: " + this.expectedDataAvailable
                + ", resource: " + this.resourceScore + ", expectedStart: " + this.expectedStart + ", implementation:"
                + this.implementationScore + "]";
    }

}
