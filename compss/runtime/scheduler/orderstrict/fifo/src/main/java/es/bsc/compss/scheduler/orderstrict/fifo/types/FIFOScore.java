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
package es.bsc.compss.scheduler.orderstrict.fifo.types;

import es.bsc.compss.scheduler.types.Score;


public class FIFOScore extends Score {

    private final long actionId;


    /**
     * Creates a new score instance.
     *
     * @param priority The priority of the action.
     * @param actionGroupPriority The MultiNodeGroup Id of the action.
     * @param actionId The id of the allocatable action.
     * @param resourceScore The score of the resource (e.g., number of data in that resource)
     * @param waitingScore The estimated time of wait in the resource.
     * @param implementationScore Implementation's score.
     */
    public FIFOScore(long priority, long actionGroupPriority, long actionId, long resourceScore, long waitingScore,
        long implementationScore) {
        super(priority, actionGroupPriority, resourceScore, 0, implementationScore);
        this.actionId = actionId;
    }

    @Override
    public boolean isBetterCustomValues(Score other) {
        FIFOScore otherFIFO = (FIFOScore) other;
        if (this.actionId == otherFIFO.actionId) {
            return this.resourceScore > otherFIFO.resourceScore;
        } else {
            return this.actionId < otherFIFO.actionId;
        }
    }
}
