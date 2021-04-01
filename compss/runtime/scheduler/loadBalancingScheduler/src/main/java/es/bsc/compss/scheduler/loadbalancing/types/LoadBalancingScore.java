/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.loadbalancing.types;

import es.bsc.compss.scheduler.types.Score;


/**
 * A score for the load balancing scheduler.
 */
public class LoadBalancingScore extends Score {

    /**
     * Creates a new score instance.
     *
     * @param priority The priority of the action.
     * @param multiNodeGroupId The MultiNodeGroup Id of the action.
     * @param resourceScore The score of the resource (e.g., number of data in that resource)
     * @param waitingScore The estimated time of wait in the resource.
     * @param implementationScore Implementation's score.
     */
    public LoadBalancingScore(long priority, long multiNodeGroupId, long resourceScore, long waitingScore,
        long implementationScore) {

        super(priority, multiNodeGroupId, resourceScore, waitingScore, implementationScore);
    }

    /**
     * Creates a copy of the {@code clone} LoadBalancingScore.
     *
     * @param clone Score to clone.
     */
    public LoadBalancingScore(LoadBalancingScore clone) {
        super(clone);
    }

    @Override
    public boolean isBetterCustomValues(Score reference) {
        // The order is different from the default Score
        LoadBalancingScore other = (LoadBalancingScore) reference;
        if (this.resourceScore != other.resourceScore) {
            return this.resourceScore > other.resourceScore;
        }
        if (this.implementationScore != other.implementationScore) {
            return this.implementationScore > other.implementationScore;
        }
        return this.waitingScore > other.waitingScore;
    }

    @Override
    public String toString() {
        return "[LoadBalancingScore = [" + "Priority: " + this.priority + ", " + "MultiNodeGroupId: "
            + this.actionGroupPriority + ", " + "Resource: " + this.resourceScore + ", " + "Waiting: "
            + this.waitingScore + ", " + "Implementation: " + this.implementationScore + "]" + "]";
    }

}
