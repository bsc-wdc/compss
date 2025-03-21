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
package es.bsc.compss.scheduler.types;

/**
 * Action score representation.
 */
public class Score implements Comparable<Score> {

    // TaskScheduler granted parameters
    protected long priority; // Action priority
    protected long actionGroupPriority; // Multi-Node action group id

    // Scheduler implementation parameters
    protected long resourceScore; // Resource Priority (e.g., data locatity)
    protected long waitingScore; // Resource Blocked Priority
    protected long implementationScore; // Implementation Priority


    /**
     * Creates a new score instance.
     *
     * @param priority The priority of the action.
     * @param actionGroupPriority The MultiNodeGroup Id of the action.
     * @param resourceScore The score of the resource (e.g., number of data in that resource)
     * @param waitingScore The estimated time of wait in the resource.
     * @param implementationScore Implementation's score.
     */
    public Score(long priority, long actionGroupPriority, long resourceScore, long waitingScore,
        long implementationScore) {

        this.priority = priority;
        this.actionGroupPriority = actionGroupPriority;

        this.resourceScore = resourceScore;
        this.waitingScore = waitingScore;
        this.implementationScore = implementationScore;
    }

    /**
     * Clones the given score.
     *
     * @param clone Score to clone.
     */
    public Score(Score clone) {
        this.priority = clone.priority;
        this.actionGroupPriority = clone.actionGroupPriority;
        this.resourceScore = clone.resourceScore;
        this.waitingScore = clone.waitingScore;
        this.implementationScore = clone.implementationScore;
    }

    /**
     * Returns the action's priority.
     * 
     * @return The action's priority.
     */
    public long getPriority() {
        return this.priority;
    }

    /**
     * Returns the action group's priority.
     * 
     * @return The action group's priority.
     */
    public long getGroupPriority() {
        return this.actionGroupPriority;
    }

    /**
     * Returns the score of the resource (number of data in that resource).
     *
     * @return The score of the resource (number of data in that resource).
     */
    public long getResourceScore() {
        return this.resourceScore;
    }

    /**
     * Returns the estimated time of wait in the resource.
     *
     * @return The estimated time of wait in the resource.
     */
    public long getWaitingScore() {
        return this.waitingScore;
    }

    /**
     * Returns the implementation score.
     *
     * @return The implementation score.
     */
    public long getImplementationScore() {
        return this.implementationScore;
    }

    /**
     * Checks whether a score is better than another.
     *
     * @param a Score to compare.
     * @param b Score to compare.
     * @return Returns {@literal true} if {@code a} is better than {@code b}, {@literal false} otherwise.
     */
    public static final boolean isBetter(Score a, Score b) {
        if (a == null) {
            return false;
        }
        if (b == null) {
            return true;
        }
        return a.isBetter(b);
    }

    /**
     * Checks if the current score is better than the given.
     *
     * @param other Score to compare.
     * @return Returns {@literal true} if {@code this} is better than {@code other}, {@literal false} otherwise.
     */
    public final boolean isBetter(Score other) {
        if (this.priority != other.priority) {
            return this.priority > other.priority;
        }
        if (this.actionGroupPriority != other.actionGroupPriority) {
            // Single actins have group -1
            // We always prioritize the single ones and the first generated groups
            return this.actionGroupPriority < other.actionGroupPriority;
        }
        return isBetterCustomValues(other);
    }

    /**
     * Checks if the current score is better than the given.
     *
     * @param other Score to compare.
     * @return Returns {@literal true} if {@code this} is better than {@code other}, {@literal false} otherwise.
     */
    public boolean isBetterCustomValues(Score other) {
        if (this.resourceScore != other.resourceScore) {
            return this.resourceScore > other.resourceScore;
        }
        if (this.waitingScore != other.waitingScore) {
            return this.waitingScore > other.waitingScore;
        }
        return this.implementationScore > other.implementationScore;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Long.hashCode(this.priority);
        result = 31 * result + Long.hashCode(this.actionGroupPriority);
        result = 31 * result + Long.hashCode(this.resourceScore);
        result = 31 * result + Long.hashCode(this.waitingScore);
        result = 31 * result + Long.hashCode(this.implementationScore);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Score) {
            Score other = (Score) obj;
            return (this.priority == other.priority && this.actionGroupPriority == other.actionGroupPriority
                && this.resourceScore == other.resourceScore && this.waitingScore == other.waitingScore
                && this.implementationScore == other.implementationScore);
        }

        return false;
    }

    @Override
    public int compareTo(Score other) {
        if (this.equals(other)) {
            return 0;
        } else if (this.isBetter(other)) {
            return 1;
        } else {
            return -1;
        }
    }

    @Override
    public String toString() {
        return "[Score = [" + "Priority: " + this.priority + ", " + "MultiNodeGroupId: " + this.actionGroupPriority
            + ", " + "Resource: " + this.resourceScore + ", " + "Waiting: " + this.waitingScore + ", "
            + "Implementation: " + this.implementationScore + "]" + "]";
    }

}
