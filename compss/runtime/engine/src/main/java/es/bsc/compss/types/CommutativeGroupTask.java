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
package es.bsc.compss.types;

import es.bsc.compss.types.data.DataAccessId;

import java.util.LinkedList;
import java.util.List;


public class CommutativeGroupTask extends AbstractTask {

    private final CommutativeIdentifier comId;

    // Tasks that access the data
    private final List<Task> commutativeTasks;

    private AbstractTask parentDataDependency;
    private int executionCount;

    // Version control
    private int finalVersion;
    private LinkedList<DataAccessId> versions;
    private DataAccessId registeredVersion;

    // Task currently being executed
    private boolean currentlyExecuting;
    private int taskExecuting;

    private boolean graphDrawn;


    /**
     * Creates a new CommutativeTaskGroup instance.
     * 
     * @param appId Application Id.
     * @param comId Commutative group identifier.
     */
    public CommutativeGroupTask(Long appId, CommutativeIdentifier comId) {
        super(appId);
        this.commutativeTasks = new LinkedList<Task>();
        this.finalVersion = 0;
        this.currentlyExecuting = false;
        this.executionCount = 0;
        this.versions = new LinkedList<>();
        this.registeredVersion = null;
        this.comId = comId;
        this.graphDrawn = false;
        this.taskExecuting = 0;
    }

    /**
     * Returns the commutative tasks associated to the group.
     * 
     * @return The commutative tasks associated to the group.
     */
    public List<Task> getCommutativeTasks() {
        return this.commutativeTasks;
    }

    /**
     * Returns the commutative identifier.
     * 
     * @return The commutative identifier.
     */
    public CommutativeIdentifier getCommutativeIdentifier() {
        return this.comId;
    }

    /**
     * Adds commutative task to group.
     *
     * @param task Task to add.
     */
    public void addCommutativeTask(Task task) {
        this.commutativeTasks.add(task);
    }

    /**
     * Sets the final version.
     *
     * @param version Final version.
     */
    public void setFinalVersion(int version) {
        this.finalVersion = version;
    }

    /**
     * Removes predecessor from group.
     *
     * @param t Predecessor to remove.
     */
    public void removePredecessor(Task t) {
        super.getPredecessors().remove(t);
    }

    /**
     * Group starts processing execution.
     *
     * @param taskId Task being executed.
     */
    public void taskBeingExecuted(int taskId) {
        this.currentlyExecuting = true;
        this.taskExecuting = taskId;
    }

    /**
     * The group ends processing execution.
     */
    public void taskEndedExecution() {
        this.currentlyExecuting = false;
        this.taskExecuting = 0;
    }

    /**
     * Adds version to list of versions.
     */
    public void addVersionToList(DataAccessId daId) {
        this.versions.add(daId);
    }

    /**
     * Returns the current version.
     */
    public DataAccessId getRegisteredVersion() {
        if (this.registeredVersion == null) {
            this.registeredVersion = this.versions.get(0);
        }
        return this.registeredVersion;
    }

    /**
     * Sets the current version.
     *
     * @param daId Data Id of the current version.
     */
    public void setRegisteredVersion(DataAccessId daId) {
        this.registeredVersion = daId;
    }

    /**
     * Changes the current version of the data.
     */
    public void nextVersion() {
        if (!this.versions.isEmpty()) {
            this.registeredVersion = this.versions.getFirst();
            this.versions.remove(this.registeredVersion);
            for (Task t : this.commutativeTasks) {
                t.setVersion(this.registeredVersion);
            }
        }
    }

    /**
     * Sets the graph of the group as drawn.
     */
    public void setGraphDrawn() {
        this.graphDrawn = true;
    }

    /**
     * Returns whether the graph of the group has been drawn or not.
     *
     * @return {@literal true} if the group has already been drawn, {@literal false} otherwise.
     */
    public boolean getGraphDrawn() {
        return this.graphDrawn;
    }

    /**
     * Returns the number of executions of the group.
     *
     * @return The number of executions of the group.
     */
    public int getExecutionCount() {
        return this.executionCount;
    }

    /**
     * Increases the number of executions of the group.
     */
    public void increaseExecutionCount() {
        this.executionCount = this.executionCount + 1;
    }

    /**
     * Returns the parent task causing a data dependency.
     *
     * @return The parent task causing a data dependency.
     */
    public AbstractTask getParentDataDependency() {
        return this.parentDataDependency;
    }

    /**
     * Sets parent task.
     *
     * @param t Parent task.
     */
    public void setParentDataDependency(AbstractTask t) {
        this.parentDataDependency = t;
    }

    /**
     * Returns whether the group is processing the given task or not.
     *
     * @param taskId Task to ask if it is being executed.
     * @return {@literal true} if the group is executing the given task, {@literal false} otherwise.
     */
    public boolean processingExecution(int taskId) {
        if (this.currentlyExecuting) {
            if (taskId == this.taskExecuting) {
                return false;
            }
        }
        return this.currentlyExecuting;
    }

    /**
     * Returns the final version of the data.
     *
     * @return The final version of the data.
     */
    public int getFinalVersion() {
        return this.finalVersion;
    }

    @Override
    public String getDotDescription() {
        return null;
    }

    @Override
    public String getLegendDescription() {
        return null;
    }

    @Override
    public String getColor() {
        return null;
    }
}
