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

import java.util.LinkedList;
import java.util.List;

import es.bsc.compss.types.data.DataAccessId;

public class CommutativeGroupTask extends AbstractTask{
    
    private final CommutativeIdentifier comId;    
    
    // Tasks that access the data
    private final List<Task> commutativeTasks;
    
    private AbstractTask parentDataDependency;
    int executionCount;
    
    // Version control
    private int finalVersion;
    private LinkedList<DataAccessId> versions;
    private DataAccessId registeredVersion;
    
    // Task currently being executed
    private boolean currentlyExecuting;
    private int taskExecuting;
    
    private boolean graphDrawn;
    
    public CommutativeGroupTask (Long appId, CommutativeIdentifier comId) {
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
     * Returns commutative tasks of group
     * 
     * @return
     */
    public List<Task> getCommutativeTasks () {
        return commutativeTasks;
    }
    
    /**
     * Returns commutative identifier
     * 
     *@return
     */
    public CommutativeIdentifier getCommutativeIdentifier() {
        return this.comId;
    }
    
    /**
     * Adds commutative task to group
     *
     * @param task
     */
    public void addCommutativeTask(Task task) {
        commutativeTasks.add(task);
    }
    
    /**
     * Sets final version
     *
     * @param version
     */
    public void setFinalVersion (int version) {
        this.finalVersion = version;
    }

    /**
     * Removes predecessor from group
     *
     * @param t
     */
    public void removePredecessor (Task t) {
        super.getPredecessors().remove(t);
    }
    
    /**
     * Group starts processing execution
     *
     * @param taskId
     */
    public void taskBeingExecuted(int taskId) {
        this.currentlyExecuting = true;
        this.taskExecuting = taskId;
    }
    
    /**
     * The group ends processing execution
     *
     */
    public void taskEndedExecution() {
        this.currentlyExecuting = false;
        this.taskExecuting = 0;
    }
    
    /**
     * Adds version to list of versions
     *
     */
    public void addVersionToList(DataAccessId daId) {
        this.versions.add(daId);
    }
    
    /**
     * Returns the current version
     *
     */
    public DataAccessId getRegisteredVersion() {
        if (registeredVersion == null) {
            registeredVersion = versions.get(0);
        }
        return registeredVersion;
    }
    
    /**
     * Sets the current version
     *
     * @param daId
     */
    public void setRegisteredVersion(DataAccessId daId) {
        registeredVersion = daId;
    }
    
    /**
     * Changes the current version of the data
     *
     */
    public void nextVersion() {
        if (!versions.isEmpty()) {
            this.registeredVersion = versions.getFirst();
            versions.remove(this.registeredVersion);
            for (Task t : commutativeTasks) {
                t.setVersion(registeredVersion);
            }
        }
    }
    
    /**
     * Sets the graph of the group as drawn
     *
     */
    public void setGraphDrawn() {
        this.graphDrawn = true;
    }
    
    /**
     * Returns if the graph of the group has been drawn
     *
     * @return
     */
    public boolean getGraphDrawn() {
        return this.graphDrawn;
    }
    
    /**
     * Returns the executions of the task
     *
     * @return
     */
    public int getExecutionCount() {
        return executionCount;
    }

    /**
     * Returns the executions of the task
     *
     * @return
     */
    public void increaseExecutionCount() {
        executionCount = executionCount + 1;
    }
    @Override
    public String getDotDescription() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getLegendDescription() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getColor() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
    * Returns the dependency with parent task
    *
    * @return
    */
    public AbstractTask getParentDataDependency() {
        return parentDataDependency;
    }
    
    /**
    * Sets parent task
    *
    * @param t
    */
    public void setParentDataDependency(AbstractTask t) {
        parentDataDependency = t;
    }

    /**
    * Returns if group is processing execution
    *
    *@param taskId
    * @return
    */
    public boolean processingExecution(int taskId) {
        if (currentlyExecuting) {
            if (taskId == this.taskExecuting) {
                return false;
            }
        }
        return currentlyExecuting;    
    }
    
    /**
    * Returns the final version of the data
    *
    * @return
    */
    public int getFinalVersion() {
        return finalVersion;
    }
}

