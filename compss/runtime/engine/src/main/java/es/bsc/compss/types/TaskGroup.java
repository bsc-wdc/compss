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

import es.bsc.compss.worker.COMPSsException;

import java.util.LinkedList;
import java.util.List;

public class TaskGroup implements AutoCloseable {
    
    // Tasks that access the data
    private final List<Task> tasks;
    
    private String name;
    
    private boolean graphDrawn;
    
    private COMPSsException exception;
    
    private boolean implicitBarrier;
    
    /**
     * Creates a task group.
     * 
     * @param groupName Name of the group.
     */
    public TaskGroup(String groupName, boolean barrier) {
        this.tasks = new LinkedList<Task>();
        this.graphDrawn = false;
        this.name = groupName;
        this.implicitBarrier = barrier;
    }
    
    /**
     * Returns commutative tasks of group.
     * 
     * @return
     */
    public List<Task> getTasks() {
        return tasks;
    }

    /**
     * Returns commutative tasks of group.
     * 
     * @return
     */
    public String getName() {
        return this.name;
    }
    
    /**
     * Adds task to group.
     *
     * @param task Task to add to the group
     */
    public void addTask(Task task) {
        tasks.add(task);
    }

    /**
     * Sets the graph of the group as drawn.
     *
     */
    public void setGraphDrawn() {
        this.graphDrawn = true;
    }
    
    /**
     * Returns if the graph of the group has been drawn.
     *
     * @return
     */
    public boolean getGraphDrawn() {
        return this.graphDrawn;
    }
    
    /**
     * Removes a task from the group.
     * 
     * @param t Task to remove.
     */
    public void removeTask(Task t) {
        this.tasks.remove(t);
    }
    
    /**
     * Returns a boolean stating if the group has pending tasks to execute.
     * 
     * @return
     */
    public boolean hasPendingTasks() {
        return !this.tasks.isEmpty();
    }
    
    @Override
    public void close() throws Exception {
        
        
    }

    /**
     * Returns if there has been any task throwing a COMPSs Exception.
     * 
     * @return
     */
    public boolean hasException() {
        return exception != null;
    }

    /**
     * Returns the COMPSs Exception.
     * 
     * @return
     */
    public COMPSsException getException() {
        return exception;
    }

    /**
     * A task of the group has raised a COMPSsException.
     * 
     */
    public void setException(COMPSsException e) {
        this.exception = e;
        
    }

    public boolean hasImplicitBarrier() {
        return implicitBarrier;
    }

}

