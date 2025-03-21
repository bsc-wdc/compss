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

package es.bsc.compss.checkpoint.types;

import es.bsc.compss.types.Task;
import java.util.LinkedList;
import java.util.List;


public class CheckpointTask {

    // Task being checkpointed
    private final Task task;

    // Data versions being read by the tasks
    private final List<CheckpointDataVersion> readValues;

    // Number of pending results of the task to be checkpointed
    private int pendingValuesToCP;


    /**
     * Finish copying data parameter.
     *
     * @param task Reader of the data.
     * @param pendingValuesToCP Number of remaining values of the task to read.
     */
    public CheckpointTask(Task task, int pendingValuesToCP) {
        this.readValues = new LinkedList<>();
        this.task = task;
        this.pendingValuesToCP = pendingValuesToCP;
    }

    public synchronized void addOutputToCheckpoint() {
        this.pendingValuesToCP++;
    }

    /**
     * Remove one pending value, if it reaches zero, then writes task as checkpointed.
     * 
     * @return {@literal true} if all the outputs of the task have been checkpointed
     **/
    public boolean checkpointedOutput() {
        pendingValuesToCP--;
        return pendingValuesToCP == 0;
    }

    /**
     * Adds a data version to be read by the task.
     *
     * @param readValue DataVersion to be read by the task
     **/
    public void addReadValue(CheckpointDataVersion readValue) {
        readValues.add(readValue);
    }

    public List<CheckpointDataVersion> getReadValues() {
        return readValues;
    }

    /**
     * Returns the task.
     * 
     * @return returns task whose checkpointing is controlled
     **/
    public Task getTask() {
        return task;
    }

}
