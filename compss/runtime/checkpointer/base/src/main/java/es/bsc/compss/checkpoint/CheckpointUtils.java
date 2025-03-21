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
package es.bsc.compss.checkpoint;

import es.bsc.compss.checkpoint.types.CheckpointGroupImpl;
import es.bsc.compss.types.Task;


public interface CheckpointUtils {

    /**
     * Adds a task to a checkpointing group.
     *
     * @param t Task to be added to a group.
     * @param groupName Name of the checkpointing group.
     * @return returns the group to which the task was assigned
     */
    public CheckpointGroupImpl addTaskToGroup(Task t, String groupName);

    /**
     * Closes a group.
     *
     * @param groupName Name of the group to be closed.
     */
    public void closeGroup(String groupName);

}
