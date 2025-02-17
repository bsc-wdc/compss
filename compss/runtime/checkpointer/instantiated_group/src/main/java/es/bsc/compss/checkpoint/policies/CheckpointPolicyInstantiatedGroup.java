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
package es.bsc.compss.checkpoint.policies;

import es.bsc.compss.checkpoint.CheckpointManagerImpl;
import es.bsc.compss.checkpoint.types.CheckpointGroupImpl;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.types.Task;

import java.util.HashMap;


public class CheckpointPolicyInstantiatedGroup extends CheckpointManagerImpl {

    public CheckpointPolicyInstantiatedGroup(HashMap<String, String> config, AccessProcessor ap) {
        super(config, 0, 0, ap);
    }

    @Override
    protected void assignTaskToGroup(Task t) {
        // Assign the task to the decided group
        CheckpointGroupImpl group = this.addTaskToGroup(t, String.valueOf(countingGroup));
        // If the group reaches its size of closure it closes (in this case is 1)
        if (group.getSize() == groupSize) {
            this.closeGroup(String.valueOf(countingGroup));
            countingGroup += 1;
        }

    }

}
