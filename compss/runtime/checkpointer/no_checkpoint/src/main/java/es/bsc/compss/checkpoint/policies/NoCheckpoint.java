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

import es.bsc.compss.checkpoint.CheckpointManager;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.info.DataInfo;

import java.util.HashMap;


public class NoCheckpoint implements CheckpointManager {

    private final CheckpointManager.User cpUser;


    public NoCheckpoint(HashMap<String, String> config, AccessProcessor ap) {
        cpUser = ap;
    }

    @Override
    public void newTask(Task t) {
    }

    @Override
    public void endTask(Task t) {
    }

    @Override
    public void mainAccess(EngineDataInstanceId daId) {
    }

    @Override
    public void deletedData(DataInfo dataInfo) {
    }

    @Override
    public void snapshot() {
    }

    @Override
    public void shutdown() {
        cpUser.allAvailableDataCheckpointed();
    }
}
