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

import es.bsc.compss.types.Task;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.request.ap.APRequest;


/**
 * Class to manage application checkpointing.
 */

public interface CheckpointManager {

    /**
     * When there is a new task it checks if it is checkpointed, and therefore we recover its data or executes it.
     *
     * @param t Task to be evaluated.
     */
    public void newTask(Task t);

    /**
     * When a task finishes decides if its data needs to be saved.
     *
     * @param t Task to be evaluated.
     */
    public void endTask(Task t);

    /**
     * Marks data as accessed by the main, and therefore will not be deleted.
     *
     * @param di DataInstance to be marked.
     */
    public void mainAccess(EngineDataInstanceId di);

    /**
     * Notifies the Checkpoint Manager that a data has been deleted.
     *
     * @param dataInfo Id of the deleted data
     */
    public void deletedData(DataInfo dataInfo);

    /**
     * Performs an snapshot of the data, closing all the groups.
     */
    public void snapshot();

    /**
     * Shuts down the checkpointer checkpointing all the available data and notifies their completion to the user.
     */
    public void shutdown();


    public static interface User {

        /**
         * Callback to accessProcessor to notify that all the available data has been checkpointed.
         */
        public void allAvailableDataCheckpointed();

        /**
         * Checkpoint request to accessProcessor.
         *
         * @param apRequest APRequest from checkpointer.
         * @param errorMessage Error message in case the request can not be enqueued.
         */
        public void addCheckpointRequest(APRequest apRequest, String errorMessage);

    }
}
