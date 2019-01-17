/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.nio.dataRequest;

import es.bsc.compss.data.DataManager;
import es.bsc.compss.types.annotations.parameter.DataType;

import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.commands.NIOData;


public class WorkerDataRequest extends DataRequest {

    private final TransferringTask task;

    public WorkerDataRequest(TransferringTask task, DataType type, NIOData source, String target) {
        super(type, source, target);
        this.task = task;
    }

    public TransferringTask getTransferringTask() {
        return this.task;
    }


    public static class TransferringTask implements DataManager.LoadDataListener {

        private final NIOTask task;
        private int params;
        private boolean error;

        public TransferringTask(NIOTask task) {
            this.task = task;
            params = task.getParams().size();
            if (task.getTarget() != null) {
                params++;
            }
        }

        public NIOTask getTask() {
            return this.task;
        }

        public int getParams() {
            return this.params;
        }

        public boolean getError() {
            return this.error;
        }

        @Override
        public void loadedValue() {
            --this.params;
        }

        public void setError(boolean error) {
            this.error = error;
        }
    }

}
