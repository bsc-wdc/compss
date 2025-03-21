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

import es.bsc.compss.types.data.info.DataVersion;
import es.bsc.compss.types.parameter.impl.Parameter;


public class CheckpointData {

    // Last data version and last data producer
    private DataVersion lastCheckpointedVersion;
    private Parameter lastCompletedProducer;
    private Integer notDeletedFinishedCopies = 0;


    public void setLastCompletedProducer(Parameter lastCompletedProducer) {
        this.lastCompletedProducer = lastCompletedProducer;
    }

    public Parameter getLastCompletedProducer() {
        return lastCompletedProducer;
    }

    /**
     * Registers a version as the last version checkpointed for the data.
     * 
     * @param dv version to register as the last checkpointed
     */
    public void setLastCheckpointedVersion(DataVersion dv) {
        this.lastCheckpointedVersion = dv;
    }

    /**
     * Returns the last version of the data that has been checkpointed.
     * 
     * @return last checkpointed data version
     */
    public DataVersion getLastCheckpointedVersion() {
        return lastCheckpointedVersion;
    }

    public void addNotDeletedFinishedCopies() {
        this.notDeletedFinishedCopies += 1;
    }

    public int getNotDeletedFinishedCopies() {
        return this.notDeletedFinishedCopies;
    }

    public void removeNotDeletedFinishedCopies() {
        this.notDeletedFinishedCopies -= 1;
    }

}
