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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.info.DataVersion;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CheckpointDataVersion {

    // DataVersion needed to be saved
    private final DataVersion dataVersion;

    // Url of the data version to be saved
    private String location;

    // COMPSs data type
    private DataType type;

    // Shows if this data has been requested to checkpoint
    private Boolean checkpointRequested = false;

    // Shows if the data has been checkpointed
    private boolean checkpointed = false;

    // Creation-time-known data, Task generating the data.
    private final CheckpointTask producer;

    // List of tasks that will read this dataVersion
    private final List<CheckpointTask> readers;


    /**
     * Finish copying data parameter.
     *
     * @param dataVersion Dataversion.
     * @param producer producer of the data.
     */
    public CheckpointDataVersion(DataVersion dataVersion, CheckpointTask producer) {
        this.dataVersion = dataVersion;
        this.producer = producer;
        this.readers = new LinkedList<>();
    }

    /**
     * Recovering of the data, needed to show that it has been checkpointed.
     *
     * @param location Data location.
     * @param type type of the data.
     * @param checkpointed True if data has been checkpointed.
     */
    public CheckpointDataVersion(String location, DataType type, Boolean checkpointed) {
        this.location = location;
        this.type = type;
        this.producer = null;
        this.readers = new LinkedList<>();
        this.dataVersion = null;
        this.checkpointRequested = checkpointed;
        this.checkpointed = checkpointed;
    }

    /**
     * Value has been checkpointed, we let it know to the producer and we try to delete intermediate versions.
     * 
     * @return {@literal true} if the value has been checkpointed for the first time.
     */
    public boolean valueCheckpointed() {
        if (!checkpointed) {
            checkpointed = true;
            return true;
        }
        return false;
    }

    /**
     * Remove reader from the readers list.
     *
     * @param reader Reader task of the data.
     * @return {@literal true} if the value has been checkpointed for the first time.
     */
    public boolean readerCheckpointed(CheckpointTask reader) {
        readers.remove(reader);
        if (!checkpointed) { // Maybe change
            if (readers.isEmpty()) {
                return valueCheckpointed();
            }
        }
        return false;
    }

    /**
     * Assigning location and type to data, once finished copying.
     *
     * @param location location.
     * @param type Datatype.
     */
    public void generatedData(String location, DataType type) {
        this.location = location;
        this.type = type;
    }

    /**
     * Adding a reader task to the data.
     */
    public void addReader(CheckpointTask ctl) {
        readers.add(ctl);
    }

    // AUXILIARY FUNCTIONS

    /**
     * Returns the version of the data.
     */
    public DataVersion getVersion() {
        return dataVersion;
    }

    /**
     * Returns the producer of the task.
     */
    public CheckpointTask getProducer() {
        return producer;
    }

    /**
     * Returns the location of the data.
     */
    public String getLocation() {
        return location;
    }

    /**
     * Returns the type of the data.
     */
    public DataType getType() {
        return type;
    }

    /**
     * Returns the id of the data.
     */
    public int getDataId() {
        return dataVersion.getDataInstanceId().getDataId();
    }

    /**
     * Returns true if the data has been requested for checkpointing.
     */
    public Boolean getCheckpointRequested() {
        return this.checkpointRequested;
    }

    /**
     * Marks the data as requested for checkpointed.
     */
    public void setCheckpointRequested() {
        this.checkpointRequested = true;
    }

    /**
     * Returns true if the data has been checkpointed.
     */
    public Boolean isCheckpointed() {
        return this.checkpointed;
    }

    /**
     * Returns true if there are no more readers for this data.
     */
    public Boolean areReadersEmpty() {
        return this.readers.isEmpty();
    }

}
