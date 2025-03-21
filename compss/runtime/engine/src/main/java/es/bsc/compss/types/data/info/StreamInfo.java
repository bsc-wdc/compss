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
package es.bsc.compss.types.data.info;

import es.bsc.compss.components.monitor.impl.EdgeType;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import es.bsc.compss.types.data.params.DataOwner;
import es.bsc.compss.types.data.params.StreamData;
import es.bsc.compss.types.parameter.impl.DependencyParameter;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.util.ErrorManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;


public class StreamInfo extends DataInfo<StreamData> {

    private final List<AbstractTask> streamWriters = new ArrayList<>();


    /**
     * Creates a new StreamInfo instance for the given stream.
     *
     * @param stream description of the stream related to the info
     * @param owner owner of the StreamInfo being created
     */
    public StreamInfo(StreamData stream, DataOwner owner) {
        super(stream, owner);
        int code = stream.getCode();
        owner.registerObjectData(code, this);
    }

    /**
     * Returns the object hashcode.
     *
     * @return The object hashcode.
     */
    public int getCode() {
        return this.getParams().getCode();
    }

    @Override
    public EngineDataAccessId willAccess(AccessMode mode) {
        EngineDataAccessId daId;
        switch (mode) {
            case R:
                this.currentVersionWillBeRead();

                daId = new RAccessId(this, this.currentVersion);
                break;

            case W:
                this.currentVersionWillBeWritten();
                daId = new WAccessId(this, this.currentVersion);
                break;
            default: // cases C, CV, RW
                ErrorManager.warn("Unsupported type of access (" + mode + ") for stream " + this.dataId);
                daId = null;
        }
        return daId;
    }

    @Override
    public void waitForDataReadyToDelete(Semaphore sem) {
        // Nothing to wait for
    }

    @Override
    public AbstractTask getLastVersionProducer() {
        if (!streamWriters.isEmpty()) {
            return streamWriters.get(0);
        }
        return null;
    }

    @Override
    public void completedProducer(AbstractTask task) {
        this.streamWriters.remove(task);
    }

    @Override
    public boolean readValue(Task task, DependencyParameter dp, boolean isConcurrent) {
        int dataId = dp.getDataAccessId().getDataId();
        if (!streamWriters.isEmpty()) {
            if (DEBUG) {
                StringBuilder sb = new StringBuilder();
                if (streamWriters.size() > 1) {
                    sb.append("Last writers for stream datum ").append(dataId).append(" are tasks ");
                } else {
                    sb.append("Last writer for stream datum ").append(dataId).append(" is task ");
                }
                for (AbstractTask lastWriter : streamWriters) {
                    sb.append(lastWriter.getId());
                    sb.append(" ");
                }
                LOGGER.debug(sb.toString());
            }

            // Add dependencies
            for (AbstractTask lastWriter : streamWriters) {
                // Debug message
                if (DEBUG) {
                    LOGGER.debug(
                        "Adding stream dependency between task " + lastWriter.getId() + " and task " + task.getId());
                }
                // Add dependency
                task.addStreamDataDependency(lastWriter);
            }
        } else {
            // Task is free
            if (DEBUG) {
                LOGGER.debug("There is no last stream writer for datum " + dataId);
            }
        }

        // Add edge to graph
        Application app = task.getApplication();
        app.getGH().addStreamDependency(task, dataId, false);
        return true;
    }

    @Override
    public void writeValue(Task t, DependencyParameter dp, boolean isConcurrent) {
        this.streamWriters.add(t);
        Integer dataId = dp.getDataAccessId().getDataId();
        Application app = t.getApplication();
        app.getGH().addStreamDependency(t, dataId, true);
    }

    @Override
    public void mainAccess(RegisterDataAccessRequest rdar, EngineDataAccessId access) {
        EngineDataInstanceId accessedData;
        if (access.isWrite()) {
            accessedData = ((EngineDataAccessId.WritingDataAccessId) access).getWrittenDataInstance();
        } else {
            accessedData = ((EngineDataAccessId.ReadingDataAccessId) access).getReadDataInstance();
        }
        // Add graph description
        Application app = rdar.getAccess().getApp();
        for (AbstractTask lastWriter : this.streamWriters) {
            app.getGH().mainAccessToData(lastWriter, EdgeType.STREAM_DEPENDENCY, accessedData);
        }
    }

    @Override
    public List<AbstractTask> getDataWriters() {
        return streamWriters;
    }
}
