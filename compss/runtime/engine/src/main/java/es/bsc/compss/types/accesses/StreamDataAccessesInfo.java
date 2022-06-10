/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.accesses;

import es.bsc.compss.components.monitor.impl.EdgeType;
import es.bsc.compss.components.monitor.impl.GraphHandler;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import java.util.ArrayList;
import java.util.List;


/**
 * Class handling all the accesses related to a stream.
 */
public class StreamDataAccessesInfo extends DataAccessesInfo {

    private final List<AbstractTask> streamWriters = new ArrayList<>();


    public StreamDataAccessesInfo(DataType dataType) {
        super(dataType);
    }

    @Override
    public AbstractTask getConstrainingProducer() {
        if (!streamWriters.isEmpty()) {
            return streamWriters.get(0);
        }
        return null;
    }

    @Override
    public void completedProducer(AbstractTask task, GraphHandler gh) {
        this.streamWriters.remove(task);
    }

    @Override
    public boolean readValue(Task task, DependencyParameter dp, boolean isConcurrent, GraphHandler gh) {
        int dataId = dp.getDataAccessId().getDataId();
        if (!streamWriters.isEmpty()) {
            if (DEBUG) {
                StringBuilder sb = new StringBuilder();
                if (streamWriters.size() > 1) {
                    sb.append("Last writers for stream datum ");
                    sb.append(dataId);
                    sb.append(" are tasks ");
                } else {
                    sb.append("Last writer for stream datum ");
                    sb.append(dataId);
                    sb.append(" is task ");
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
        if (IS_DRAW_GRAPH) {
            gh.drawStreamEdge(task, dataId, false);
        }
        return true;
    }

    @Override
    public void writeValue(Task t, DependencyParameter dp, boolean isConcurrent, GraphHandler gh) {
        this.streamWriters.add(t);
        if (IS_DRAW_GRAPH) {
            Integer dataId = dp.getDataAccessId().getDataId();
            gh.drawStreamEdge(t, dataId, true);
        }
    }

    @Override
    public void mainAccess(RegisterDataAccessRequest rdar, GraphHandler gh, int dataId, int dataVersion) {
        // Add graph description
        if (IS_DRAW_GRAPH) {
            for (AbstractTask lastWriter : this.streamWriters) {
                gh.addEdgeFromTaskToMain(lastWriter, EdgeType.STREAM_DEPENDENCY, dataId, dataVersion);
            }
        }
    }

    @Override
    public boolean isFinalProducer(Task t) {
        return (this.streamWriters.isEmpty());
    }

    @Override
    public String toStringDetails() {
        StringBuilder sb = new StringBuilder();
        sb.append("streamWriters = [");
        for (AbstractTask t : this.streamWriters) {
            sb.append(t.getId()).append(" ");
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public List<AbstractTask> getDataWriters() {
        return streamWriters;
    }
}
