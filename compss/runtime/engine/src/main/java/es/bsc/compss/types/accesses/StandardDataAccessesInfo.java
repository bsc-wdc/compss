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
import es.bsc.compss.types.Application;
import es.bsc.compss.types.CommutativeGroupTask;
import es.bsc.compss.types.CommutativeIdentifier;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/**
 * Class handling all the accesses related to a standard data value.
 */
public class StandardDataAccessesInfo extends DataAccessesInfo {

    private AbstractTask lastWriter;

    private final List<Task> concurrentReaders = new ArrayList<>();


    public StandardDataAccessesInfo(DataType dataType) {
        super(dataType);
    }

    @Override
    public void completedProducer(AbstractTask task, GraphHandler gh) {
        int producerTaskId = task.getId();
        if (lastWriter != null && lastWriter.getId() == producerTaskId) {
            updateLastWriter(null, gh);
        }
    }

    @Override
    public AbstractTask getConstrainingProducer() {
        return this.lastWriter;
    }

    @Override
    public boolean readValue(Task task, DependencyParameter dp, boolean isConcurrent, GraphHandler gh) {
        if (this.concurrentReaders.isEmpty() || isConcurrent) {
            return readDependency(task, dp, gh);
        } else {
            return concurrentDependency(task, dp, gh);
        }
    }

    private boolean readDependency(Task task, DependencyParameter dp, GraphHandler gh) {
        int dataId = dp.getDataAccessId().getDataId();
        boolean hasEdge = false;
        if (lastWriter != null && lastWriter != task) {
            if (DEBUG) {
                LOGGER.debug("Last writer for datum " + dataId + " is task " + lastWriter.getId());
                LOGGER.debug("Adding dependency between task " + lastWriter.getId() + " and task " + task.getId());
            }

            if (lastWriter instanceof CommutativeGroupTask) {
                CommutativeGroupTask group = (CommutativeGroupTask) lastWriter;
                if (dp.getDirection() == Direction.COMMUTATIVE && !group.isClosed()) {
                    Integer coreId = task.getTaskDescription().getCoreElement().getCoreId();
                    CommutativeIdentifier comId = new CommutativeIdentifier(coreId, dataId);
                    if (CommutativeIdentifier.equals(group.getCommutativeIdentifier(), comId)) {
                        AbstractTask predecessor = group.getGroupPredecessor();
                        if (predecessor != null) {
                            task.addDataDependency(predecessor, dp);
                            if (IS_DRAW_GRAPH) {
                                gh.drawStandardEdge(task, group.getGroupPredecessorAccess(), predecessor);
                            }
                            hasEdge = true;
                        }
                        return hasEdge;
                    }
                }
                closeCommutativeTasksGroup(group, gh);
            }

            // Add dependency
            task.addDataDependency(lastWriter, dp);
            if (IS_DRAW_GRAPH) {
                gh.drawStandardEdge(task, dp.getDataAccessId(), lastWriter);
            }
            hasEdge = true;
        } else {
            // Task is free
            if (DEBUG) {
                LOGGER.debug("There is no last writer for datum " + dataId);
            }
            task.registerFreeParam(dp);
        }
        return hasEdge;
    }

    private boolean concurrentDependency(Task task, DependencyParameter dp, GraphHandler gh) {
        int dataId = dp.getDataAccessId().getDataId();
        if (!this.concurrentReaders.contains(task)) {
            if (DEBUG) {
                LOGGER.debug("There was a concurrent access for datum " + dataId);
                LOGGER.debug("Adding dependency between concurrent list and task " + task.getId());
            }
            for (AbstractTask t : this.concurrentReaders) {
                // Add dependency
                task.addDataDependency(t, dp);
                if (IS_DRAW_GRAPH) {
                    gh.drawStandardEdge(task, dp.getDataAccessId(), t);
                }
            }
        } else {
            if (DEBUG) {
                LOGGER.debug("There is no last writer for datum " + dataId);
            }
            task.registerFreeParam(dp);
        }
        return true;
    }

    @Override
    public void writeValue(Task t, DependencyParameter dp, boolean isConcurrent, GraphHandler gh) {
        if (isConcurrent) {
            this.concurrentReaders.add((Task) t);
        } else {
            int dataId = dp.getDataAccessId().getDataId();
            LOGGER.info("Setting writer for data " + dataId);

            if (dp.getDirection() == Direction.COMMUTATIVE) {
                Integer coreId = t.getTaskDescription().getCoreElement().getCoreId();
                CommutativeIdentifier comId = new CommutativeIdentifier(coreId, dataId);

                CommutativeGroupTask group = null;
                if (lastWriter instanceof CommutativeGroupTask) {
                    CommutativeGroupTask prevGroup = (CommutativeGroupTask) lastWriter;
                    if (CommutativeIdentifier.equals(prevGroup.getCommutativeIdentifier(), comId)) {
                        if (!prevGroup.isClosed()) {
                            group = prevGroup;
                        }
                    }
                }

                DataAccessId daId = dp.getDataAccessId();
                if (group == null) {
                    Application app = t.getApplication();
                    group = new CommutativeGroupTask(app, comId);
                    group.setGroupPredecessor(lastWriter, (RWAccessId) daId);
                }
                group.addAccess((RWAccessId) daId);
                group.addCommutativeTask(t);
                group.addDataDependency(t, dp);
                t.setCommutativeGroup(group, daId);
                dp.setDataAccessId(group.getAccessPlaceHolder());
                if (IS_DRAW_GRAPH) {
                    gh.drawTaskInCommutativeGroup(t, group);
                }
                updateLastWriter(group, gh);
            } else {
                updateLastWriter(t, gh);
            }
            this.concurrentReaders.clear();
        }
    }

    private void updateLastWriter(AbstractTask newWriter, GraphHandler gh) {
        if (lastWriter != newWriter) {
            if (lastWriter instanceof CommutativeGroupTask) {
                CommutativeGroupTask prevGroup = (CommutativeGroupTask) lastWriter;
                closeCommutativeTasksGroup(prevGroup, gh);
            }
        }
        this.lastWriter = newWriter;
    }

    private void closeCommutativeTasksGroup(CommutativeGroupTask group, GraphHandler gh) {
        if (!group.isClosed()) {
            group.close();
            if (IS_DRAW_GRAPH) {
                gh.closeCommutativeTasksGroup(group);
            }
        }
    }

    @Override
    public void mainAccess(RegisterDataAccessRequest rdar, GraphHandler gh, int dataId, int dataVersion) {
        if (lastWriter != null) {
            if (IS_DRAW_GRAPH) {
                gh.addEdgeFromTaskToMain(lastWriter, EdgeType.DATA_DEPENDENCY, dataId, dataVersion);
            }
            // Release task if possible. Otherwise add to waiting
            if (lastWriter.isPending()) {
                lastWriter.addListener(rdar);
                rdar.addPendingOperation();
                if (rdar.getTaskAccessMode() == AccessParams.AccessMode.RW) {
                    updateLastWriter(null, gh);
                }
            }
        }

        for (AbstractTask task : this.concurrentReaders) {
            if (IS_DRAW_GRAPH) {
                gh.addEdgeFromTaskToMain(task, EdgeType.DATA_DEPENDENCY, dataId, dataVersion);
            }
            if (task != null && task.isPending()) {
                task.addListener(rdar);
                rdar.addPendingOperation();
            }
        }
        this.concurrentReaders.clear();
    }

    @Override
    public boolean isFinalProducer(Task t) {
        return (this.concurrentReaders.isEmpty() && this.lastWriter == t);
    }

    @Override
    public String toStringDetails() {
        StringBuilder sb = new StringBuilder();
        sb.append("concurrentReaders = [");
        for (AbstractTask t : this.concurrentReaders) {
            sb.append(t.getId()).append(" ");
        }
        sb.append("], ");
        sb.append("dataWriter = ").append(this.lastWriter != null ? this.lastWriter.getId() : "null");
        return sb.toString();
    }

    @Override
    public List<AbstractTask> getDataWriters() {
        List<AbstractTask> writers = new LinkedList<>();
        if (lastWriter != null) {
            writers.add(lastWriter);
        }
        return writers;
    }
}
