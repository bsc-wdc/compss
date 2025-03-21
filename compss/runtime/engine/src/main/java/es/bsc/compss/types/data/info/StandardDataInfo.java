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
import es.bsc.compss.types.CommutativeGroupTask;
import es.bsc.compss.types.CommutativeIdentifier;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.params.DataOwner;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.parameter.impl.DependencyParameter;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.util.ErrorManager;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public abstract class StandardDataInfo<T extends DataParams> extends DataInfo<T> {

    private AbstractTask lastWriter;

    private final List<Task> concurrentReaders = new ArrayList<>();


    public StandardDataInfo(T data, DataOwner owner) {
        super(data, owner);
    }

    @Override
    public final EngineDataAccessId willAccess(AccessParams.AccessMode mode) {
        EngineDataAccessId daId = null;
        switch (mode) {
            case C:
            case R:
                this.currentVersionWillBeRead();
                daId = new RAccessId(this, this.currentVersion);
                break;

            case W:
                newVersion();
                this.currentVersionWillBeWritten();
                daId = new WAccessId(this, this.currentVersion);
                break;

            case CV:
            case RW:
                this.currentVersionWillBeRead();
                DataVersion readInstance = this.currentVersion;
                newVersion();
                this.currentVersionWillBeWritten();
                DataVersion writtenInstance = this.currentVersion;
                if (readInstance != null) {
                    daId = new RWAccessId(this, readInstance, writtenInstance);
                } else {
                    ErrorManager.warn("Previous instance for data" + this.dataId + " is null.");
                }
                break;
        }
        return daId;
    }

    @Override
    public void completedProducer(AbstractTask task) {
        int producerTaskId = task.getId();
        if (lastWriter != null && lastWriter.getId() == producerTaskId) {
            updateLastWriter(null);
        }
    }

    @Override
    public AbstractTask getLastVersionProducer() {
        return this.lastWriter;
    }

    @Override
    public boolean readValue(Task task, DependencyParameter dp, boolean isConcurrent) {
        if (this.concurrentReaders.isEmpty() || isConcurrent) {
            return readDependency(task, dp);
        } else {
            return dependsFromConcurrent(task, dp);
        }
    }

    private boolean readDependency(Task task, DependencyParameter dp) {
        int dataId = dp.getDataAccessId().getDataId();
        boolean hasEdge = false;
        if (lastWriter != null && lastWriter != task) {
            if (DEBUG) {
                LOGGER.debug("Last writer for datum " + dataId + " is task " + lastWriter.getId());
                LOGGER.debug("Adding dependency between task " + lastWriter.getId() + " and task " + task.getId());
            }

            Application app = task.getApplication();
            if (lastWriter instanceof CommutativeGroupTask) {
                CommutativeGroupTask group = (CommutativeGroupTask) lastWriter;
                if (dp.getDirection() == Direction.COMMUTATIVE && !group.isClosed()) {
                    Integer coreId = task.getTaskDescription().getCoreElement().getCoreId();
                    CommutativeIdentifier comId = new CommutativeIdentifier(coreId, dataId);
                    if (CommutativeIdentifier.equals(group.getCommutativeIdentifier(), comId)) {
                        AbstractTask predecessor = group.getGroupPredecessor();
                        if (predecessor != null) {
                            task.addDataDependency(predecessor, dp);
                            app.getGH().addStandandDependency(task, group.getGroupPredecessorAccess(), predecessor);
                            hasEdge = true;
                        }
                        return hasEdge;
                    }
                }
                group.close();
            }

            // Add dependency
            task.addDataDependency(lastWriter, dp);
            app.getGH().addStandandDependency(task, dp.getDataAccessId(), lastWriter);
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

    private boolean dependsFromConcurrent(Task task, DependencyParameter dp) {
        int dataId = dp.getDataAccessId().getDataId();
        if (!this.concurrentReaders.contains(task)) {
            if (DEBUG) {
                LOGGER.debug("There was a concurrent access for datum " + dataId);
                LOGGER.debug("Adding dependency between concurrent list and task " + task.getId());
            }
            for (AbstractTask t : this.concurrentReaders) {
                // Add dependency
                task.addDataDependency(t, dp);
                Application app = task.getApplication();
                app.getGH().addStandandDependency(task, dp.getDataAccessId(), t);
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
    public void writeValue(Task t, DependencyParameter dp, boolean isConcurrent) {
        if (isConcurrent) {
            this.concurrentReaders.add(t);
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

                EngineDataAccessId daId = dp.getDataAccessId();
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
                updateLastWriter(group);
            } else {
                updateLastWriter(t);
            }
            this.concurrentReaders.clear();
        }
    }

    private void updateLastWriter(AbstractTask newWriter) {
        if (lastWriter != newWriter) {
            if (lastWriter instanceof CommutativeGroupTask) {
                CommutativeGroupTask prevGroup = (CommutativeGroupTask) lastWriter;
                prevGroup.close();
            }
        }
        this.lastWriter = newWriter;
    }

    @Override
    public void mainAccess(RegisterDataAccessRequest rdar, EngineDataAccessId access) {
        EngineDataInstanceId accessedData;
        if (access.isWrite()) {
            accessedData = ((EngineDataAccessId.WritingDataAccessId) access).getWrittenDataInstance();
        } else {
            accessedData = ((EngineDataAccessId.ReadingDataAccessId) access).getReadDataInstance();
        }
        Application app = rdar.getAccess().getApp();
        if (lastWriter != null) {
            app.getGH().mainAccessToData(lastWriter, EdgeType.DATA_DEPENDENCY, accessedData);
            lastWriter.addListener(rdar);
            rdar.addPendingOperation();
        }
        for (AbstractTask task : this.concurrentReaders) {
            app.getGH().mainAccessToData(task, EdgeType.DATA_DEPENDENCY, accessedData);
            task.addListener(rdar);
            rdar.addPendingOperation();
        }
        this.concurrentReaders.clear();

        if (rdar.getAccess().getParameters().getMode().isWrite()) {
            updateLastWriter(null);
        }
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
