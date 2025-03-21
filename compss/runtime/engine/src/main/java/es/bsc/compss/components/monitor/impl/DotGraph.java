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
package es.bsc.compss.components.monitor.impl;

import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.CommutativeGroupTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.info.CollectionInfo;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.request.ap.BarrierGroupRequest;

import java.io.BufferedWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


public class DotGraph implements GraphHandler {

    private final GraphGenerator gm;

    // Graph drawing
    private int synchronizationId;
    private boolean taskDetectedAfterSync;


    /**
     * Constructs a new Graph Handler.
     */
    public DotGraph(String graphDir) {
        this.gm = new GraphGenerator(graphDir);

        // Add initial synchronization point
        this.synchronizationId = 0;
        this.gm.addSynchroToGraph(0);

        this.taskDetectedAfterSync = false;
    }

    @Override
    public void openTaskGroup(String groupName) {
        this.gm.addTaskGroupToGraph(groupName);
    }

    @Override
    public void closeTaskGroup() {
        this.gm.closeGroupInGraph();
    }

    @Override
    public void startTaskAnalysis(Task task) {
        // Set the syncId of the task
        task.setSynchronizationId(this.synchronizationId);
        // Update current sync status
        this.taskDetectedAfterSync = true;

        // Add task to graph
        this.gm.addTaskToGraph(task);
    }

    @Override
    public void createCommutativeGroup(CommutativeGroupTask group) {
        this.gm.addCommutativeGroup(group.getCommutativeIdentifier().toString());
    }

    @Override
    public void taskBelongsToCommutativeGroup(Task task, CommutativeGroupTask group) {
        this.gm.addTaskToCommutativeGroup(task, group.getCommutativeIdentifier().toString());
    }

    @Override
    public void closeCommutativeGroup(CommutativeGroupTask group) {
        this.gm.closeCommutativeGroup(group.getCommutativeIdentifier().toString());
    }

    @Override
    public void startGroupingEdges() {
        this.gm.startGroupingEdges();
    }

    @Override
    public void stopGroupingEdges() {
        this.gm.stopGroupingEdges();
    }

    @Override
    public void addStandandDependency(Task consumer, EngineDataAccessId daId, AbstractTask producer) {
        // Retrieve common information
        int dataId = daId.getDataId();
        int dataVersion;
        if (daId.isRead()) {
            dataVersion = ((ReadingDataAccessId) daId).getRVersionId();
        } else {
            dataVersion = ((WritingDataAccessId) daId).getWVersionId();
        }

        if (producer != null && producer != consumer) {
            if (producer instanceof Task) {
                addDataEdgeFromTaskToTask((Task) producer, consumer, dataId, dataVersion);
            } else {
                addEdgeFromCommutativeToTask(consumer, dataId, dataVersion, ((CommutativeGroupTask) producer), true);
            }
        } else {
            addDataEdgeFromMainToTask(consumer, dataId, dataVersion);
        }
    }

    /**
     * We will execute a task whose data is produced by another task. STEPS: Add an edge from the previous task or the
     * last synchronization point to the new task.
     *
     * @param source Source task.
     * @param dest Destination task.
     * @param dataId Data causing the dependency.
     * @param dataVersion Data version.
     */
    private void addDataEdgeFromTaskToTask(Task source, Task dest, int dataId, int dataVersion) {
        String src;
        if (source.getSynchronizationId() == dest.getSynchronizationId()) {
            src = String.valueOf(source.getId());
        } else {
            src = "Synchro" + dest.getSynchronizationId();
        }
        String dst = String.valueOf(dest.getId());
        String dep = dataId + "v" + dataVersion;
        this.gm.addEdgeToGraph(src, dst, EdgeType.DATA_DEPENDENCY, dep);
    }

    /**
     * We will execute a task with no predecessors, data must be retrieved from the last synchronization point. STEPS:
     * Add edge from sync to task
     *
     * @param dest Destination task.
     * @param dataId Data causing the dependency.
     * @param dataVersion Data version.
     */
    private void addDataEdgeFromMainToTask(Task dest, int dataId, int dataVersion) {
        String src = "Synchro" + dest.getSynchronizationId();
        String dst = String.valueOf(dest.getId());
        String dep = dataId + "v" + dataVersion;
        this.gm.addEdgeToGraph(src, dst, EdgeType.DATA_DEPENDENCY, dep);
    }

    /**
     * Addition of an edge from the commutative group to a task.
     *
     * @param dest Destination task.
     * @param dataId Id of the data causing the dependency.
     * @param dataVersion Version of the data causing the dependency.
     * @param cgt Commutative task group.
     * @param comToTask Whether the edge should be printed as a group to task or viceversa.
     */
    private void addEdgeFromCommutativeToTask(Task dest, int dataId, int dataVersion, CommutativeGroupTask cgt,
        boolean comToTask) {
        String src = String.valueOf(cgt.getCommutativeTasks().get(0).getId());
        String dst = String.valueOf(dest.getId());
        String dep = dataId + "v" + dataVersion;
        String comId = cgt.getCommutativeIdentifier().toString();
        if (comToTask) {
            this.gm.addEdgeToGraphFromGroup(src, dst, dep, comId, "clusterCommutative", EdgeType.DATA_DEPENDENCY);
        } else {
            this.gm.addEdgeToGraphFromGroup(dst, src, dep, comId, "clusterCommutative", EdgeType.DATA_DEPENDENCY);
        }
    }

    @Override
    public void addStreamDependency(AbstractTask task, Integer streamDataId, boolean isWrite) {
        String stream = "Stream" + streamDataId;

        // Add stream node even if it exists
        this.gm.addStreamToGraph(stream);

        // Add dependency
        String taskId = String.valueOf(task.getId());
        if (isWrite) {
            this.gm.addEdgeToGraph(taskId, stream, EdgeType.STREAM_DEPENDENCY, "");
        } else {
            this.gm.addEdgeToGraph(stream, taskId, EdgeType.STREAM_DEPENDENCY, "");
        }
    }

    @Override
    public void endTaskAnalysis(Task task, boolean taskHasEdge) {
        if (!taskHasEdge) {
            // If the graph must be written and the task has no edge due to its parameters,
            // add a direct dependency from last sync to task.
            String src = "Synchro" + task.getSynchronizationId();
            String dst = String.valueOf(task.getId());
            String dep = "";
            this.gm.addEdgeToGraph(src, dst, EdgeType.DATA_DEPENDENCY, dep);
        }
    }

    @Override
    public void endApp() {
        this.gm.closeCommutativeGroups();
        this.gm.commitGraph(true);
    }

    @Override
    public void mainAccessToData(AbstractTask task, EdgeType edgeType, EngineDataInstanceId accessedData) {
        String newSynch = addSynchro(false);
        int dataId = accessedData.getDataId();
        int dataVersion = accessedData.getVersionId();
        String label = dataId + "v" + dataVersion;
        // Add edge from task to sync
        if (task instanceof CommutativeGroupTask && !((CommutativeGroupTask) task).getCommutativeTasks().isEmpty()) {
            // Add edge from commutative group to synch
            CommutativeGroupTask commGroupTask = (CommutativeGroupTask) task;
            String src = String.valueOf(commGroupTask.getCommutativeTasks().get(0).getId());
            String groupId = commGroupTask.getCommutativeIdentifier().toString();

            this.gm.addEdgeToGraphFromGroup(src, newSynch, label, groupId, "clusterCommutative", edgeType);
        } else {
            // Add edge from task to sync
            String src = String.valueOf(task.getId());
            this.gm.addEdgeToGraph(src, newSynch, edgeType, label);
        }
    }

    @Override
    public void groupBarrier(BarrierGroupRequest barrier) {
        // Addition of missing commutative groups to graph
        this.gm.closeCommutativeGroups();
        /**
         * We have explicitly called the barrier group API call. STEPS: Add a new synchronization node. Add an edge from
         * last synchronization point to barrier. Add edges from group tasks to barrier.
         */
        String newSyncStr = addSynchro(true);

        int groupsLastTaskID = barrier.getGraphSource();
        if (groupsLastTaskID > 0) {
            String src = String.valueOf(groupsLastTaskID);
            String groupName = barrier.getGroupName();
            this.gm.addEdgeToGraphFromGroup(src, newSyncStr, "", groupName, "clusterTasks", EdgeType.USER_DEPENDENCY);
        }

        // We can draw the graph on a barrier while we wait for tasks
        this.gm.commitGraph(false);
    }

    @Override
    public void barrier(Map<String, FileInfo> files, Map<Integer, DataInfo> objects,
        Map<String, CollectionInfo> collections) {
        // Addition of missing commutative groups to graph
        this.gm.closeCommutativeGroups();
        /**
         * We have explicitly called the barrier API. STEPS: Add a new synchronization node. Add an edge from last
         * synchronization point to barrier. Add edges from writer tasks to barrier.
         */

        int oldSync = this.synchronizationId;
        String newSyncStr = addSynchro(true);

        // Add edges from writers to barrier
        Set<AbstractTask> uniqueWriters = new HashSet<>();
        for (DataInfo di : files.values()) {
            if (di != null) {
                List<AbstractTask> dataWriters = di.getDataWriters();
                uniqueWriters.addAll(dataWriters);
            }
        }
        for (DataInfo di : objects.values()) {
            List<AbstractTask> dataWriters = di.getDataWriters();
            uniqueWriters.addAll(dataWriters);
        }
        for (DataInfo di : collections.values()) {
            List<AbstractTask> dataWriters = di.getDataWriters();
            uniqueWriters.addAll(dataWriters);
        }
        for (AbstractTask writer : uniqueWriters) {
            if (writer != null && writer.getSynchronizationId() == oldSync) {
                String taskId = String.valueOf(writer.getId());
                this.gm.addEdgeToGraph(taskId, newSyncStr, EdgeType.USER_DEPENDENCY, "");
            }
        }

        // We can draw the graph on a barrier while we wait for tasks
        this.gm.commitGraph(false);
    }

    private String addSynchro(boolean barrier) {
        // Add barrier node
        int oldSync = this.synchronizationId;
        String oldSyncStr = "Synchro" + oldSync;
        String newSyncStr;
        if (this.taskDetectedAfterSync || barrier) {
            // Add barrier node and edge from last sync
            this.synchronizationId++;
            newSyncStr = "Synchro" + this.synchronizationId;
            EdgeType eType;
            if (barrier) {
                this.gm.addBarrierToGraph(this.synchronizationId);
                eType = EdgeType.USER_DEPENDENCY;
            } else {
                this.gm.addSynchroToGraph(this.synchronizationId);
                eType = EdgeType.DATA_DEPENDENCY;
            }
            this.gm.addEdgeToGraph(oldSyncStr, newSyncStr, eType, "");

            // Reset task detection
            this.taskDetectedAfterSync = false;
        } else {
            newSyncStr = oldSyncStr;
        }
        return newSyncStr;
    }

    @Override
    public BufferedWriter getAndOpenCurrentGraph() {
        return this.gm.getAndOpenCurrentGraph();
    }

    @Override
    public void closeCurrentGraph() {
        this.gm.closeCurrentGraph();
    }

    @Override
    public void removeCurrentGraph() {
        this.gm.removeTemporaryGraph();
    }

}
