/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.COMPSsConstants;
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
import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Graph handler that submits graph events to the monitor graph endpoint.
 */
public class MonitorGraph implements GraphHandler {

    private static final String GRAPH_API = "compss.graph.api";
    private static final String DEFAULT_GRAPH_API = "http://localhost:8088/graph-events";
    private static final String DEFAULT_MASTER_NAME = "local-master";

    private final GraphSink graphSink;
    private final String runId;
    private final String masterName;


    /**
     * Creates a new MonitorGraph.
     */
    public MonitorGraph() {
        String graphEP = System.getProperty(GRAPH_API, DEFAULT_GRAPH_API);
        this.graphSink = new GraphSink(graphEP);
        this.graphSink.start();
        this.runId = extractRunId(System.getProperty("compss.uuid", System.getProperty(COMPSsConstants.LOG_DIR)));
        this.masterName = System.getProperty(COMPSsConstants.MASTER_NAME, DEFAULT_MASTER_NAME);
    }

    @Override
    public void appStarted(long appId) {
        emit("APP_START", appId);
    }

    @Override
    public void openTaskGroup(String groupName) {
        // Do nothing
    }

    @Override
    public void closeTaskGroup() {
        // Do nothing
    }

    @Override
    public void startTaskAnalysis(Task task) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("task_id", task.getId());
        payload.put("task_name", task.getTaskDescription().getName());
        emit("TASK_CREATED", task.getApplication().getId(), payload);
    }

    @Override
    public void createCommutativeGroup(CommutativeGroupTask group) {
        // Do nothing
    }

    @Override
    public void taskBelongsToCommutativeGroup(Task task, CommutativeGroupTask group) {
        // Do nothing
    }

    @Override
    public void closeCommutativeGroup(CommutativeGroupTask group) {
        // Do nothing
    }

    @Override
    public void startGroupingEdges() {
        // Do nothing
    }

    @Override
    public void stopGroupingEdges() {
        // Do nothing
    }

    @Override
    public void addStandandDependency(Task consumer, EngineDataAccessId daId, AbstractTask producer) {
        if (producer == null || producer == consumer) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("producer_id", producer instanceof Task ? producer.getId() : null);
        payload.put("producer_name",
            producer instanceof Task ? ((Task) producer).getTaskDescription().getName() : "commutative_group");
        payload.put("consumer_id", consumer.getId());
        payload.put("consumer_name", consumer.getTaskDescription().getName());
        payload.put("data_id", daId.getDataId());
        payload.put("data_version", getDataVersion(daId));
        emit("DATA_DEP", consumer.getApplication().getId(), payload);
    }

    @Override
    public void addStreamDependency(AbstractTask task, Integer streamDataId, boolean isWrite) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("task_id", task.getId());
        payload.put("task_name", task instanceof Task ? ((Task) task).getTaskDescription().getName() : null);
        payload.put("stream_id", streamDataId);
        payload.put("is_write", isWrite);
        emit("STREAM_DEP", task.getApplication().getId(), payload);
    }

    @Override
    public void taskFinished(Task task) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("task_id", task.getId());
        payload.put("task_name", task.getTaskDescription().getName());
        emit("TASK_FINISHED", task.getApplication().getId(), payload);
    }

    @Override
    public void endTaskAnalysis(Task task, boolean taskHasEdge) {
        // Do nothing
    }

    @Override
    public void mainAccessToData(AbstractTask task, EdgeType edgeType, EngineDataInstanceId accessedData) {
        // Do nothing
    }

    @Override
    public void groupBarrier(BarrierGroupRequest barrier) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("group_name", barrier.getGroupName());
        payload.put("graph_source_task_id", barrier.getGraphSource());
        emit("GROUP_BARRIER", barrier.getApp().getId(), payload);
    }

    @Override
    public void barrier(long appId, Map<String, FileInfo> files, Map<Integer, DataInfo> objects,
        Map<String, CollectionInfo> collections) {
        emit("BARRIER", appId);
    }

    @Override
    public void endApp(long appId) {
        emit("APP_END", appId);
    }

    @Override
    public BufferedWriter getAndOpenCurrentGraph() {
        return null;
    }

    @Override
    public void closeCurrentGraph() {
        // Do nothing
    }

    @Override
    public void removeCurrentGraph() {
        this.graphSink.stop();
    }

    private void emit(String type, long appId) {
        emit(type, appId, null);
    }

    private void emit(String type, long appId, Map<String, Object> payload) {
        MonitorGraphEvent event = new MonitorGraphEvent(this.runId, appId, type, this.masterName, payload);
        this.graphSink.enqueue(event.toJson());
    }

    private static int getDataVersion(EngineDataAccessId daId) {
        if (daId.isRead()) {
            return ((ReadingDataAccessId) daId).getRVersionId();
        }
        return ((WritingDataAccessId) daId).getWVersionId();
    }

    private static String extractRunId(String logDir) {
        if (logDir == null || logDir.trim().isEmpty()) {
            return "unknown_run";
        }
        try {
            File dir = new File(logDir);
            return dir.getName();
        } catch (Exception e) {
            return "unknown_run";
        }
    }
}
