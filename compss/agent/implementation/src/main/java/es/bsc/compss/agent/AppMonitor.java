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
package es.bsc.compss.agent;

import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.agent.types.ApplicationParameterCollection;
import es.bsc.compss.api.ParameterCollectionMonitor;
import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.worker.COMPSsException;

import java.util.List;


public abstract class AppMonitor implements TaskMonitor {

    private long appId;
    private TaskResult[] taskResults;
    private COMPSsException exception;


    /**
     * Constructs a new Application monitor.
     *
     * @param args Monitored execution's arguments
     * @param target Monitored execution's target
     * @param results Monitored execution's results
     */
    public AppMonitor(ApplicationParameter[] args, ApplicationParameter target, ApplicationParameter[] results) {
        int argsCount = args.length;
        int resultsCount = results.length;
        int numParams = argsCount + resultsCount + (target != null ? 1 : 0);
        this.taskResults = new TaskResult[numParams];

        int offset = 0;
        for (; offset < argsCount; offset++) {
            ApplicationParameter param = args[offset];
            this.taskResults[offset] = buildResult(param);
        }

        if (target != null) {
            this.taskResults[offset] = buildResult(target);
            offset++;
        }

        for (int resultIdx = 0; resultIdx < results.length; resultIdx++) {
            ApplicationParameter param = results[resultIdx];
            this.taskResults[offset] = buildResult(param);
            offset++;
        }
    }

    private TaskResult buildResult(ApplicationParameter param) {
        switch (param.getType()) {
            case COLLECTION_T:
            case DICT_COLLECTION_T:
                ApplicationParameterCollection<ApplicationParameter> colParam;
                colParam = (ApplicationParameterCollection<ApplicationParameter>) param;
                List<ApplicationParameter> subParams = colParam.getCollectionParameters();
                int numElements = subParams.size();
                TaskResult[] subResults = new TaskResult[numElements];
                int subElementIdx = 0;
                for (ApplicationParameter subParam : subParams) {
                    subResults[subElementIdx] = buildResult(subParam);
                    subElementIdx++;
                }
                return new CollectionTaskResult(param.getDataMgmtId(), subResults);
            default:
                return new TaskResult(param.getDataMgmtId());
        }
    }

    @Override
    public ParameterMonitor getParameterMonitor(int paramId) {
        return this.taskResults[paramId].getMonitor();
    }

    public void setAppId(long appId) {
        this.appId = appId;
    }

    public long getAppId() {
        return this.appId;
    }

    @Override
    public void onCreation() {
    }

    @Override
    public void onAccessesProcessed() {
    }

    @Override
    public void onSchedule() {
    }

    @Override
    public void onSubmission() {
    }

    @Override
    public void onDataReception() {
    }

    @Override
    public void onAbortedExecution() {
    }

    @Override
    public void onErrorExecution() {
    }

    @Override
    public void onFailedExecution() {
    }

    @Override
    public void onSuccesfulExecution() {
    }

    @Override
    public void onCancellation() {
    }

    @Override
    public void onException(COMPSsException e) {
        this.exception = e;
    }

    public TaskResult[] getResults() {
        return this.taskResults;
    }

    public void setResults(TaskResult[] params) {
        this.taskResults = params;
    }

    @Override
    public void onCompletion() {
        Agent.finishedApplication(appId);
    }

    @Override
    public void onFailure() {
        Agent.finishedApplication(appId);
    }

    public COMPSsException getException() {
        return exception;
    }


    /**
     * Class representing a result of a task execution.
     */
    public static class TaskResult {

        private final String externalDataId;
        private DataType type;
        private String dataLocation;


        public TaskResult(String externalDataId) {
            this.externalDataId = externalDataId;
        }

        public DataType getType() {
            return type;
        }

        public void setType(DataType type) {
            this.type = type;
        }

        public String getDataLocation() {
            return dataLocation;
        }

        public void setDataLocation(String dataLocation) {
            this.dataLocation = dataLocation;
        }

        public ParameterMonitor getMonitor() {
            return new ParameterUpdater();
        }


        public class ParameterUpdater implements ParameterMonitor {

            @Override
            public void onCreation(DataType type, String dataName, String dataLocation) {
                TaskResult.this.type = type;
                TaskResult.this.dataLocation = dataLocation;

                LogicalData ld = Comm.getData(dataName);
                if (type == DataType.OBJECT_T) {
                    if (ld.getPscoId() != null) {
                        TaskResult.this.type = DataType.PSCO_T;
                        SimpleURI targetURI = new SimpleURI(ProtocolType.PERSISTENT_URI.getSchema() + ld.getPscoId());
                        TaskResult.this.dataLocation = targetURI.toString();
                    } else {
                        SimpleURI targetURI = new SimpleURI(ProtocolType.OBJECT_URI.getSchema() + externalDataId);
                        TaskResult.this.dataLocation = targetURI.toString();
                    }
                }
                if (dataName.compareTo(externalDataId) != 0) {
                    try {
                        Comm.linkData(externalDataId, dataName);
                    } catch (CommException ce) {
                        ErrorManager.error("Could not link " + externalDataId + " and " + dataName, ce);
                    }
                }
            }

        }

    }

    /**
     * Class representing a result of a task execution.
     */
    public static class CollectionTaskResult extends TaskResult {

        private final TaskResult[] subElements;


        public CollectionTaskResult(String externalDataId, TaskResult[] subResults) {
            super(externalDataId);
            this.subElements = subResults;
        }

        public TaskResult[] getSubelements() {
            return this.subElements;
        }

        @Override
        public ParameterMonitor getMonitor() {
            return new CollectionParameterUpdater();
        }


        public class CollectionParameterUpdater extends ParameterUpdater implements ParameterCollectionMonitor {

            @Override
            public ParameterMonitor getParameterMonitor(int i) {
                return CollectionTaskResult.this.subElements[i].getMonitor();
            }

        }
    }
}
