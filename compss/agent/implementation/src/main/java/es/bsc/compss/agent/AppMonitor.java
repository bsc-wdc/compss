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
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.transferable.SafeCopyTransferable;
import es.bsc.compss.types.uri.MultiURI;
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
                return new CollectionTaskResult(param.getDataMgmtId(), subResults, this);
            default:
                return new TaskResult(param.getDataMgmtId(), this);
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
    public final void onCreation() {
        specificOnCreation();
    }

    protected abstract void specificOnCreation();

    @Override
    public final void onAccessesProcessed() {
        specificOnAccessesProcessed();
    }

    protected abstract void specificOnAccessesProcessed();

    @Override
    public final void onSchedule() {
        specificOnSchedule();
    }

    protected abstract void specificOnSchedule();

    @Override
    public final void onSubmission() {
        specificOnSubmission();
    }

    protected abstract void specificOnSubmission();

    @Override
    public final void onDataReception() {
        specificOnDataReception();
    }

    protected abstract void specificOnDataReception();

    @Override
    public final void onExecutionStart() {
        specificOnExecutionStart();
    }

    protected abstract void specificOnExecutionStart();

    @Override
    public final void onExecutionStartAt(long t) {
        specificOnExecutionStartAt(t);
    }

    protected abstract void specificOnExecutionStartAt(long t);

    @Override
    public final void onExecutionEnd() {
        specificOnExecutionEnd();
    }

    protected abstract void specificOnExecutionEnd();

    @Override
    public final void onExecutionEndAt(long t) {
        specificOnExecutionEndAt(t);
    }

    protected abstract void specificOnExecutionEndAt(long t);

    @Override
    public final void onAbortedExecution() {
        specificOnAbortedExecution();
    }

    protected abstract void specificOnAbortedExecution();

    @Override
    public final void onErrorExecution() {
        specificOnErrorExecution();
    }

    protected abstract void specificOnErrorExecution();

    @Override
    public final void onFailedExecution() {
        specificOnFailedExecution();
    }

    protected abstract void specificOnFailedExecution();

    @Override
    public final void onSuccesfulExecution() {
        specificOnSuccessfulExecution();
    }

    protected abstract void specificOnSuccessfulExecution();

    @Override
    public final void onCancellation() {
        specificOnCancellation();
    }

    protected abstract void specificOnCancellation();

    @Override
    public final void onException(COMPSsException e) {
        this.exception = e;
        specificOnException(e);
    }

    protected abstract void specificOnException(COMPSsException e);

    public TaskResult[] getResults() {
        return this.taskResults;
    }

    public void setResults(TaskResult[] params) {
        this.taskResults = params;
    }


    boolean failed = false;
    boolean enabled = false;
    int pendingOperations = 0;


    private void addPendingOperation() {
        this.pendingOperations++;
    }

    private void performedPendingOperation() {
        pendingOperations--;
        notifyEnd();
    }

    private void notifyEnd() {
        if (enabled && pendingOperations == 0) {
            if (failed) {
                new Thread() {

                    @Override
                    public void run() {
                        Agent.finishedApplication(appId);
                        specificOnFailure();
                    }
                }.start();
            } else {
                new Thread() {

                    @Override
                    public void run() {
                        Agent.finishedApplication(appId);
                        specificOnCompletion();
                    }
                }.start();
            }
        }
    }

    @Override
    public final void onCompletion() {
        failed = false;
        enabled = true;
        notifyEnd();
    }

    protected abstract void specificOnCompletion();

    @Override
    public void onFailure() {
        failed = true;
        enabled = true;
        notifyEnd();
    }

    protected abstract void specificOnFailure();

    public COMPSsException getException() {
        return exception;
    }


    /**
     * Class representing a result of a task execution.
     */
    public static class TaskResult {

        private final String externalDataId;
        private String dataLocation;
        private final AppMonitor monitor;


        public TaskResult(String externalDataId, AppMonitor monitor) {
            this.externalDataId = externalDataId;
            this.monitor = monitor;
        }

        public boolean isCollective() {
            return false;
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
                if (dataName.compareTo(externalDataId) != 0) {
                    try {
                        Comm.linkData(externalDataId, dataName);
                    } catch (CommException ce) {
                        ErrorManager.error("Could not link " + externalDataId + " and " + dataName, ce);
                    }
                }

                TaskResult.this.dataLocation = dataLocation;

                LogicalData ld = Comm.getData(dataName);
                if (type == DataType.OBJECT_T) {
                    if (ld.getPscoId() != null) {
                        SimpleURI targetURI = new SimpleURI(ProtocolType.PERSISTENT_URI.getSchema() + ld.getPscoId());
                        TaskResult.this.dataLocation = targetURI.toString();
                    } else {
                        SimpleURI targetURI = new SimpleURI(ProtocolType.OBJECT_URI.getSchema() + externalDataId);
                        TaskResult.this.dataLocation = targetURI.toString();
                    }
                }

                if (!ld.getAllHosts().contains(Comm.getAppHost())) {
                    monitor.addPendingOperation();
                    Comm.getAppHost().getData(ld, new SafeCopyTransferable(), new EventListener() {

                        @Override
                        public void notifyEnd(DataOperation d) {
                            String location = null;
                            for (MultiURI mu : ld.getURIsInHost(Comm.getAppHost())) {
                                location = mu.getPath();
                            }
                            TaskResult.this.dataLocation = location;
                            monitor.performedPendingOperation();
                        }

                        @Override
                        public void notifyFailure(DataOperation d, Exception excptn) {
                        }
                    });
                }
            }

        }

    }

    /**
     * Class representing a result of a task execution.
     */
    public static class CollectionTaskResult extends TaskResult {

        private final TaskResult[] subElements;


        public CollectionTaskResult(String externalDataId, TaskResult[] subResults, AppMonitor monitor) {
            super(externalDataId, monitor);
            this.subElements = subResults;
        }

        @Override
        public boolean isCollective() {
            return true;
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
