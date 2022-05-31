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
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.api.TaskMonitor.CollectionTaskResult;
import es.bsc.compss.api.TaskMonitor.TaskResult;
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
    private final ApplicationParameter[] taskParams;
    private TaskResult[] taskResults;
    private COMPSsException exception;


    /**
     * Constructs a new Application monitor.
     *
     * @param originalParams Monitored execution's parameters
     */
    public AppMonitor(ApplicationParameter[] originalParams) {
        this.taskParams = originalParams;
    }

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
        taskResults = new TaskResult[numParams];
        this.taskParams = new ApplicationParameter[numParams];
        int offset = 0;
        System.arraycopy(args, 0, this.taskParams, 0, argsCount);
        offset = argsCount;
        if (target != null) {
            taskParams[argsCount] = target;
            offset++;
        }
        System.arraycopy(results, 0, this.taskParams, offset, resultsCount);
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

    private void updateResult(ApplicationParameter param, TaskResult result) {
        String originalDataMgmtId = param.getDataMgmtId();
        String dataId = result.getDataName();

        LogicalData ld = Comm.getData(dataId);
        if (result.getType() == DataType.OBJECT_T) {
            if (ld.getPscoId() != null) {
                result.setType(DataType.PSCO_T);
                SimpleURI targetURI = new SimpleURI(ProtocolType.PERSISTENT_URI.getSchema() + ld.getPscoId());
                result.setDataLocation(targetURI.toString());
            } else {
                result.setDataLocation(originalDataMgmtId);
            }
        }
        if (dataId.compareTo(originalDataMgmtId) != 0) {
            try {
                Comm.linkData(originalDataMgmtId, ld.getName());
            } catch (CommException ce) {
                ErrorManager.error("Could not link " + originalDataMgmtId + " and " + dataId, ce);
            }
        }

        if (result.getType() == DataType.COLLECTION_T) {
            List<ApplicationParameter> subparams;
            subparams = ((ApplicationParameterCollection<ApplicationParameter>) param).getCollectionParameters();
            TaskResult[] subResults = ((CollectionTaskResult) result).getSubelements();

            for (int i = 0; i < subparams.size(); i++) {
                updateResult(subparams.get(i), subResults[i]);
            }
        }
    }

    @Override
    public void valueGenerated(int paramId, TaskResult result) {
        ApplicationParameter originalParam = this.taskParams[paramId];
        updateResult(originalParam, result);
        taskResults[paramId] = result;
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

}
