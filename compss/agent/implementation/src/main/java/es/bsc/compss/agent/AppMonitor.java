/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;


public abstract class AppMonitor implements TaskMonitor {

    private long appId;
    private final ApplicationParameter[] params;
    private final DataType[] paramTypes;
    private final String[] paramLocations;


    /**
     * Constructs a new Application monitor.
     *
     * @param params Monitored execution's parameters
     */
    public AppMonitor(ApplicationParameter[] params) {
        int numParams = params.length;

        this.params = params;
        this.paramTypes = new DataType[numParams];
        this.paramLocations = new String[numParams];
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

        this.params = new ApplicationParameter[numParams];
        int offset = 0;
        System.arraycopy(args, 0, this.params, 0, argsCount);
        offset = argsCount;
        if (target != null) {
            params[argsCount] = target;
            offset++;
        }
        System.arraycopy(results, 0, this.params, offset, resultsCount);

        this.paramTypes = new DataType[numParams];
        this.paramLocations = new String[numParams];
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
    public void valueGenerated(int paramId, String paramName, DataType paramType, String dataId, Object dataLocation) {
        ApplicationParameter originalParam = this.params[paramId];
        String originalDataMgmtId = originalParam.getDataMgmtId();

        this.paramTypes[paramId] = paramType;
        if (paramType == DataType.OBJECT_T) {
            LogicalData ld = Comm.getData(dataId);
            if (ld.getPscoId() != null) {
                this.paramTypes[paramId] = DataType.PSCO_T;
                SimpleURI targetURI = new SimpleURI(ProtocolType.PERSISTENT_URI.getSchema() + ld.getPscoId());
                this.paramLocations[paramId] = targetURI.toString();
            } else {
                this.paramTypes[paramId] = paramType;
                this.paramLocations[paramId] = originalDataMgmtId;
            }
        } else {
            this.paramTypes[paramId] = paramType;
            this.paramLocations[paramId] = dataLocation.toString();
        }

        if (dataId.compareTo(originalDataMgmtId) != 0) {
            try {
                Comm.linkData(originalDataMgmtId, dataId);
            } catch (CommException ce) {
                ErrorManager.error("Could not link " + originalDataMgmtId + " and " + dataId, ce);
            }
        }
    }

    public String[] getParamLocations() {
        return paramLocations;
    }

    public DataType[] getParamTypes() {
        return paramTypes;
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
    public void onException() {
    }

    @Override
    public void onCompletion() {
        Agent.finishedApplication(appId);
    }

    @Override
    public void onFailure() {
        Agent.finishedApplication(appId);
    }
}
