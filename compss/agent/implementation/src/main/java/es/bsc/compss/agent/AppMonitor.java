/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class AppMonitor implements TaskMonitor {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.API);

    private long appId;
    private final ApplicationParameter[] originalParams;
    private final DataType[] paramTypes;
    private final String[] paramLocations;
    private Object[][] params;


    /**
     * Constructs a new Application monitor.
     *
     * @param originalParams Monitored execution's parameters
     */
    public AppMonitor(ApplicationParameter[] originalParams) {
        int numParams = params.length;

        this.originalParams = originalParams;
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
        params = new Object[numParams][];
        this.originalParams = new ApplicationParameter[numParams];
        int offset = 0;
        System.arraycopy(args, 0, this.originalParams, 0, argsCount);
        offset = argsCount;
        if (target != null) {
            originalParams[argsCount] = target;
            offset++;
        }
        System.arraycopy(results, 0, this.originalParams, offset, resultsCount);

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

    void valueGeneratedElement(Object[] param, ApplicationParameter originalParam) {
        String dataId = param[DATA_ID_POS].toString();
        String originalDataMgmtId = originalParam.getDataMgmtId();
        LogicalData ld = Comm.getData(dataId);
        if (param[TYPE_POS] == DataType.OBJECT_T) {
            if (ld.getPscoId() != null) {
                param[TaskMonitor.TYPE_POS] = DataType.PSCO_T;
                SimpleURI targetURI = new SimpleURI(ProtocolType.PERSISTENT_URI.getSchema() + ld.getPscoId());
                param[TaskMonitor.LOCATION_POS] = targetURI.toString();
            } else {
                param[TaskMonitor.LOCATION_POS] = originalDataMgmtId;
            }
        }
        if (dataId.compareTo(originalDataMgmtId) != 0) {
            try {
                Comm.linkData(originalDataMgmtId, ld.getName());
            } catch (CommException ce) {
                ErrorManager.error("Could not link " + originalDataMgmtId + " and " + dataId, ce);
            }
        }
    }

    void valueGenerated(Object[] param, ApplicationParameter originalParam) {
        valueGeneratedElement(param, originalParam);
        if (param[TYPE_POS] == DataType.COLLECTION_T) {
            Object[][] subParams = (Object[][]) param[3];
            @SuppressWarnings("unchecked") // Because originalParams correspond to the same parameter of param
            List<ApplicationParameter> originalSubparams =
                ((ApplicationParameterCollection<ApplicationParameter>) originalParam).getCollectionParameters();
            for (int i = 0; i < subParams.length; i++) {
                valueGenerated(subParams[i], originalSubparams.get(i));
            }
        }
    }

    @Override
    public void valueGenerated(int paramId, Object[] param) {
        ApplicationParameter originalParam = this.originalParams[paramId];

        valueGenerated(param, originalParam);

        this.paramTypes[paramId] = (DataType) param[TaskMonitor.TYPE_POS];
        this.paramLocations[paramId] = param[TaskMonitor.LOCATION_POS].toString();
        params[paramId] = param;
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

    public Object[][] getParams() {
        return this.params;
    }

    public void setParams(Object[][] params) {
        this.params = params;
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
