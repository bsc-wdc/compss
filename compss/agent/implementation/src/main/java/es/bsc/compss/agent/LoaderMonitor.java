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

import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.types.annotations.parameter.DataType;


public class LoaderMonitor implements TaskMonitor {

    private long appId;
    private AppMonitor nestedMonitor;


    /**
     * Constructs a new Loader monitor.
     *
     * @param appId AppId corresponding to the loader task
     * @param nestedMonitor monitor for the execution of the inner tasks.
     */
    public LoaderMonitor(long appId, AppMonitor nestedMonitor) {
        this.nestedMonitor = nestedMonitor;
        this.appId = appId;
    }

    public void setAppId(long appId) {
        this.appId = appId;
    }

    public long getAppId() {
        return this.appId;
    }

    @Override
    public void onCreation() {
        this.nestedMonitor.onCreation();
    }

    @Override
    public void onAccessesProcessed() {
        this.nestedMonitor.onAccessesProcessed();
    }

    @Override
    public void onSchedule() {
        this.nestedMonitor.onSchedule();
    }

    @Override
    public void onSubmission() {
        this.nestedMonitor.onSubmission();
    }

    @Override
    public void onDataReception() {
        this.nestedMonitor.onDataReception();
    }

    @Override
    public void valueGenerated(int paramId, String paramName, DataType paramType, String dataId, Object dataLocation) {
        this.nestedMonitor.valueGenerated(paramId, paramName, paramType, dataId, dataLocation);
    }

    @Override
    public void onAbortedExecution() {
        this.nestedMonitor.onAbortedExecution();
    }

    @Override
    public void onErrorExecution() {
        this.nestedMonitor.onErrorExecution();
    }

    @Override
    public void onFailedExecution() {
        this.nestedMonitor.onFailedExecution();
    }

    @Override
    public void onSuccesfulExecution() {
        this.nestedMonitor.onSuccesfulExecution();
    }

    @Override
    public void onCancellation() {
        this.nestedMonitor.onCancellation();
    }

    @Override
    public void onException() {
        this.nestedMonitor.onException();
    }

    @Override
    public void onCompletion() {
        this.nestedMonitor.onCompletion();
        Agent.finishedApplication(appId);
    }

    @Override
    public void onFailure() {
        this.nestedMonitor.onFailure();
        Agent.finishedApplication(appId);
    }
}
