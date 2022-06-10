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
package es.bsc.compss.types;

import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.worker.COMPSsException;


public class DoNothingTaskMonitor implements TaskMonitor {

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
    public void valueGenerated(int paramId, TaskResult param) {
    }

    @Override
    public void onExecutionStart() {
    }

    @Override
    public void onExecutionStartAt(long ts) {
    }

    @Override
    public void onExecutionEnd() {
    }

    @Override
    public void onExecutionEndAt(long ts) {
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
    public void onCompletion() {
    }

    @Override
    public void onFailure() {
    }

    @Override
    public void onException(COMPSsException e) {
    }
}
