/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.agent.rest;

import es.bsc.compss.agent.Agent.AppMonitor;
import es.bsc.compss.types.annotations.parameter.DataType;


public class AppMainMonitor extends AppMonitor {

    //private final TaskProfile profile;
    private final String serviceInstanceId;
    private final String operation;

    public AppMainMonitor(
            String serviceInstanceId,
            String operation
    ) {
        super();
        this.serviceInstanceId = serviceInstanceId;
        this.operation = operation;
        //this.profile = new TaskProfile();
    }

    @Override
    public void onCreation() {
        //profile.created();
    }

    @Override
    public void onAccessesProcessed() {
        //profile.processedAccesses();
    }

    @Override
    public void onSchedule() {
        //profile.scheduled();
    }

    @Override
    public void onSubmission() {
        //profile.submitted();
    }

    @Override
    public void valueGenerated(int paramId, DataType type, Object value) {

    }

    @Override
    public void onErrorExecution() {
        //profile.finished();
    }

    @Override
    public void onFailedExecution() {
        //profile.finished();
    }

    @Override
    public void onSuccesfulExecution() {
        //profile.finished();
    }

    @Override
    public void onCompletion() {
        //profile.end();
    }

    @Override
    public void onFailure() {
    }
}
