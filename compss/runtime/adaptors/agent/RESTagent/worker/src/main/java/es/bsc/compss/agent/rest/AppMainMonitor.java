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
package es.bsc.compss.agent.rest;

import es.bsc.compss.agent.AppMonitor;
import es.bsc.compss.agent.rest.types.TaskProfile;
import es.bsc.compss.agent.types.ApplicationParameter;


public class AppMainMonitor extends AppMonitor {

    private final TaskProfile profile;


    public AppMainMonitor(ApplicationParameter[] params) {
        super(params);
        this.profile = new TaskProfile();
    }

    public AppMainMonitor(ApplicationParameter[] args, ApplicationParameter target, ApplicationParameter[] results) {
        super(args, target, results);
        this.profile = new TaskProfile();
    }

    @Override
    public void onCreation() {
        super.onCreation();
        profile.created();
    }

    @Override
    public void onAccessesProcessed() {
        super.onAccessesProcessed();
        profile.processedAccesses();
    }

    @Override
    public void onSchedule() {
        super.onSchedule();
        profile.scheduled();
    }

    @Override
    public void onSubmission() {
        super.onSubmission();
        profile.submitted();
    }

    @Override
    public void onAbortedExecution() {
        super.onAbortedExecution();
        profile.finished();
    }

    @Override
    public void onErrorExecution() {
        super.onAbortedExecution();
        profile.finished();
    }

    @Override
    public void onFailedExecution() {
        super.onFailedExecution();
        profile.finished();
    }

    @Override
    public void onException() {
        super.onException();
        profile.finished();
    }

    @Override
    public void onSuccesfulExecution() {
        super.onSuccesfulExecution();
        profile.finished();
    }

    @Override
    public void onCancellation() {
        super.onCancellation();
        profile.end();
        System.out.println("Main Job cancelled after " + profile.getTotalTime());
    }

    @Override
    public void onCompletion() {
        super.onCompletion();
        profile.end();
        System.out.println("Main Job completed after " + profile.getTotalTime());
    }

    @Override
    public void onFailure() {
        super.onFailure();
        profile.end();
        System.out.println("Main Job failed after " + profile.getTotalTime());
    }
}
