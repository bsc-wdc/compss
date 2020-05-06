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
package es.bsc.compss.agent.comm;

import es.bsc.compss.agent.AppMonitor;
import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.types.annotations.parameter.DataType;


public class PrintMonitor extends AppMonitor {

    public PrintMonitor(ApplicationParameter[] args, ApplicationParameter target, ApplicationParameter[] results) {
        super(args, target, results);
    }

    @Override
    public void onCreation() {
        System.out.println("Task created");
        super.onCreation();
    }

    @Override
    public void onAccessesProcessed() {
        System.out.println("Accesses processed");
        super.onAccessesProcessed();
    }

    @Override
    public void onSchedule() {
        System.out.println("Scheduling");
        super.onSchedule();
    }

    @Override
    public void onSubmission() {
        System.out.println("Submitted");
        super.onSubmission();
    }

    @Override
    public void onDataReception() {
        System.out.println("All data received on the designated worker");
        super.onSubmission();
    }

    @Override
    public void valueGenerated(int paramId, String paramName, DataType paramType, String dataId, Object dataLocation) {
        System.out.println("Generated " + paramType + "-value with dataId " + dataId + " at location " + dataLocation
            + " for parameter on position " + paramId + " and  name " + paramName);
        super.valueGenerated(paramId, paramName, paramType, dataId, dataLocation);
    }

    @Override
    public void onAbortedExecution() {
        System.out.println("Execution aborted");
        super.onAbortedExecution();
    }

    @Override
    public void onErrorExecution() {
        System.out.println("Error on execution");
        super.onErrorExecution();
    }

    @Override
    public void onFailedExecution() {
        System.out.println("Failed Execution");
        super.onFailedExecution();
    }

    @Override
    public void onSuccesfulExecution() {
        System.out.println("Successful execution");
        super.onSuccesfulExecution();
    }

    @Override
    public void onCancellation() {
        System.out.println("Cancelled");
        super.onCancellation();
    }

    @Override
    public void onException() {
        System.out.println("COMPSsException raised");
        super.onException();
    }

    @Override
    public void onCompletion() {
        System.out.println("Completed");
        super.onCompletion();
    }

    @Override
    public void onFailure() {
        System.out.println("Failed!");
        super.onFailure();
    }

}
