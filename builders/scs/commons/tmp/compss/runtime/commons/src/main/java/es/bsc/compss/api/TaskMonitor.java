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
package es.bsc.compss.api;

import es.bsc.compss.types.annotations.parameter.DataType;


public interface TaskMonitor {

    public void onCreation();

    public void onAccessesProcessed();

    public void onSchedule();

    public void onSubmission();

    public void valueGenerated(int paramId, DataType type, Object value);

    public void onErrorExecution();

    public void onFailedExecution();

    public void onSuccesfulExecution();

    public void onCompletion();

    public void onFailure();
}
