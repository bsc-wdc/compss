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
package es.bsc.compss.invokers.external.piped.commands;

import es.bsc.compss.invokers.external.commands.EndTaskExternalCommand;
import es.bsc.compss.invokers.external.piped.PipeCommand;
import es.bsc.compss.invokers.types.ExternalTaskStatus;


public class EndTaskPipeCommand extends EndTaskExternalCommand implements PipeCommand {

    public final Integer jobId;
    public final ExternalTaskStatus taskStatus;


    public EndTaskPipeCommand(String[] line) {
        jobId = Integer.parseInt(line[1]);
        if (line.length > 3) {
            taskStatus = new ExternalTaskStatus(line);
        } else {
            int exitValue = Integer.parseInt(line[2]);
            taskStatus = new ExternalTaskStatus(exitValue);
        }
    }

    public ExternalTaskStatus getTaskStatus() {
        return taskStatus;
    }
}
