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
package es.bsc.compss.invokers.external.piped;

import es.bsc.compss.invokers.external.ExternalCommand;
import es.bsc.compss.invokers.types.ExternalTaskStatus;


/**
 *
 * @author flordan
 */
public interface PipeCommand extends ExternalCommand {

    public class ExecuteTaskPipeCommand extends ExecuteTaskExternalCommand implements PipeCommand {

        public final Integer jobId;

        public ExecuteTaskPipeCommand(Integer jobId) {
            this.jobId = jobId;
        }

        @Override
        public String getAsString() {

            StringBuilder sb = new StringBuilder(EXECUTE_TASK);
            sb.append(TOKEN_SEP);
            sb.append(String.valueOf(jobId));
            for (String c : arguments) {
                sb.append(TOKEN_SEP);
                sb.append(c);
            }
            return sb.toString();
        }
    }


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


    public class ErrorTaskPipeCommand extends ErrorTaskExternalCommand implements PipeCommand {

        public ErrorTaskPipeCommand(String[] result) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        public ExternalTaskStatus getTaskStatus() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }


    public class QuitPipeCommand extends QuitExternalCommand implements PipeCommand {

    }
}
