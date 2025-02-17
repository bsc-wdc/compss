/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.executor.external.piped.commands;

import es.bsc.compss.executor.external.commands.ExecuteTaskExternalCommand;


/**
 * Description of a task execution command to be sent through a pipe.
 */
public class ExecuteTaskPipeCommand extends ExecuteTaskExternalCommand implements PipeCommand {

    private final Integer jobId;
    private final String sandBox;


    /**
     * Execute task command constructor.
     * 
     * @param jobId Job Identifier
     * @param sandBox Location where to run the job
     */
    public ExecuteTaskPipeCommand(Integer jobId, String sandBox) {
        super();
        this.jobId = jobId;
        this.sandBox = sandBox;
    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(CommandType.EXECUTE_TASK.name());
        sb.append(TOKEN_SEP);
        sb.append(String.valueOf(this.jobId));
        sb.append(TOKEN_SEP);
        sb.append(sandBox);
        for (String c : this.arguments) {
            sb.append(TOKEN_SEP);
            sb.append(c);
        }
        return sb.toString();
    }

    @Override
    public int compareTo(PipeCommand t) {
        int value = Integer.compare(this.getType().ordinal(), t.getType().ordinal());
        if (value != 0) {
            value = Integer.compare(this.jobId, ((ExecuteTaskPipeCommand) t).jobId);
        }
        return value;
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
