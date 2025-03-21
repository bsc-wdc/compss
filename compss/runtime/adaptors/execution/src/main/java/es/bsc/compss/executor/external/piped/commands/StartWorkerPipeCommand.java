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

import es.bsc.compss.executor.external.commands.StartWorkerExternalCommand;
import es.bsc.compss.executor.external.piped.ControlPipePair;


public class StartWorkerPipeCommand extends StartWorkerExternalCommand implements PipeCommand {

    private final ControlPipePair pipe;
    private final String launchWorkerCommand;
    private final String logDir;


    /**
     * Creates a new StartWorkerPipeCommand instance.
     * 
     * @param launchWorkerCommand Command to launch the worker.
     * @param pipe Pair of pipes (control and result).
     * @param logDir Path where the logs are stored.
     */
    public StartWorkerPipeCommand(String launchWorkerCommand, ControlPipePair pipe, String logDir) {
        this.pipe = pipe;
        this.launchWorkerCommand = launchWorkerCommand;
        this.logDir = logDir;
    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(super.getAsString());
        sb.append(TOKEN_SEP);
        sb.append(pipe.getOutboundPipe()).append(TOKEN_SEP);
        sb.append(pipe.getInboundPipe()).append(TOKEN_SEP);
        sb.append(this.logDir).append(TOKEN_SEP);
        sb.append(launchWorkerCommand);
        return sb.toString();
    }

    @Override
    public int compareTo(PipeCommand t) {
        int value = Integer.compare(this.getType().ordinal(), t.getType().ordinal());
        if (value == 0) {
            value = pipe.getInboundPipe().compareTo(((StartWorkerPipeCommand) t).pipe.getInboundPipe());
        }
        return value;
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        // Do nothing
    }
}
