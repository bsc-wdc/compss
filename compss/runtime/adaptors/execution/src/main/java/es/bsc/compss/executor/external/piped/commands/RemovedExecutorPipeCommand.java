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

import es.bsc.compss.executor.external.commands.RemovedExecutorExternalCommand;
import es.bsc.compss.executor.external.piped.PipePair;


public class RemovedExecutorPipeCommand extends RemovedExecutorExternalCommand implements PipeCommand {

    private final String inPipe;
    private final String outPipe;


    public RemovedExecutorPipeCommand(String[] command) {
        this.inPipe = command[1];
        this.outPipe = command[2];
    }

    public RemovedExecutorPipeCommand(PipePair pp) {
        this.inPipe = pp.getInboundPipe();
        this.outPipe = pp.getOutboundPipe();
    }

    @Override
    public String getAsString() {
        return super.getAsString() + " " + inPipe + " " + this.outPipe;
    }

    @Override
    public int compareTo(PipeCommand t) {
        int value = Integer.compare(this.getType().ordinal(), t.getType().ordinal());
        if (value == 0) {
            RemovedExecutorPipeCommand rt = (RemovedExecutorPipeCommand) t;
            value = this.inPipe.compareTo(rt.inPipe);
        }
        if (value == 0) {
            RemovedExecutorPipeCommand rt = (RemovedExecutorPipeCommand) t;
            value = this.outPipe.compareTo(rt.outPipe);
        }
        return value;
    }

    @Override
    public void join(PipeCommand receivedCommand) {
    }

}
