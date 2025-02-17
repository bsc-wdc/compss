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

import es.bsc.compss.executor.external.commands.CancelJobExternalCommand;
import es.bsc.compss.executor.external.piped.PipePair;


public class CancelJobCommand extends CancelJobExternalCommand implements PipeCommand {

    public PipePair pipe;


    public CancelJobCommand(PipePair pipe) {
        this.pipe = pipe;
    }

    public CancelJobCommand() {

    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(super.getAsString());
        sb.append(" ").append(pipe.getOutboundPipe()).append(" ").append(pipe.getInboundPipe());
        return sb.toString();
    }

    @Override
    public int compareTo(PipeCommand t) {
        int value = Integer.compare(this.getType().ordinal(), t.getType().ordinal());
        if (value != 0) {
            value = pipe.getPipesLocation().compareTo(((CancelJobCommand) t).pipe.getPipesLocation());
        }
        return value;
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
