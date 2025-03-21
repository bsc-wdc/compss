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

import es.bsc.compss.executor.external.commands.AliveReplyExternalCommand;
import java.util.LinkedList;
import java.util.List;


/**
 * Alive notification command send through a pipe.
 */
public class AliveReplyPipeCommand extends AliveReplyExternalCommand implements PipeCommand {

    private final List<Integer> pids = new LinkedList<>();


    /**
     * Alive notification constructor.
     * 
     * @param command Command content
     */
    public AliveReplyPipeCommand(String[] command) {
        for (int i = 1; i < command.length; i++) {
            pids.add(Integer.parseInt(command[i]));
        }
    }

    /**
     * Alive notification default constructor.
     */
    public AliveReplyPipeCommand() {

    }

    @Override
    public int compareTo(PipeCommand t) {
        return Integer.compare(this.getType().ordinal(), t.getType().ordinal());
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        pids.addAll(((AliveReplyPipeCommand) receivedCommand).pids);
    }

    public List<Integer> getAliveProcesses() {
        return pids;
    }

}
