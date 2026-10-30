/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.execution.executor.binding.piped.commands;

import es.bsc.compss.execution.executor.binding.commands.GetAvailableFilesExternalCommand;

public class GetAvailableFilesPipeCommand extends GetAvailableFilesExternalCommand implements PipeCommand {

    /**
     * Constructs a GetAvailableFilesCommand out of the message received through the pipe.
     *
     * @param args message received through the pipe
     */
    public GetAvailableFilesPipeCommand(String[] args) {
        super();
        this.count = Integer.parseInt(args[1]);
        this.fileNames = new String[this.count];
        for (int i = 0; i < this.count; i++) {
            this.fileNames[i] = args[2 + i];
        }
    }

    @Override
    public int compareTo(PipeCommand t) {
        return Integer.compare(this.getType().ordinal(), t.getType().ordinal());
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        // Do nothing
    }

}
