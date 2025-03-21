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

import es.bsc.compss.executor.external.commands.CompssExceptionExternalCommand;


public class CompssExceptionPipeCommand extends CompssExceptionExternalCommand implements PipeCommand {

    public final Integer jobId;
    public final String message;


    public CompssExceptionPipeCommand(Integer jobId, String message) {
        this.jobId = jobId;
        this.message = message;
    }

    public CompssExceptionPipeCommand(String[] line) {
        this.jobId = Integer.parseInt(line[1]);
        this.message = line[2].replaceAll("_", " ");
    }

    public String getMessage() {
        return this.message;
    }

    @Override
    public int compareTo(PipeCommand t) {
        int value = Integer.compare(this.getType().ordinal(), t.getType().ordinal());
        if (value != 0) {
            value = Integer.compare(this.jobId, ((CompssExceptionPipeCommand) t).jobId);
        }
        return value;
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(super.getAsString());
        sb.append(TOKEN_SEP);
        sb.append(this.jobId);
        sb.append(TOKEN_SEP);
        sb.append(message.replace(" ", "_"));

        return sb.toString();
    }
}
