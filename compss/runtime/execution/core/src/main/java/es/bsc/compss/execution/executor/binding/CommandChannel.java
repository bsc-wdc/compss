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
package es.bsc.compss.execution.executor.binding;

import es.bsc.compss.execution.executor.binding.commands.ExternalCommand;

/**
 * Transport-agnostic channel to exchange commands with an out-of-process executor.
 *
 * @param <T> Type of commands transmitted through the channel
 */
public interface CommandChannel<T extends ExternalCommand> {

    /**
     * Sends a command through the channel.
     *
     * @param command Command to send
     * @return {@literal true} if the command was successfully submitted, {@literal false} otherwise
     */
    public boolean sendCommand(T command);

    /**
     * Waits for the next command received through the channel.
     *
     * @return Next command received through the channel
     * @throws CommandChannelException Error reading from the channel
     */
    public T readCommand() throws CommandChannelException;
}
