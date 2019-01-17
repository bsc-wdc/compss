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
package es.bsc.compss.invokers.external.commands;

import java.util.LinkedList;
import java.util.List;

import es.bsc.compss.invokers.external.ExternalCommand;


public class ExecuteTaskExternalCommand implements ExternalCommand {

    protected final LinkedList<String> arguments = new LinkedList<>();


    @Override
    public CommandType getType() {
        return CommandType.EXECUTE_TASK;
    }

    public void prependArgument(String argument) {
        this.arguments.addFirst(argument);
    }

    public void appendArgument(String argument) {
        this.arguments.add(argument);
    }

    public void appendAllArguments(List<String> arguments) {
        this.arguments.addAll(arguments);
    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(CommandType.EXECUTE_TASK.name());
        for (String c : arguments) {
            sb.append(TOKEN_SEP);
            sb.append(c);
        }
        return sb.toString();
    }
}
