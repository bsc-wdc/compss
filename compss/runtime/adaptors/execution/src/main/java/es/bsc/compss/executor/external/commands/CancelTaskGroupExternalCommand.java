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
package es.bsc.compss.executor.external.commands;

public class CancelTaskGroupExternalCommand implements ExternalCommand {

    protected String groupName;


    public String getGroupName() {
        return groupName;
    }

    @Override
    public CommandType getType() {
        return CommandType.CANCEL_TASK_GROUP;
    }

    @Override
    public String getAsString() {
        return CommandType.CANCEL_TASK_GROUP.name();
    }

}
