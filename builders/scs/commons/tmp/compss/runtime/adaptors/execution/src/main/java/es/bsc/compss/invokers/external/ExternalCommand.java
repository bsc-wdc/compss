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
package es.bsc.compss.invokers.external;

public interface ExternalCommand {

    public static enum CommandType {
        EXECUTE_TASK, // Execute a task
        END_TASK, // Task finished
        ERROR_TASK, // Error in task execution
        QUIT, // Finish execution
        REMOVE, // Remove data
        SERIALIZE; // Serialize data
    }


    public static final String TOKEN_SEP = " ";


    public CommandType getType();

    public String getAsString();

}
