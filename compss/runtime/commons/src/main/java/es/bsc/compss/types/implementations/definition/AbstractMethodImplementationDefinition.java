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
package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;

import java.util.List;


public interface AbstractMethodImplementationDefinition extends ImplementationDefinition {

    public TaskType getTaskType();

    public MethodType getMethodType();

    /**
     * Method to append AbstractMethodDefinition properties to an arguments list.
     * 
     * @param args arguments list
     * @param auxParam auxiliar parameter to pass in order to customize an argument according to the remote node (path,
     *            default value,..)
     */
    public void appendToArgs(List<String> args, String auxParam);

}
