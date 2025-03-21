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
package es.bsc.compss.types;

public interface ApplicationTaskMonitor {

    /**
     * Notifies the monitor that a new task has been created.
     *
     * @param t Created task
     */
    void onTaskCreation(Task t);

    /**
     * A task analysis has been started.
     *
     * @param t task whose analysis started
     */
    void onTaskAnalysisStart(Task t);

    /**
     * A task analysis has finished.
     *
     * @param t task whose analysis finished
     * @param hasEdge @return {@literal true}, if the task has requested an edge in the graph; {@literal false},
     *            otherwise.
     */
    void onTaskAnalysisEnd(Task t, boolean hasEdge);

    /**
     * The application creates a new group of Commutative Task.
     *
     * @param g newly created group of commutative tasks
     */
    void onCommutativeGroupCreation(CommutativeGroupTask g);

    /**
     * The application detected that a task belongs to a commutative tasks group.
     *
     * @param t task belonging to a group
     * @param g grop to whom the task belongs
     */
    void onTaskBelongsToCommutativeGroup(Task t, CommutativeGroupTask g);

    /**
     * The application detects that a commutative task group has been closed.
     * 
     * @param g closed group
     */
    void onCommutativeGroupClosure(CommutativeGroupTask g);
}
