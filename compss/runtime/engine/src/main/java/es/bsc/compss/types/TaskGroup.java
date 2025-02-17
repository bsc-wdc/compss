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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.worker.COMPSsException;
import java.util.Collection;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TaskGroup implements AutoCloseable {

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);

    // Group name
    private final String name;

    // Application to whom the group belongs.
    private Application app;

    // Tasks belong to the group
    private final List<Task> tasks;

    // Has group been closed (true) or is it still open (false) and new tasks can be added
    private boolean closed;

    private boolean barrierSet;
    // Barrier pending to be resolved or the information that has to be forwarded upon its arrival.
    private Barrier barrier;


    /**
     * Creates a task group.
     *
     * @param groupName Name of the group.
     * @param app application to which the group belongs.
     */
    public TaskGroup(String groupName, Application app) {
        this.name = groupName;
        this.app = app;
        this.tasks = new LinkedList<>();
        this.closed = false;
        this.barrierSet = false;
        this.barrier = new PendingBarrier();
    }

    /**
     * Returns the name of group.
     *
     * @return name of the group
     */
    public String getName() {
        return this.name;
    }

    /**
     * Gets the application to whom the group belongs.
     *
     * @return the application to whom the group belongs.
     */
    public Application getApp() {
        return app;
    }

    /**
     * Adds task to group.
     *
     * @param task Task to add to the group
     */
    public synchronized void addTask(Task task) {
        tasks.add(task);
        this.barrier.setGraphSource(task.getId());
    }

    /**
     * Returns tasks of group.
     *
     * @return list of tasks belonging to the group
     */
    public List<Task> getTasks() {
        return tasks;
    }

    /**
     * Returns the ID of the last inserted task.
     *
     * @return Id of the last task registered added to the group.
     */
    public int getLastTaskId() {
        return this.barrier.getGraphSource();
    }

    /**
     * Returns a boolean stating if the group has pending tasks to execute.
     *
     * @return @literal{true}, if the group has pending tasks to execute; @literal{false} otherwise.
     */
    public boolean hasPendingTasks() {
        return !this.tasks.isEmpty();
    }

    /**
     * Removes a task from the group.
     *
     * @param t Task to remove.
     */
    public synchronized void removeTask(Task t) {
        this.tasks.remove(t);
    }

    /**
     * Adds into a list all the executions from tasks belonging to the group other than the passed in as a parameter.
     *
     * @param coll Collection where to add all the executions
     * @param task task whose executions are not to be included in the collection
     */
    public synchronized void addToCollectionExecutionForTasksOtherThan(Collection<AllocatableAction> coll, Task task) {
        for (Task t : this.tasks) {
            if (t.getId() != task.getId()) {
                for (AllocatableAction aa : t.getExecutions()) {
                    if (aa != null && aa.isPending()) {
                        LOGGER.debug(" Adding Task " + t.getId() + " to members group of task " + task.getId()
                            + "(Group " + this.getName() + ")");
                        coll.add(aa);
                    }
                }
            }
        }
    }

    /**
     * Registers a barrier request on the group. When all tasks are completed, the barrier will be released and any
     * possible COMPSsException raised due to the tasks of the group notified.
     *
     * @param request object to notify the end of the group
     */
    public void registerBarrier(Barrier request) {
        LOGGER.debug("Added barrier for group " + this.name);
        int currentGraphSource = this.barrier.getGraphSource();
        COMPSsException currentException = this.barrier.getException();
        request.setException(currentException);
        request.setGraphSource(currentGraphSource);
        if (hasPendingTasks()) {
            this.barrierSet = true;
            this.barrier = request;
        } else {
            this.barrierSet = false;
            this.barrier = new PendingBarrier();
            request.release();
        }
    }

    @Override
    public void close() throws Exception {

    }

    /**
     * Returns if there has been any task throwing a COMPSs Exception.
     *
     * @return
     */
    public boolean hasException() {
        return this.barrier.getException() != null;
    }

    /**
     * Returns the COMPSs Exception.
     *
     * @return
     */
    public COMPSsException getException() {
        return this.barrier.getException();
    }

    /**
     * A task of the group has raised a COMPSsException.
     *
     * @param e Exception raised due to the execution of the tasks of the group
     */
    public void setException(COMPSsException e) {
        LOGGER.debug("Exception set for group " + this.name);
        this.barrier.setException(e);
    }

    public boolean hasBarrier() {
        return this.barrierSet;
    }

    public void releaseBarrier() {
        this.barrierSet = false;
        this.barrier.release();
    }

    /**
     * Sets the closed flag to true.
     */
    public void setClosed() {
        this.closed = true;
    }

    /**
     * Returns if the task group is closed or not.
     *
     * @return @literal{true}, if the group is closed
     */
    public boolean isClosed() {
        return this.closed;
    }


    private static class PendingBarrier implements Barrier {

        private COMPSsException exception;
        private int graphSource = Integer.MIN_VALUE;


        @Override
        public void setException(COMPSsException exception) {
            this.exception = exception;
        }

        @Override
        public COMPSsException getException() {
            return this.exception;
        }

        @Override
        public void release() {
            // Do nothing
        }

        @Override
        public int getGraphSource() {
            return this.graphSource;
        }

        @Override
        public void setGraphSource(int id) {
            this.graphSource = id;
        }
    }
}
