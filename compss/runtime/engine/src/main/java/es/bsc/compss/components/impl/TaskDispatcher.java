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
package es.bsc.compss.components.impl;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.components.ResourceUser;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.request.listener.RequestListener;
import es.bsc.compss.types.request.td.ActionUpdate;
import es.bsc.compss.types.request.td.CERegistration;
import es.bsc.compss.types.request.td.CancelTaskRequest;
import es.bsc.compss.types.request.td.ExecuteTasksRequest;
import es.bsc.compss.types.request.td.MonitoringDataRequest;
import es.bsc.compss.types.request.td.PrintCurrentGraphRequest;
import es.bsc.compss.types.request.td.PrintCurrentLoadRequest;
import es.bsc.compss.types.request.td.ShutdownRequest;
import es.bsc.compss.types.request.td.TDRequest;
import es.bsc.compss.types.request.td.TaskSummaryRequest;
import es.bsc.compss.types.request.td.UpdateLocalCEIRequest;
import es.bsc.compss.types.request.td.WorkerRestartRequest;
import es.bsc.compss.types.request.td.WorkerUpdateRequest;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.updates.PerformedIncrease;
import es.bsc.compss.types.resources.updates.ResourceUpdate;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.CEIParser;
import es.bsc.compss.util.Classpath;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.worker.COMPSsException;

import java.io.BufferedWriter;
import java.io.File;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Component used as interface between the task analysis and the task scheduler Manage and handles requests for task
 * execution, task status, etc.
 */
public class TaskDispatcher extends RequestDispatcher<TDRequest> implements ResourceUser, ActionOrchestrator {

    // Schedulers jars path
    private static final String SCHEDULERS_REL_PATH = File.separator + "Runtime" + File.separator + "scheduler";

    // Subcomponents
    protected TaskScheduler scheduler;

    // Logging
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TD_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String ERR_LOAD_SCHEDULER = "Error loading scheduler";


    /**
     * Creates a new task dispatcher instance.
     */
    @SuppressWarnings("unchecked")
    public <A extends WorkerResourceDescription> TaskDispatcher() {
        super("Task Dispatcher", LOGGER);

        // Load scheduler jars
        loadSchedulerJars();

        // Parse interface
        CEIParser.parse();

        // Load resources
        ResourceManager.load(this);

        // Initialize structures
        String schedFQN = System.getProperty(COMPSsConstants.SCHEDULER);
        try {
            scheduler = TaskScheduler.constructScheduler(schedFQN, this);
        } catch (Exception e) {
            ErrorManager.fatal(ERR_LOAD_SCHEDULER, e);
        }

        // Insert workers
        for (Worker<?> worker : ResourceManager.getStaticResources()) {
            Worker<A> w = (Worker<A>) worker;
            scheduler.updateWorker(w, new PerformedIncrease<A>(w.getDescription()));
        }
        start();
        LOGGER.info("Initialization finished");
    }

    @Override
    public TraceEvent getThreadEvent() {
        return TraceEvent.TD_THREAD_ID;
    }

    @Override
    public void handleRequest(TDRequest request) throws ShutdownException, COMPSsException {
        request.process(scheduler);
    }

    /**
     * Adds a new execute task request.
     *
     * @param ap Access processor.
     * @param task Task to execute.
     */
    public void executeTask(AccessProcessor ap, Task task) {
        if (DEBUG) {
            StringBuilder sb = new StringBuilder("Schedule task: ");
            sb.append(task.getTaskDescription().getName()).append("(").append(task.getId()).append(") ");
            LOGGER.debug(sb);
        }
        ExecuteTasksRequest request = new ExecuteTasksRequest(ap, (Task) task);
        offerRequest(request, "execute task");
    }

    /**
     * Cancels the execution of a set of tasks.
     *
     * @param task task to cancel
     * @param listener object to notify when the tasks have been cancelled
     */
    public void cancelTasks(Task task, RequestListener listener) {
        CancelTaskRequest request = new CancelTaskRequest(task, listener);
        offerRequest(request, "cancel tasks");
    }

    // Notification thread
    @Override
    public void actionRunning(AllocatableAction action) {
        ActionUpdate request = new ActionUpdate(action, ActionUpdate.Update.RUNNING);
        offerRequest(request, "action running");
    }

    // Notification thread
    @Override
    public void actionCompletion(AllocatableAction action) {
        ActionUpdate request = new ActionUpdate(action, ActionUpdate.Update.COMPLETED);
        offerRequest(request, "action completed");
    }

    // Notification thread
    @Override
    public void actionError(AllocatableAction action) {
        ActionUpdate request = new ActionUpdate(action, ActionUpdate.Update.ERROR);
        offerRequest(request, "action error");
    }

    // Notification thread
    @Override
    public void actionException(AllocatableAction action, COMPSsException e) {
        ActionUpdate request = new ActionUpdate(action, ActionUpdate.Update.EXCEPTION);
        request.setCOMPSsException(e);
        offerRequest(request, "action exception");
    }

    @Override
    public void actionUpgrade(AllocatableAction action) {
        scheduler.upgradeAction(action);
    }

    /**
     * Adds a new tasks summary request.
     *
     * @param logger Logger whether to print the tasks summary.
     */
    public void getTaskSummary(Logger logger) {
        Semaphore sem = new Semaphore(0);
        TaskSummaryRequest request = new TaskSummaryRequest(logger, sem);
        offerRequest(request, "get task summary");
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns a string with the description of the tasks in the graph.
     *
     * @return The description of the current tasks in the graph.
     */
    public String getCurrentMonitoringData() {
        Semaphore sem = new Semaphore(0);
        MonitoringDataRequest request = new MonitoringDataRequest(sem);
        offerRequest(request, "getMonitorData");
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return request.getResponse();
    }

    /**
     * Adds a new request to print the current state.
     */
    public void printCurrentState() {
        PrintCurrentLoadRequest request = new PrintCurrentLoadRequest();
        offerRequest(request, "print current state");
    }

    /**
     * Adds a new request to print the current monitor graph.
     *
     * @param graph BufferedWriter whether to print the current monitor graph.
     */
    public void printCurrentGraph(BufferedWriter graph) {
        Semaphore sem = new Semaphore(0);
        PrintCurrentGraphRequest request = new PrintCurrentGraphRequest(sem, graph);
        offerRequest(request, "print current GRaph");

        // Synchronize until request has been processed
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public <T extends WorkerResourceDescription> void restartedResource(Worker<T> r, ResourceUpdate<T> modification) {
        WorkerRestartRequest<T> request = new WorkerRestartRequest<>(r, modification);
        offerRequestWithPriority(request, "restart resource");
    }

    @Override
    public <T extends WorkerResourceDescription> void updatedResource(Worker<T> r, ResourceUpdate<T> modification) {
        WorkerUpdateRequest<T> request = new WorkerUpdateRequest<>(r, modification);
        offerRequestWithPriority(request, "update resource");
    }

    /**
     * Adds a new request to add a new interface.
     *
     * @param forName Class name of the interface.
     */
    public void addInterface(Class<?> forName) {
        if (DEBUG) {
            LOGGER.debug("Updating CEI " + forName.getName());
        }
        Semaphore sem = new Semaphore(0);
        UpdateLocalCEIRequest request = new UpdateLocalCEIRequest(forName, sem);
        offerRequest(request, "add interface");

        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (DEBUG) {
            LOGGER.debug("Updated CEI " + forName.getName());
        }
    }

    /**
     * Adds a new request to register a new CoreElement.
     *
     * @param ced CoreElementDefinition to register.
     */
    public void registerNewCoreElement(CoreElementDefinition ced) {

        if (DEBUG) {
            LOGGER.debug("Requesting the registration of new CoreElement " + ced);
        }

        Semaphore sem = new Semaphore(0);

        CERegistration request = new CERegistration(ced, sem);
        if (request.isUseful()) {
            if (DEBUG) {
                LOGGER.debug("All implementations of CoreElement " + ced.getCeSignature() + " already registered");
            }
            return;
        }
        offerRequest(request, "register new CoreElement");

        // Waiting for registration
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (DEBUG) {
            LOGGER.debug("Registered new CoreElement");
        }
    }

    /**
     * Shuts down the component.
     */
    public void shutdown() {
        Semaphore sem = new Semaphore(0);
        ShutdownRequest request = new ShutdownRequest(sem);
        offerRequest(request, "shutdown");
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "TaskDispatcher[Instance" + this.hashCode() + "]";
    }

    private static void loadSchedulerJars() {
        LOGGER.info("Loading schedulers...");
        String compssHome = System.getenv(COMPSsConstants.COMPSS_HOME);

        if (compssHome == null || compssHome.isEmpty()) {
            LOGGER.warn("WARN: COMPSS_HOME not defined, no schedulers loaded.");
            return;
        }

        Classpath.loadJarsInPath(compssHome + SCHEDULERS_REL_PATH, LOGGER);
    }

}
