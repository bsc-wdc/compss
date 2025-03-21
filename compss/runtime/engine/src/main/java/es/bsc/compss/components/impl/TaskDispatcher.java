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
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.util.CEIParser;
import es.bsc.compss.util.Classpath;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;

import java.io.BufferedWriter;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Component used as interface between the task analysis and the task scheduler Manage and handles requests for task
 * execution, task status, etc.
 */
public class TaskDispatcher implements Runnable, ResourceUser, ActionOrchestrator {

    // Schedulers jars path
    private static final String SCHEDULERS_REL_PATH = File.separator + "Runtime" + File.separator + "scheduler";

    // Subcomponents
    protected TaskScheduler scheduler;
    protected LinkedBlockingDeque<TDRequest> requestQueue;

    // Scheduler thread
    protected Thread dispatcher;
    protected boolean keepGoing;

    // Logging
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TD_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String ERROR_QUEUE_OFFER = "ERROR: TaskDispatcher queue offer error on ";
    private static final String ERR_LOAD_SCHEDULER = "Error loading scheduler";


    /**
     * Creates a new task dispatcher instance.
     */
    @SuppressWarnings("unchecked")
    public <A extends WorkerResourceDescription> TaskDispatcher() {
        requestQueue = new LinkedBlockingDeque<>();
        dispatcher = new Thread(this);
        dispatcher.setName("Task Dispatcher");

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

        keepGoing = true;
        if (Tracer.isActivated()) {
            Tracer.enablePThreads(1);
        }
        dispatcher.start();

        LOGGER.info("Initialization finished");
    }

    // Dispatcher thread
    @Override
    public void run() {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.TD_THREAD_ID);
            Tracer.disablePThreads(1);
        }
        while (keepGoing) {
            String requestType = "Not defined";
            try {
                TDRequest request = requestQueue.take();
                requestType = request.getEvent().toString();

                if (Tracer.isActivated()) {
                    Tracer.emitEvent(request.getEvent());
                }
                request.process(scheduler);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                continue;
            } catch (ShutdownException se) {
                LOGGER.debug("Exiting dispatcher because of shutting down");
                se.getSemaphore().release();
                break;
            } catch (Exception e) {
                LOGGER.error("Error in TaskDispatcher request:" + e.getMessage());
                ErrorManager.error("Error in TaskDispatcher request " + requestType, e);
                continue;
            } finally {
                if (Tracer.isActivated()) {
                    Tracer.emitEventEnd(TraceEventType.RUNTIME);
                }
            }
        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.TD_THREAD_ID);
        }
    }

    /**
     * Adds a new request to the task dispatcher.
     *
     * @param request New Task Dispatcher request.
     */
    private void addRequest(TDRequest request) {
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "add request");
        }
    }

    /**
     * Adds a new prioritary request to the task dispatcher.
     *
     * @param request New Task Dispatcher request.
     */
    private void addPrioritaryRequest(TDRequest request) {
        if (!requestQueue.offerFirst(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "add prioritary request");
        }
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
            sb.append(((Task) task).getTaskDescription().getName()).append("(").append(task.getId()).append(") ");
            LOGGER.debug(sb);
        }
        ExecuteTasksRequest request = new ExecuteTasksRequest(ap, (Task) task);
        addRequest(request);
    }

    /**
     * Cancels the execution of a set of tasks.
     *
     * @param task task to cancel
     * @param listener object to notify when the tasks have been cancelled
     */
    public void cancelTasks(Task task, RequestListener listener) {
        CancelTaskRequest request = new CancelTaskRequest(task, listener);
        addRequest(request);
    }

    // Notification thread
    @Override
    public void actionRunning(AllocatableAction action) {
        ActionUpdate request = new ActionUpdate(action, ActionUpdate.Update.RUNNING);
        addRequest(request);
    }

    // Notification thread
    @Override
    public void actionCompletion(AllocatableAction action) {
        ActionUpdate request = new ActionUpdate(action, ActionUpdate.Update.COMPLETED);
        addRequest(request);
    }

    // Notification thread
    @Override
    public void actionError(AllocatableAction action) {
        ActionUpdate request = new ActionUpdate(action, ActionUpdate.Update.ERROR);
        addRequest(request);
    }

    // Notification thread
    @Override
    public void actionException(AllocatableAction action, COMPSsException e) {
        ActionUpdate request = new ActionUpdate(action, ActionUpdate.Update.EXCEPTION);
        request.setCOMPSsException(e);
        addRequest(request);
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
        addRequest(request);
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
        addRequest(request);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return (String) request.getResponse();
    }

    /**
     * Adds a new request to print the current state.
     */
    public void printCurrentState() {
        PrintCurrentLoadRequest request = new PrintCurrentLoadRequest();
        addRequest(request);
    }

    /**
     * Adds a new request to print the current monitor graph.
     *
     * @param graph BufferedWriter whether to print the current monitor graph.
     */
    public void printCurrentGraph(BufferedWriter graph) {
        Semaphore sem = new Semaphore(0);
        PrintCurrentGraphRequest request = new PrintCurrentGraphRequest(sem, graph);
        addRequest(request);

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
        addPrioritaryRequest(request);
    }

    @Override
    public <T extends WorkerResourceDescription> void updatedResource(Worker<T> r, ResourceUpdate<T> modification) {
        WorkerUpdateRequest<T> request = new WorkerUpdateRequest<>(r, modification);
        addPrioritaryRequest(request);
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
        addRequest(request);

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
        addRequest(request);

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
        addRequest(request);
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
