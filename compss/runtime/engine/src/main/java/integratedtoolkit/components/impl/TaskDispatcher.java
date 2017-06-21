package integratedtoolkit.components.impl;

import integratedtoolkit.types.request.td.ActionUpdate;
import integratedtoolkit.types.request.td.CERegistration;
import integratedtoolkit.types.request.td.ExecuteTasksRequest;
import integratedtoolkit.types.request.td.MonitoringDataRequest;
import integratedtoolkit.types.request.td.PrintCurrentGraphRequest;
import integratedtoolkit.types.request.td.ShutdownRequest;
import integratedtoolkit.types.request.td.TDRequest;
import integratedtoolkit.types.request.td.TaskSummaryRequest;
import integratedtoolkit.types.request.td.UpdateLocalCEIRequest;
import integratedtoolkit.types.request.td.WorkerUpdateRequest;
import integratedtoolkit.ITConstants;
import integratedtoolkit.components.ResourceUser;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.implementations.AbstractMethodImplementation.MethodType;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.request.td.PrintCurrentLoadRequest;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.resources.updates.PerformedIncrease;
import integratedtoolkit.types.resources.updates.ResourceUpdate;
import integratedtoolkit.util.CEIParser;
import integratedtoolkit.util.Classpath;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.ResourceManager;
import integratedtoolkit.util.Tracer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Component used as interface between the task analysis and the task scheduler
 * Manage and handles requests for task execution, task status, etc.
 *
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

    private static final String ERR_LOAD_SCHEDULER = "Error loading scheduler";
    private static final String ERROR_QUEUE_OFFER = "ERROR: TaskDispatcher queue offer error on ";

    /**
     * Creates a new task dispatcher instance
     *
     */
    @SuppressWarnings("unchecked")
    public TaskDispatcher() {
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
        scheduler = constructScheduler();
        if (scheduler == null) {
            ErrorManager.fatal(ERR_LOAD_SCHEDULER);
        }
        scheduler.setOrchestrator(this);

        keepGoing = true;

        if (Tracer.basicModeEnabled()) {
            Tracer.enablePThreads();
        }
        dispatcher.start();
        if (Tracer.basicModeEnabled()) {
            Tracer.disablePThreads();
        }

        // Insert workers
        for (Worker<? extends WorkerResourceDescription> worker : ResourceManager.getAllWorkers()) {
            scheduler.updateWorker(worker, new PerformedIncrease(worker.getDescription()));
        }
        LOGGER.info("Initialization finished");
    }

    // Dispatcher thread
    @Override
    public void run() {
        while (keepGoing) {
            String requestType = "Not defined";
            try {
                TDRequest request = requestQueue.take();
                requestType = request.getType().toString();
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.getTDRequestEvent(request.getType().name()).getId(), Tracer.getRuntimeEventsType());
                }
                request.process(scheduler);
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
            } catch (InterruptedException ie) {
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                Thread.currentThread().interrupt();
                continue;
            } catch (ShutdownException se) {
                LOGGER.debug("Exiting dispatcher because of shutting down");
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                se.getSemaphore().release();
                break;
            } catch (Exception e) {
                LOGGER.error("Error in request " + requestType, e);
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                continue;
            }
        }
    }

    /**
     * Adds a new request to the task dispatcher
     *
     * @param request
     */
    private void addRequest(TDRequest request) {
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "add request");
        }
    }

    /**
     * Adds a new prioritary request to the task dispatcher
     *
     * @param request
     */
    private void addPrioritaryRequest(TDRequest request) {
        if (!requestQueue.offerFirst(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "add prioritary request");
        }
    }

    /**
     * Adds a new execute task request
     *
     * @param producer
     * @param task
     */
    public void executeTask(TaskProducer producer, Task task) {
        if (DEBUG) {
            StringBuilder sb = new StringBuilder("Schedule task: ");
            sb.append(task.getTaskDescription().getName()).append("(").append(task.getId()).append(") ");
            LOGGER.debug(sb);
        }
        ExecuteTasksRequest request = new ExecuteTasksRequest(producer, task);
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

    /**
     * Adds a new task summary request
     *
     * @param logger
     */
    public void getTaskSummary(Logger logger) {
        Semaphore sem = new Semaphore(0);
        TaskSummaryRequest request = new TaskSummaryRequest(LOGGER, sem);
        addRequest(request);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns a string with the description of the tasks in the graph
     *
     * @return description of the current tasks in the graph
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
     * Adds a new request to print the current state
     */
    public void printCurrentState() {
        PrintCurrentLoadRequest request = new PrintCurrentLoadRequest();
        addRequest(request);
    }

    /**
     * Adds a new request to print the current monitor graph
     *
     * @param graph
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

    @SuppressWarnings("unchecked")
    @Override
    public <T extends WorkerResourceDescription> void updatedResource(Worker<T> r, ResourceUpdate<T> modification) {
        WorkerUpdateRequest<T> request = new WorkerUpdateRequest(r, modification);
        addPrioritaryRequest(request);
    }

    /**
     * Adds a new request to add a new interface
     *
     * @param forName
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
     * Adds a new request to register a new CoreElement
     *
     * @param coreElementSignature
     * @param implSignature
     * @param implConstraints
     * @param implType
     * @param implTypeArgs
     */
    public void registerNewCoreElement(String coreElementSignature, String implSignature, MethodResourceDescription implConstraints,
            MethodType implType, String[] implTypeArgs) {

        if (DEBUG) {
            LOGGER.debug("Registering new CoreElement");
        }
        Semaphore sem = new Semaphore(0);
        CERegistration request = new CERegistration(coreElementSignature, implSignature, implConstraints, implType, implTypeArgs,
                sem);
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
     * Shuts down the component
     *
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
        String itHome = System.getenv(ITConstants.IT_HOME);

        if (itHome == null || itHome.isEmpty()) {
            LOGGER.warn("WARN: IT_HOME not defined, no schedulers loaded.");
            return;
        }

        try {
            Classpath.loadPath(itHome + SCHEDULERS_REL_PATH, LOGGER);
        } catch (FileNotFoundException ex) {
            LOGGER.warn("WARN: Schedulers folder not defined, no schedulers loaded.");
        }
    }

    @SuppressWarnings("unchecked")
    private TaskScheduler constructScheduler() {
        TaskScheduler scheduler = null;
        try {
            String schedFQN = System.getProperty(ITConstants.IT_SCHEDULER);
            Class<?> schedClass = Class.forName(schedFQN);
            Constructor schedCnstr = schedClass.getDeclaredConstructors()[0];
            scheduler = (TaskScheduler) schedCnstr.newInstance();
            if (DEBUG) {
                LOGGER.debug("Loaded scheduler " + scheduler);
            }
        } catch (Exception e) {
            ErrorManager.fatal(ERR_LOAD_SCHEDULER, e);
        }
        return scheduler;
    }

}
