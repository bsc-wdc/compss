package integratedtoolkit.components.impl;

import integratedtoolkit.types.request.td.UpdateLocalCEIRequest;
import integratedtoolkit.ITConstants;
import integratedtoolkit.components.ResourceUser;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Task;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.AllocatableAction.ActionOrchestrator;
import integratedtoolkit.types.request.td.*;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
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


public class TaskDispatcher<P extends Profile, T extends WorkerResourceDescription> implements Runnable, ResourceUser, ActionOrchestrator {

    public interface TaskProducer {

        public void notifyTaskEnd(Task task);
    }


    // Schedulers jars path
    private static final String SCHEDULERS_REL_PATH = File.separator + "Runtime" + File.separator + "scheduler";

    // Subcomponents
    protected TaskScheduler<P, T> scheduler;
    protected LinkedBlockingDeque<TDRequest<P, T>> requestQueue;

    // Scheduler thread
    protected Thread dispatcher;
    protected boolean keepGoing;

    // Logging
    protected static final Logger logger = LogManager.getLogger(Loggers.TD_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    private static final String ERR_LOAD_SCHEDULER = "Error loading scheduler";
    private static final String ERROR_QUEUE_OFFER = "ERROR: TaskDispatcher queue offer error on ";


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
        keepGoing = true;

        if (Tracer.basicModeEnabled()) {
            Tracer.enablePThreads();
        }
        dispatcher.start();
        if (Tracer.basicModeEnabled()) {
            Tracer.disablePThreads();
        }

        AllocatableAction.setOrchestrator(this);

        // Insert workers
        for (Worker<?> w : ResourceManager.getAllWorkers()) {
            Worker<T> worker = (Worker<T>) w;
            scheduler.updatedWorker(worker);
        }
        logger.info("Initialization finished");
    }

    // Dispatcher thread
    public void run() {
        while (keepGoing) {
            try {
                TDRequest<P, T> request = requestQueue.take();
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
                continue;
            } catch (ShutdownException se) {
                logger.debug("Exiting dispatcher because of shutting down");
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                se.getSemaphore().release();
                break;
            } catch (Exception e) {
                logger.error("RequestError", e);
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                continue;
            }
        }
    }

    private void addRequest(TDRequest<P, T> request) {
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "add request");
        }
    }

    private void addPrioritaryRequest(TDRequest<P, T> request) {
        if (!requestQueue.offerFirst(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "add prioritary request");
        }
    }

    // TP (TA)
    public void executeTask(TaskProducer producer, Task task) {
        if (debug) {
            StringBuilder sb = new StringBuilder("Schedule tasks: ");
            sb.append(task.getTaskDescription().getName()).append("(").append(task.getId()).append(") ");
            logger.debug(sb);
        }
        ExecuteTasksRequest<P, T> request = new ExecuteTasksRequest<P, T>(producer, task);
        addRequest(request);
    }

    // Notification thread
    @Override
    public void actionCompletion(AllocatableAction<?, ?> action) {
        ActionUpdate<P, T> request = new ActionUpdate(action, ActionUpdate.Update.COMPLETED);
        addRequest(request);
    }

    // Notification thread
    @Override
    public void actionError(AllocatableAction<?, ?> action) {
        ActionUpdate<P, T> request = new ActionUpdate(action, ActionUpdate.Update.ERROR);
        addRequest(request);
    }

    // Scheduling optimizer thread
    @Override
    public WorkloadStatus getWorkload() {
        Semaphore sem = new Semaphore(0);
        GetCurrentScheduleRequest<P, T> request = new GetCurrentScheduleRequest<P, T>(sem);
        addPrioritaryRequest(request);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
        }

        return request.getResponse();
    }
    
    public void getTaskSummary(Logger logger) {
        Semaphore sem = new Semaphore(0);
        TaskSummaryRequest<P, T> request = new TaskSummaryRequest<P,T>(logger, sem);
        addRequest(request);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
        }
    }

    /**
     * Returns a string with the description of the tasks in the graph
     *
     * @return description of the current tasks in the graph
     */
    public String getCurrentMonitoringData() {
        Semaphore sem = new Semaphore(0);
        MonitoringDataRequest<P, T> request = new MonitoringDataRequest<P, T>(sem);
        addRequest(request);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
        }
        return (String) request.getResponse();
    }

    public void printCurrentGraph(BufferedWriter graph) {
        Semaphore sem = new Semaphore(0);
        PrintCurrentGraphRequest<P, T> request = new PrintCurrentGraphRequest<P, T>(sem, graph);
        addRequest(request);

        // Synchronize until request has been processed
        try {
            sem.acquire();
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void updatedResource(Worker<?> r) {
        WorkerUpdateRequest<P, T> request = new WorkerUpdateRequest(r);
        addPrioritaryRequest(request);
    }

    public void addInterface(Class<?> forName) {
        if (debug) {
            logger.debug("Updating CEI " + forName.getName());
        }
        Semaphore sem = new Semaphore(0);
        UpdateLocalCEIRequest<P, T> request = new UpdateLocalCEIRequest<P, T>(forName, sem);
        addRequest(request);

        try {
            sem.acquire();
        } catch (InterruptedException e) {
        }

        if (debug) {
            logger.debug("Updated CEI " + forName.getName());
        }
    }

    public void registerCEI(String signature, String methodName, String declaringClass, MethodResourceDescription constraints) {
        if (debug) {
            logger.debug("Registering CEI");
        }
        Semaphore sem = new Semaphore(0);
        CERegistration<P, T> request = new CERegistration<P, T>(signature, methodName, declaringClass, constraints, sem);
        addRequest(request);

        try {
            sem.acquire();
        } catch (InterruptedException e) {
        }

        if (debug) {
            logger.debug("Registered CEI");
        }
    }

    // TP (TA)
    public void shutdown() {
        Semaphore sem = new Semaphore(0);
        ShutdownRequest<P, T> request = new ShutdownRequest<P, T>(sem);
        addRequest(request);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
    }

    private static void loadSchedulerJars() {
        logger.info("Loading schedulers...");
        String itHome = System.getenv(ITConstants.IT_HOME);

        if (itHome == null || itHome.isEmpty()) {
            logger.warn("WARN: IT_HOME not defined, no schedulers loaded.");
            return;
        }

        try {
            Classpath.loadPath(itHome + SCHEDULERS_REL_PATH, logger);
        } catch (FileNotFoundException ex) {
            logger.warn("WARN: Schedulers folder not defined, no schedulers loaded.");
        }
    }

    @SuppressWarnings("unchecked")
    private TaskScheduler<P, T> constructScheduler() {
        TaskScheduler<P, T> scheduler = null;
        try {
            String schedFQN = System.getProperty(ITConstants.IT_SCHEDULER);
            Class<?> schedClass = Class.forName(schedFQN);
            Constructor<?> schedCnstr = schedClass.getDeclaredConstructors()[0];
            scheduler = (TaskScheduler<P, T>) schedCnstr.newInstance();
        } catch (Exception e) {
            ErrorManager.fatal(ERR_LOAD_SCHEDULER, e);
        }
        return scheduler;
    }

}
