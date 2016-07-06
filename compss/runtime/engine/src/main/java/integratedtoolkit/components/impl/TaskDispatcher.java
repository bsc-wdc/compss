package integratedtoolkit.components.impl;

import integratedtoolkit.types.request.td.UpdateLocalCEIRequest;
import integratedtoolkit.ITConstants;
import integratedtoolkit.components.ResourceUser;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Task;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.AllocatableAction.ActionOrchestrator;
import integratedtoolkit.types.request.td.*;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Worker;
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

import org.apache.log4j.Logger;

public class TaskDispatcher implements Runnable, ResourceUser, ActionOrchestrator {

    public interface TaskProducer {

        public void notifyTaskEnd(Task task);
    }
    
    // Schedulers jars path
    private static final String SCHEDULERS_REL_PATH = File.separator + "Runtime" + File.separator + "scheduler";

    // Subcomponents
    protected TaskScheduler<?, ?> scheduler;
    protected LinkedBlockingDeque<TDRequest<?, ?>> requestQueue;
    
    // Scheduler thread
    protected Thread dispatcher;
    protected boolean keepGoing;

    // Logging
    protected static final Logger logger = Logger.getLogger(Loggers.TD_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    private static final String ERR_LOAD_SCHEDULER = "Error loading scheduler";

    // Tracing
    protected static boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;

            
    public TaskDispatcher() {
        requestQueue = new LinkedBlockingDeque<TDRequest<?, ?>>();
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
        
        AllocatableAction.orchestrator = this;

        // Insert workers
        for (Worker w : ResourceManager.getAllWorkers()) {
            scheduler.updatedWorker(w);
        }
        logger.info("Initialization finished");
    }

    // Dispatcher thread
    public void run() {
        while (keepGoing) {
            try {
                TDRequest request = requestQueue.take();
                if (tracing) {
                    Tracer.emitEvent(Tracer.getTDRequestEvent(request.getType().name()).getId(), Tracer.getRuntimeEventsType());
                }
                request.process(scheduler);
                if (tracing) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
            } catch (InterruptedException ie) {
                if (tracing) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                continue;
            } catch (ShutdownException se) {
                logger.debug("Exiting dispatcher because of shutting down");
                if (tracing) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                break;
            } catch (Exception e) {
                logger.error("RequestError", e);
                if (tracing) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                continue;
            }
        }
    }

    private void addRequest(TDRequest<?, ?> request) {
        requestQueue.offer(request);
    }

    private void addPrioritaryRequest(TDRequest<?, ?> request) {
        requestQueue.offerFirst(request);
    }

    // TP (TA)
    public void executeTask(TaskProducer producer, Task task) {
        if (debug) {
            StringBuilder sb = new StringBuilder("Schedule tasks: ");
            sb.append(task.getTaskParams().getName()).append("(").append(task.getId()).append(") ");
            logger.debug(sb);
        }
        addRequest(new ExecuteTasksRequest(producer, task));
    }

    // Notification thread
    @Override
    public void actionCompletion(AllocatableAction<?, ?> action) {
        addRequest(new ActionUpdate(action, ActionUpdate.Update.COMPLETED));
    }

    // Notification thread
    @Override
    public void actionError(AllocatableAction<?, ?> action) {
        addRequest(new ActionUpdate(action, ActionUpdate.Update.ERROR));
    }

    // Scheduling optimizer thread
    public WorkloadStatus getWorkload() {
        Semaphore sem = new Semaphore(0);
        GetCurrentScheduleRequest request = new GetCurrentScheduleRequest(sem);
        addPrioritaryRequest(request);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
        }

        return request.getResponse();
    }

    /**
     * Returs a string with the description of the tasks in the graph
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
        }
        return (String) request.getResponse();
    }
    
    public void printCurrentGraph(BufferedWriter graph) {
    	Semaphore sem = new Semaphore(0);
    	PrintCurrentGraphRequest request = new PrintCurrentGraphRequest(sem, graph);
    	addRequest(request);
    	
    	// Synchronize until request has been processed
    	try {
            sem.acquire();
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void updatedResource(Worker<?> r) {
        WorkerUpdateRequest request = new WorkerUpdateRequest(r);
        addPrioritaryRequest(request);
    }

    public void addInterface(Class<?> forName) {
        if (debug) {
            logger.debug("Updating CEI " + forName.getName());
        }
        Semaphore sem = new Semaphore(0);
        addRequest(new UpdateLocalCEIRequest(forName, sem));

        try {
            sem.acquire();
        } catch (InterruptedException e) {
        }

        if (debug) {
            logger.debug("Updated CEI " + forName.getName());
        }
    }

    public void registerCEI(String signature, String declaringClass, MethodResourceDescription constraints) {
        if (debug) {
            logger.debug("Registering CEI");
        }
        Semaphore sem = new Semaphore(0);
        addRequest(new CERegistration(signature, declaringClass, constraints, sem));

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
        ShutdownRequest request = new ShutdownRequest(sem);
        addRequest(request);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
    }

    private static void loadSchedulerJars() {
        logger.info("Loading schedulers...");
        String itHome = System.getenv("IT_HOME");

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

    private TaskScheduler<?, ?> constructScheduler() {
        TaskScheduler<?, ?> scheduler = null;
        try {
            String schedFQN = System.getProperty(ITConstants.IT_SCHEDULER);
            Class<?> schedClass = Class.forName(schedFQN);
            Constructor<?> schedCnstr = schedClass.getDeclaredConstructors()[0];
            scheduler = (TaskScheduler<?, ?>) schedCnstr.newInstance();
        } catch (Exception e) {
            ErrorManager.fatal(ERR_LOAD_SCHEDULER, e);
        }
        return scheduler;
    }
}
