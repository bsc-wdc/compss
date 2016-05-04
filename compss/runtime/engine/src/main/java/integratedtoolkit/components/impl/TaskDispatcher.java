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
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.ResourceManager;
import integratedtoolkit.util.Tracer;
import java.io.File;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

public class TaskDispatcher implements Runnable, ResourceUser, ActionOrchestrator {

    public interface TaskProducer {

        public void notifyTaskEnd(Task task);
    }

    // Subcomponents
    protected TaskScheduler scheduler;

    protected LinkedBlockingDeque<TDRequest> requestQueue;
    // Scheduler thread
    protected Thread dispatcher;
    protected boolean keepGoing;

    // Logging
    protected static final Logger logger = Logger.getLogger(Loggers.TD_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    private static final String CREAT_INIT_VM_ERR = "Error creating initial VMs";

    // Tracing
    protected static boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;

    public TaskDispatcher() {
        requestQueue = new LinkedBlockingDeque<TDRequest>();
        dispatcher = new Thread(this);
        dispatcher.setName("Task Dispatcher");

        try {
            URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
            Class<?> sysclass = URLClassLoader.class;
            String itHome = System.getenv("IT_HOME");
            Method method = sysclass.getDeclaredMethod("addURL", new Class[]{URL.class});
            method.setAccessible(true);
            File directory = new File(itHome + File.separator + "scheduler");
            File[] jarList = directory.listFiles();
            for (File jar : jarList) {
                try {
                    method.invoke(sysloader, new Object[]{(new File(jar.getAbsolutePath())).toURI().toURL()});
                } catch (Exception e) {
                    logger.error("COULD NOT LOAD SCHEDULER JAR " + jar.getAbsolutePath(), e);
                }

            }
        } catch (Exception e) {
            //Could not load any scheduler.
            //DO nothing
            e.printStackTrace();
        }

        CEIParser.parse();

        ResourceManager.load(this);

        try {
            String schedulerPath = System.getProperty(ITConstants.IT_SCHEDULER);
            schedulerPath = "integratedtoolkit.scheduler.readyscheduler.ReadyScheduler";
            if (schedulerPath == null || schedulerPath.compareTo("default") == 0) {
                scheduler = new TaskScheduler();
            } else {
                Class<?> conClass = Class.forName(schedulerPath);
                Constructor<?> ctor = conClass.getDeclaredConstructors()[0];
                scheduler = (TaskScheduler) ctor.newInstance();
            }
        } catch (Exception e) {
            ErrorManager.fatal(CREAT_INIT_VM_ERR, e);
        }

        keepGoing = true;

        if (Tracer.basicModeEnabled()) {
            Tracer.enablePThreads();
        }
        dispatcher.start();

        AllocatableAction.orchestrator = this;

        if (Tracer.basicModeEnabled()) {
            Tracer.disablePThreads();
        }

        logger.info("Initialization finished");
    }

    // Dispatcher thread
    public void run() {
        while (keepGoing) {
            try {
                TDRequest request = requestQueue.take();
                if (tracing){
                    Tracer.masterEventStart(Tracer.getTDRequestEvent(request.getType().name()).getId());
                }
                request.process(scheduler);
                if (tracing){
                    Tracer.masterEventFinish();
                }
            } catch (InterruptedException ie) {
                if (tracing){
                    Tracer.masterEventFinish();
                }
                continue;
            } catch (ShutdownException se) {
                logger.debug("Exiting dispatcher because of shutting down");
                if (tracing){
                    Tracer.masterEventFinish();
                }
                break;
            } catch (Exception e) {
                logger.error("RequestError", e);
                if (tracing){
                    Tracer.masterEventFinish();
                }
                continue;
            }
        }
    }

    private void addRequest(TDRequest request) {
        requestQueue.offer(request);
    }

    private void addPrioritaryRequest(TDRequest request) {
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
    public void actionCompletion(AllocatableAction action) {
        addRequest(new ActionUpdate(action, ActionUpdate.Update.COMPLETED));
    }

    // Notification thread
    @Override
    public void actionError(AllocatableAction action) {
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
}
