package integratedtoolkit.components.impl;

import integratedtoolkit.types.request.td.UpdateLocalCEIRequest;
import integratedtoolkit.ITConstants;
import integratedtoolkit.components.ResourceUser;
import integratedtoolkit.components.scheduler.impl.DefaultTaskScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.Task.TaskState;
import integratedtoolkit.types.request.td.*;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.util.CEIParser;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.ResourceManager;
import integratedtoolkit.util.Tracer;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;


public class TaskDispatcher implements Runnable, ResourceUser {

    // Other supercomponent
    protected AccessProcessor accessProcessor;
    // Subcomponents
    protected TaskScheduler scheduler;
    protected JobManager jobManager;

    protected LinkedBlockingDeque<TDRequest> requestQueue;
    // Scheduler thread
    protected Thread dispatcher;
    protected boolean keepGoing;

    //End of Execution
    private boolean endRequested;
    //Number of Tasks to execute
    private int[] taskCountToEnd;
    // Logging
    protected static final Logger logger = Logger.getLogger(Loggers.TD_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    private static final String RES_LOAD_ERR = "Error loading resource information";
    private static final String CREAT_INIT_VM_ERR = "Error creating initial VMs";

    // Tracing
    protected static boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;


    public TaskDispatcher() {
        endRequested = false;

        requestQueue = new LinkedBlockingDeque<TDRequest>();
        dispatcher = new Thread(this);
        dispatcher.setName("Task Dispatcher");

        CEIParser.parse();
        try {
            ResourceManager.load(this);
        } catch (ClassNotFoundException e) {
        	ErrorManager.fatal(CREAT_INIT_VM_ERR, e);
        } catch (Exception e) {
        	ErrorManager.fatal(RES_LOAD_ERR, e);
        }

        try {
            String schedulerPath = System.getProperty(ITConstants.IT_SCHEDULER);
            if (schedulerPath == null || schedulerPath.compareTo("default") == 0) {
                scheduler = new DefaultTaskScheduler();
            } else {
                Class<?> conClass = Class.forName(schedulerPath);
                Constructor<?> ctor = conClass.getDeclaredConstructors()[0];
                scheduler = (TaskScheduler) ctor.newInstance();
            }
        } catch (Exception e) {
        	ErrorManager.fatal(CREAT_INIT_VM_ERR, e);
        }

        jobManager = new JobManager();

        taskCountToEnd = new int[CoreManager.getCoreCount()];

        keepGoing = true;

        if (Tracer.basicModeEnabled()){
            Tracer.enablePThreads();
        }
        dispatcher.start();
        if (Tracer.basicModeEnabled()){
            Tracer.disablePThreads();
        }


        logger.info("Initialization finished");
    }

    public void setTP(AccessProcessor TP) {
        this.accessProcessor = TP;
        scheduler.setCoWorkers(jobManager);
        jobManager.setCoWorkers(TP, this);
    }

    // Dispatcher thread
    public void run() {
        while (keepGoing) {

            try {
                TDRequest request = requestQueue.take();
                dispatchRequest(request);
            } catch (InterruptedException ie) {
                continue;
            } catch (ShutdownException se) {
                logger.debug("Exiting dispatcher because of shutting down");
                break;
            } catch (Exception e) {
                e.printStackTrace();
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

    protected void dispatchRequest(TDRequest request) throws Exception {
        Task task;
        int coreId;
        int taskId;
        int implId;
        Worker resource;
        switch (request.getRequestType()) {
            case SCHEDULE_TASKS:
                if (tracing){
                    Tracer.masterEventStart(Tracer.Event.SCHEDULE_TASK.getId());
                }
                ScheduleTasksRequest stRequest = (ScheduleTasksRequest) request;
                List<Task> toSchedule = stRequest.getToSchedule();
                for (Task currentTask : toSchedule) {
                	int coreID = currentTask.getTaskParams().getId();
                	if (debug){
                		logger.debug("Treating Scheduling request for task " + currentTask.getId()+"(core "+coreID+")");
                	}
                	taskCountToEnd[coreID]++;
                    currentTask.setStatus(TaskState.TO_SCHEDULE);
                    scheduler.scheduleTask(currentTask);
                }
                if (tracing){
                    Tracer.masterEventFinish();
                }
                break;
            case FINISHED_TASK:
                if (tracing){
                    Tracer.masterEventStart(Tracer.Event.FINISHED_TASK.getId());
                }
                NotifyTaskEndRequest nte = (NotifyTaskEndRequest) request;
                task = nte.getTask();
                implId = nte.getImplementationId();
                resource = nte.getWorker();
                coreId = task.getTaskParams().getId();
                taskCountToEnd[coreId]--;
                scheduler.taskEnd(task, resource, implId);
                resource.endTask(CoreManager.getCoreImplementations(coreId)[implId].getRequirements());

                if (!scheduler.scheduleToResource(resource) && endRequested) {
                    // TODO: OPTIMIZATION - check if the resource could be removed since it has no load
                }
                if (tracing){
                    Tracer.masterEventFinish();
                }
                break;
            case RESCHEDULE_TASK:
                if (tracing){
                    Tracer.masterEventStart(Tracer.Event.RESCHEDULE_TASK.getId());
                }
                // Get the corresponding task to reschedule
                RescheduleTaskRequest rqr = (RescheduleTaskRequest) request;
                task = rqr.getTask();
                coreId = task.getTaskParams().getId();
                taskId = task.getId();
                implId = rqr.getImplementationId();
                resource = rqr.getResource();
                if (debug) {
                    logger.debug("Reschedule: Task " + taskId + " failed to run in " + resource.getName());
                }
                //register task execution end
                scheduler.taskEnd(task, resource, implId);
                resource.endTask(CoreManager.getCoreImplementations(coreId)[implId].getRequirements());

                scheduler.scheduleToResource(resource); //resource freed, we can reschedule another task to it
                if( !scheduler.rescheduleTask(task, resource) ) {
            		task.setLastResource(resource.getName());
                }
                if (tracing){
                    Tracer.masterEventFinish();
                }

                break;
            case NEW_WAITING_TASK:
                if (tracing){
                    Tracer.masterEventStart(Tracer.Event.NEW_WAITING_TASK.getId());
                }
                NewWaitingTaskRequest nwtRequest = (NewWaitingTaskRequest) request;
                if (tracing){
                    Tracer.masterEventFinish();
                }
                break;
            case DEBUG:
                if (tracing){
                    Tracer.masterEventStart(Tracer.Event.DEBUG_TASK.getId());
                    Tracer.masterEventFinish();
                }
                break;

            default:
                if (tracing){
                    Tracer.masterEventStart(Tracer.Event.DEFAULT_TASK.getId());
                    Tracer.masterEventFinish();
                }
                request.process(scheduler, jobManager);
        }
    }

    // TP (TA)
    public void scheduleTasks(List<Task> toSchedule, boolean waiting, int[] waitingCount) {
        if (debug) {
            StringBuilder sb = new StringBuilder("Schedule tasks: ");
            for (Task t : toSchedule) {
                sb.append(t.getTaskParams().getName()).append("(").append(t.getId()).append(") ");
            }
            logger.debug(sb);
        }
        addRequest(new ScheduleTasksRequest(toSchedule, waiting, waitingCount));
    }

    // Notification thread (JM)
    public void notifyJobEnd(Task task, int implId, Worker<?> resource) {
        addRequest(new NotifyTaskEndRequest(task, implId, resource));
    }

    // Notification thread (JM) / Transfer threads (FTM)
    public void rescheduleJob(Task task, int implId, Worker<?> failedResource) {
        task.setStatus(TaskState.TO_RESCHEDULE);
        addRequest(new RescheduleTaskRequest(task, implId, failedResource));
    }

    // TP (TA)
    public void newWaitingTask(int methodId) {
        addRequest(new NewWaitingTaskRequest(methodId));
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
    public void createdResources(Worker<?> r) {
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
