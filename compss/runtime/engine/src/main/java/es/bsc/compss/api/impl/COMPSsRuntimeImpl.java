/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.api.impl;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.ApplicationRunner;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.api.Workflow;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.components.impl.socketserver.SocketServer;
import es.bsc.compss.components.monitor.impl.RuntimeMonitor;
import es.bsc.compss.loader.total.StreamRegistry;
import es.bsc.compss.log.LoggerManager;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.ErrorHandler;
import es.bsc.compss.types.WallClockTimerTask;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.implementations.ExecType;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.definition.ContainerDescription;
import es.bsc.compss.types.listeners.CancelTaskGroupOnResourceCreation;
import es.bsc.compss.types.resources.MasterResourceImpl;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.tracing.APIEvent;
import es.bsc.compss.types.tracing.APITracer;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.util.RuntimeConfigManager;
import es.bsc.compss.util.Tracer;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class COMPSsRuntimeImpl implements COMPSsRuntime, ErrorHandler {

    // Exception constants definition
    private static final String WARN_VERSION_PROPERTIES =
        "WARNING: COMPSs Runtime VERSION-BUILD" + " properties file could not be read";

    // COMPSS Version and buildnumber attributes
    private static final String COMPSs_VERSION;
    private static final String COMPSs_BUILDNUMBER;

    // Boolean for initialization
    private static boolean initialized = false;
    private boolean stopped = false;

    // Components
    private static AccessProcessor ap;
    private static TaskDispatcher td;
    private static SocketServer ss;

    // Monitor
    private static RuntimeMonitor runtimeMonitor;

    // Application Timer
    private static Timer timer = null;

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.API);
    // Data Provenance logger
    private static final Logger DP_LOGGER = LogManager.getLogger(Loggers.DATA_PROVENANCE);
    private static final boolean DP_ENABLED = Boolean.parseBoolean(System.getProperty(COMPSsConstants.DATA_PROVENANCE));

    static {
        // Load COMPSS version and buildNumber
        String version = null;
        String buildNum = null;
        try {
            Properties props = new Properties();
            props.load(COMPSsRuntimeImpl.class.getResourceAsStream("/version.properties"));
            version = props.getProperty("compss.version");
            buildNum = props.getProperty("compss.build");
            if (buildNum.endsWith("rnull")) {
                buildNum = buildNum.substring(0, buildNum.length() - 6);
            }
        } catch (IOException | NullPointerException e) {
            LOGGER.warn(WARN_VERSION_PROPERTIES);
        }
        COMPSs_VERSION = version;
        COMPSs_BUILDNUMBER = buildNum;

        RuntimeConfigManager.setProperties();

        // Start tracing system
        boolean tracingTaskDep = Boolean.parseBoolean(System.getProperty(COMPSsConstants.TRACING_TASK_DEPENDENCIES));
        String installDir = System.getenv(COMPSsConstants.COMPSS_HOME);
        Tracer.init(0, "master", installDir, tracingTaskDep);
        if (Tracer.isActivated()) {
            Tracer.emitEvent(APIEvent.STATIC_IT);
        }

        /*
         * Initializes the COMM library and the MasterResource (Master reconfigures the logger)
         */
        Comm.init(new MasterResourceImpl());
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(APIEvent.STATIC_IT);
        }
    }

    /*
     * ************************************************************************************************************
     * CONSTRUCTOR
     * ************************************************************************************************************
     */


    /**
     * Creates a new COMPSs Runtime instance.
     */
    public COMPSsRuntimeImpl() {
        if (COMPSs_VERSION == null) {
            LOGGER.debug("Deploying COMPSs Runtime");
        } else {
            if (COMPSs_BUILDNUMBER == null) {
                LOGGER.debug("Deploying COMPSs Runtime v" + COMPSs_VERSION);
            } else {
                LOGGER.debug("Deploying COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
            }
        }
        ErrorManager.init(this);
        ((MasterResourceImpl) Comm.getAppHost()).setupNestedSupport(this);
    }

    /*
     * ************************************************************************************************************
     * ***************************************** RUNTIME CONTROL **************************************************
     * ************************************************************************************************************
     */
    @Override
    public synchronized void startIT() {
        APITracer.traced(APIEvent.START, (Runnable) () -> {
            // Console Log
            Thread.currentThread().setName("APPLICATION");
            if (COMPSs_VERSION == null) {
                LOGGER.warn("Starting COMPSs Runtime");
            } else {
                if (COMPSs_BUILDNUMBER == null) {
                    LOGGER.warn("Starting COMPSs Runtime v" + COMPSs_VERSION);
                } else {
                    LOGGER.warn("Starting COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
                }
            }

            // Init Runtime
            if (!initialized) {
                // Application
                synchronized (this) {
                    LOGGER.debug("Initializing components");

                    // Initialize main runtime components
                    td = new TaskDispatcher();
                    ap = new AccessProcessor(td);
                    if ("true".equals(System.getProperty(COMPSsConstants.SOCKET_MODE))) {
                        String socketPath = System.getProperty(COMPSsConstants.SOCKET_PATH);
                        ss = new SocketServer(this, socketPath);
                        try {
                            ss.start();
                        } catch (IOException ioe) {
                            ErrorManager.fatal("Unable to start runtime socket server", ioe);
                        }
                    }

                    // Initialize runtime tools components
                    runtimeMonitor = new RuntimeMonitor(ap, td);
                    WorkflowImpl.setAP(ap);
                    Application.setGH(runtimeMonitor.getGraphHandler());

                    // Log initialization
                    initialized = true;
                    LOGGER.debug("Ready to process tasks");
                }
            }

            if (DP_ENABLED) {
                DP_LOGGER.info(COMPSs_VERSION);
                DP_LOGGER.info(System.getProperty(COMPSsConstants.APP_NAME));
                DP_LOGGER.info(Instant.now().truncatedTo(ChronoUnit.MICROS).toString());
            }
        });
    }

    @Override
    public void stopIT(boolean terminate) {
        synchronized (this) {
            if (!stopped) {
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(APIEvent.STOP);
                }

                LOGGER.debug("Stopping Wall Clock limit Timer");
                if (timer != null) {
                    timer.cancel();
                }

                LOGGER.debug("Cancelling all remaining tasks...");
                // In some case, when runtime is stop because an error the java process is not stopped
                // because some threads are blocked at barriers waiting for the end of tasks
                for (Application app : Application.getApplications()) {
                    ap.cancelApplicationTasks(app);
                    // ap.barrier(app);
                }
                // Add task summary
                boolean taskSummaryEnabled = System.getProperty(COMPSsConstants.TASK_SUMMARY) != null
                    && !System.getProperty(COMPSsConstants.TASK_SUMMARY).isEmpty()
                    && Boolean.parseBoolean(System.getProperty(COMPSsConstants.TASK_SUMMARY));
                if (taskSummaryEnabled) {
                    td.getTaskSummary(LOGGER);
                }

                // Stop monitor components
                LOGGER.info("Stop IT reached");
                runtimeMonitor.shutdown();

                // Stop runtime components
                LOGGER.debug("Stopping AP...");
                if (ap != null) {
                    ap.shutdown();
                } else {
                    LOGGER.debug("AP was not initialized...");
                }
                runtimeMonitor.getGraphHandler().removeCurrentGraph();

                LOGGER.debug("Stopping TD...");
                if (td != null) {
                    td.shutdown();
                } else {
                    LOGGER.debug("TD was not initialized...");
                }

                LOGGER.debug("Stopping Comm...");
                Comm.stop();
                if (ss != null) {
                    try {
                        ss.stop();
                    } catch (IOException ioe) {
                        LOGGER.warn("Failed to stop socket server cleanly", ioe);
                    }
                }
                // LOGGER.debug("Releasing all barriers...");
                // In some case, when runtime is stop because an error the java process is not stopped
                // because some threads are blocked at barriers waiting for the end of tasks
                for (Application app : Application.getApplications()) {
                    app.getBaseTaskGroup().releaseBarrier();
                }
                if (Tracer.isActivated()) {
                    // Emit last EVENT_END event for STOP
                    Tracer.emitEventEnd(APIEvent.STOP);
                    LOGGER.debug("Stopping tracing...");

                    // Stop tracing system
                    Tracer.fini();
                    // Generate Trace
                    Tracer.generateMasterPackage();
                }
                LOGGER.debug("Runtime stopped");
                stopped = true;
            } else {
                LOGGER.debug("Duplicated Stop");
                throw (new RuntimeException("Runtime already stopped"));
            }

        }
        LOGGER.warn("Execution Finished");

        if (DP_ENABLED) {
            DP_LOGGER.info(Instant.now().truncatedTo(ChronoUnit.MICROS).toString());
        }

    }

    /*
     * ************************************************************************************************************
     * ************************************* RUNTIME SETUP MANAGEMENT *********************************************
     * ************************************************************************************************************
     */

    @Override
    public String getTempDir() {
        return Comm.getAppHost().getWorkingDirectory();
    }

    @Override
    public String getApplicationDirectory() {
        return LoggerManager.getLogDir();
    }

    /**
     * Returns the action orchestrator associated to the Runtime (only for testing purposes).
     *
     * @return The action orchestrator associated to the Runtime.
     */
    public static ActionOrchestrator getOrchestrator() {
        return td;
    }

    /*
     * ************************************************************************************************************
     * ************************************* APPLICATION MANAGEMENT ***********************************************
     * ************************************************************************************************************
     */

    @Override
    public Workflow registerWorkflow(String parallelismSource, ApplicationRunner runner) {
        return APITracer.traced(APIEvent.REGISTER_APP, () -> {
            LOGGER.info("Thread " + Thread.currentThread().getName() + " registering workflow " //
                + (parallelismSource != null ? "parallelism source: " + parallelismSource : "") //
                + (runner != null ? "runner: " + runner : ""));
            Workflow wf = new WorkflowImpl(parallelismSource, runner);
            LOGGER.info("Thread " + Thread.currentThread().getName() + " registered workflow " + wf.getId());
            return wf;
        });
    }

    @Override
    public void registerCoreElement(String coreElementSignature, String implSignature, String implConstraints,
        String implType, String implLocal, String implIO, String[] prolog, String[] epilog, String[] container,
        String... implTypeArgs) {
        APITracer.traced(APIEvent.REGISTER_CE, (Runnable) () -> {
            LOGGER.info("Registering CoreElement " + coreElementSignature);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("\t - Implementation: " + implSignature);
                LOGGER.debug("\t - Constraints   : " + implConstraints);
                LOGGER.debug("\t - Local process : " + implLocal);
                LOGGER.debug("\t - Type          : " + implType);
                LOGGER.debug("\t - I/O           : " + implIO);
                LOGGER.debug("\t - Prolog        : ");
                for (String pro : prolog) {
                    LOGGER.debug("\t\t -- : " + pro);
                }
                LOGGER.debug("\t - Epilog        : ");
                for (String epi : epilog) {
                    LOGGER.debug("\t\t -- : " + epi);
                }

                LOGGER.debug("\t - Container        : ");
                for (String cont : container) {
                    LOGGER.debug("\t\t -- : " + cont);
                }

                LOGGER.debug("\t - ImplTypeArgs  : ");
                for (String implTypeArg : implTypeArgs) {
                    LOGGER.debug("\t\t Arg: " + implTypeArg);
                }
            }

            MethodResourceDescription mrd = new MethodResourceDescription(implConstraints);
            boolean isImplIO = Boolean.parseBoolean(implIO);
            if (isImplIO) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Nulling computing resources for I/O task: " + implSignature);
                }
                mrd.setIOResources();
            }
            boolean isLocalImpl;
            isLocalImpl = Boolean.parseBoolean(implLocal);

            CoreElementDefinition ced = new CoreElementDefinition();
            ced.setCeSignature(coreElementSignature);

            ExecType pro = null;
            if (prolog != null && prolog.length > 0) {
                if (prolog.length != ExecType.ARRAY_LENGTH) {
                    throw new IllegalArgumentException("Incorrect number of parameters in prolog.");
                }
                pro = new ExecType(prolog[0], prolog[1], Boolean.parseBoolean(prolog[2]));
                if (!pro.isAssigned()) {
                    pro = null;
                }
            }

            ExecType epi = null;
            if (epilog != null && epilog.length > 0) {
                if (epilog.length != ExecType.ARRAY_LENGTH) {
                    throw new IllegalArgumentException("Incorrect number of parameters in epilog.");
                }
                epi = new ExecType(epilog[0], epilog[1], Boolean.parseBoolean(epilog[2]));
                if (!epi.isAssigned()) {
                    epi = null;
                }
            }

            ContainerDescription cont;
            if (container != null && container.length > 0 && container[0] != null && !container[0].isEmpty()
                && !container[0].equals(Constants.UNASSIGNED)) {
                String engineStr = container[0].toUpperCase();
                ContainerDescription.ContainerEngine engine = ContainerDescription.ContainerEngine.valueOf(engineStr);
                cont = new ContainerDescription(engine, container[1], container[2]);
            } else {
                cont = null;
            }

            ImplementationDescription<?, ?> implDef = ImplementationDescription.defineImplementation(implType,
                implSignature, isLocalImpl, mrd, pro, epi, cont, implTypeArgs);
            ced.addImplementation(implDef);

            td.registerNewCoreElement(ced);
        });
    }

    @Override
    public void registerCoreElement(CoreElementDefinition ced) {
        APITracer.traced(APIEvent.REGISTER_CE, (Runnable) () -> {
            LOGGER.info("Registering CoreElement " + ced.getCeSignature());
            if (LOGGER.isDebugEnabled()) {
                int implId = 0;
                for (ImplementationDescription<?, ?> implDef : ced.getImplementations()) {
                    LOGGER.debug("\t - Implementation " + implId + ":");
                    try {
                        LOGGER.debug(implDef.toString());
                    } catch (Exception e) {
                        LOGGER.debug("Error printing implDef", e);
                    }
                }
            }

            td.registerNewCoreElement(ced);
        });
    }

    /**
     * Notifies the runtime that an application will not produce more tasks.
     *
     * @param app Application that finished generating tasks
     */
    public void noMoreTasks(Application app) {
        LOGGER.info("No more tasks for app " + app.getId());
        // Wait until all tasks have finished
        ap.noMoreTasks(app);
        if (!stopped) {
            app.cancelTimerTask();
            // Retrieve result files
            LOGGER.debug("Getting Result Files for app" + app.getId());
            ap.getResultFiles(app);
        }
    }

    /*
     * ************************************************************************************************************
     * ************************************** OTHER FUNCTIONALITIES ***********************************************
     * ************************************************************************************************************
     */
    @Override
    public void emitEvent(int type, long id) {
        Tracer.emitEvent(type, id);
    }

    /*
     * ************************************************************************************************************
     * FatalErrorHandler INTERFACE
     * ************************************************************************************************************
     */
    @Override
    public boolean handleError() {
        return handleFatalError();
    }

    @Override
    public boolean handleFatalError() {
        if (DP_ENABLED) {
            DP_LOGGER.info("master status FAILED");
        }
        ErrorManager.info("Shutting down COMPSs...", null, System.err);
        new Thread() {

            public void run() {
                ErrorManager.logError("Error detected. Shutting down COMPSs", null);
                COMPSsRuntimeImpl.this.stopIT(true);
                ErrorManager.logError("Shutting down the running process", null);
                Runtime.getRuntime().halt(1);
            }
        }.start();
        return true;
    }

    /*
     * ************************************************************************************************************ Wall
     * Clock Manager
     * ************************************************************************************************************
     */

    private void createWallClockReaper() {
        // Enable thread detection on tracing
        if (Tracer.isActivated()) {
            Tracer.enablePThreads(1);
        }
        // Create Timer
        timer = new Timer("Application wall clock limit timer");

        if (Tracer.isActivated()) {
            // Register new timerTask to be executed immediately. It emits threadID event and disables thread detection.
            timer.schedule(new TimerTask() {

                @Override
                public void run() {
                    Tracer.disablePThreads(1);
                    Tracer.emitEvent(TraceEvent.WALLCLOCK_THREAD_ID);

                }
            }, 0);
        }
    }

    @Override
    public void setWallClockLimit(Long appId, long wcl, boolean stopRT) {
        APITracer.traced(APIEvent.SET_WALLCLOCK, (Runnable) () -> {
            if (wcl > 0) {
                if (timer == null) {
                    createWallClockReaper();
                }
                LOGGER.info("Setting wall clock limit for app " + appId + " of " + wcl + " seconds.");
                Application app = Application.registerApplication(appId);
                WallClockTimerTask wcTask = new WallClockTimerTask(app, ap, (stopRT ? this : null));
                app.setTimerTask(wcTask);
                // One second is added to allow possible stop from the binding
                timer.schedule(wcTask, (wcl + 1) * 1000);
            }
        });
    }
}
