/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.COMPSsConstants.TaskExecution;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.data.BindingDataManager;
import es.bsc.compss.exceptions.AnnounceException;
import es.bsc.compss.executor.ExecutionManager;
import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.types.ExecutionListener;
import es.bsc.compss.executor.utils.ThreadedPrintStream;
import es.bsc.compss.invokers.types.CParams;
import es.bsc.compss.invokers.types.JavaParams;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.local.LocalJob;
import es.bsc.compss.local.LocalParameter;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.LocationType;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.types.execution.exceptions.UnloadableValueException;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Serializer;
import es.bsc.compss.worker.COMPSsException;
import es.bsc.distrostreamlib.server.types.StreamBackend;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


/**
 * Representation of the COMPSs Master Node Only 1 instance per execution.
 */
public final class COMPSsMaster extends COMPSsWorker implements InvocationContext {

    private static final String ERROR_COMPSs_LOG_BASE_DIR = "ERROR: Cannot create .COMPSs base log directory";
    private static final String ERROR_APP_OVERLOAD = "ERROR: Cannot erase overloaded directory";
    private static final String ERROR_APP_LOG_DIR = "ERROR: Cannot create application log directory";
    private static final String ERROR_TEMP_DIR = "ERROR: Cannot create temp directory";
    private static final String ERROR_JOBS_DIR = "ERROR: Cannot create jobs directory";
    private static final String ERROR_WORKERS_DIR = "ERROR: Cannot create workers directory";
    private static final String WARN_FOLDER_OVERLOAD = "WARNING: Reached maximum number of executions for this"
        + " application. To avoid this warning please clean .COMPSs folder";
    private static final String EXECUTION_MANAGER_ERR = "Error starting ExecutionManager";

    private static final int MAX_OVERLOAD = 100; // Maximum number of executions of same application
    public static final String SUFFIX_OUT = ".out";
    public static final String SUFFIX_ERR = ".err";

    private final String storageConf;
    private final TaskExecution executionType;

    private final String userExecutionDirPath;
    private final String compssLogBaseDirPath;
    private final String appLogDirPath;

    private final String installDirPath;
    private final String appDirPath;
    private final String tempDirPath;
    private final String jobsDirPath;
    private final String workersDirPath;

    private final LanguageParams[] langParams = new LanguageParams[COMPSsConstants.Lang.values().length];
    private boolean persistentEnabled;

    private ExecutionManager executionManager;
    private final ThreadedPrintStream out;
    private final ThreadedPrintStream err;
    private boolean started = false;


    /**
     * New COMPSs Master.
     */
    public COMPSsMaster() {
        super();

        // Gets user execution directory
        this.userExecutionDirPath = System.getProperty("user.dir");

        /* Creates base Runtime structure directories ************************** */
        boolean mustCreateExecutionSandbox = true;
        // Checks if specific log base dir has been given
        String specificOpt = System.getProperty(COMPSsConstants.SPECIFIC_LOG_DIR);
        if (specificOpt != null && !specificOpt.isEmpty()) {
            this.compssLogBaseDirPath =
                specificOpt.endsWith(File.separator) ? specificOpt : specificOpt + File.separator;
            mustCreateExecutionSandbox = false; // This is the only case where
            // the sandbox is provided
        } else {
            // Checks if base log dir has been given
            String baseOpt = System.getProperty(COMPSsConstants.BASE_LOG_DIR);
            if (baseOpt != null && !baseOpt.isEmpty()) {
                baseOpt = baseOpt.endsWith(File.separator) ? baseOpt : baseOpt + File.separator;
                this.compssLogBaseDirPath = baseOpt + ".COMPSs" + File.separator;
            } else {
                // No option given - load default (user home)
                this.compssLogBaseDirPath =
                    System.getProperty("user.home") + File.separator + ".COMPSs" + File.separator;
            }
        }

        if (!new File(this.compssLogBaseDirPath).exists()) {
            if (!new File(this.compssLogBaseDirPath).mkdir()) {
                ErrorManager.error(ERROR_COMPSs_LOG_BASE_DIR + " at " + compssLogBaseDirPath);
            }
        }

        // Load working directory. Different for regular applications and
        // services
        if (mustCreateExecutionSandbox) {
            String appName = System.getProperty(COMPSsConstants.APP_NAME);
            if (System.getProperty(COMPSsConstants.SERVICE_NAME) != null) {
                /*
                 * SERVICE - Gets appName - Overloads the service folder for different executions - MAX_OVERLOAD raises
                 * warning - Changes working directory to serviceName !!!!
                 */
                String serviceName = System.getProperty(COMPSsConstants.SERVICE_NAME);
                int overloadCode = 1;
                String appLog =
                    this.compssLogBaseDirPath + serviceName + "_0" + String.valueOf(overloadCode) + File.separator;
                String oldest = appLog;
                while ((new File(appLog).exists()) && (overloadCode <= MAX_OVERLOAD)) {
                    // Check oldest file (for overload if needed)
                    if (new File(oldest).lastModified() > new File(appLog).lastModified()) {
                        oldest = appLog;
                    }
                    // Next step
                    overloadCode = overloadCode + 1;
                    if (overloadCode < 10) {
                        appLog = this.compssLogBaseDirPath + serviceName + "_0" + String.valueOf(overloadCode)
                            + File.separator;
                    } else {
                        appLog = this.compssLogBaseDirPath + serviceName + "_" + String.valueOf(overloadCode)
                            + File.separator;
                    }
                }
                if (overloadCode > MAX_OVERLOAD) {
                    // Select the last modified folder
                    appLog = oldest;

                    // Overload
                    System.err.println(WARN_FOLDER_OVERLOAD);
                    System.err.println("Overwriting entry: " + appLog);

                    // Clean previous results to avoid collisions
                    if (!deleteDirectory(new File(appLog))) {
                        ErrorManager.error(ERROR_APP_OVERLOAD);
                    }
                }

                // We have the final appLogDirPath
                this.appLogDirPath = appLog;
                if (!new File(this.appLogDirPath).mkdir()) {
                    ErrorManager.error(ERROR_APP_LOG_DIR);
                }
            } else {
                /*
                 * REGULAR APPLICATION - Gets appName - Overloads the app folder for different executions - MAX_OVERLOAD
                 * raises warning - Changes working directory to appName !!!!
                 */
                int overloadCode = 1;
                String appLog =
                    this.compssLogBaseDirPath + appName + "_0" + String.valueOf(overloadCode) + File.separator;
                String oldest = appLog;
                while ((new File(appLog).exists()) && (overloadCode <= MAX_OVERLOAD)) {
                    // Check oldest file (for overload if needed)
                    if (new File(oldest).lastModified() > new File(appLog).lastModified()) {
                        oldest = appLog;
                    }
                    // Next step
                    overloadCode = overloadCode + 1;
                    if (overloadCode < 10) {
                        appLog =
                            this.compssLogBaseDirPath + appName + "_0" + String.valueOf(overloadCode) + File.separator;
                    } else {
                        appLog =
                            this.compssLogBaseDirPath + appName + "_" + String.valueOf(overloadCode) + File.separator;
                    }
                }
                if (overloadCode > MAX_OVERLOAD) {
                    // Select the last modified folder
                    appLog = oldest;

                    // Overload
                    System.err.println(WARN_FOLDER_OVERLOAD);
                    System.err.println("Overwriting entry: " + appLog);

                    // Clean previous results to avoid collisions
                    if (!deleteDirectory(new File(appLog))) {
                        ErrorManager.error(ERROR_APP_OVERLOAD);
                    }
                }

                // We have the final appLogDirPath
                this.appLogDirPath = appLog;
                if (!new File(this.appLogDirPath).mkdir()) {
                    ErrorManager.error(ERROR_APP_LOG_DIR);
                }
            }
        } else {
            // The option specific_log_dir has been given. NO sandbox created
            this.appLogDirPath = this.compssLogBaseDirPath;
        }

        // Set the environment property (for all cases) and reload logger
        // configuration
        System.setProperty(COMPSsConstants.APP_LOG_DIR, this.appLogDirPath);
        ((LoggerContext) LogManager.getContext(false)).reconfigure();

        /*
         * Create a tmp directory where to store: - Files whose first opened stream is an input one - Object files
         */
        this.tempDirPath = this.appLogDirPath + "tmpFiles" + File.separator;
        if (!new File(this.tempDirPath).mkdir()) {
            ErrorManager.error(ERROR_TEMP_DIR);
        }

        /*
         * Create a jobs dir where to store: - Jobs output files - Jobs error files
         */
        this.jobsDirPath = this.appLogDirPath + "jobs" + File.separator;
        if (!new File(this.jobsDirPath).mkdir()) {
            ErrorManager.error(ERROR_JOBS_DIR);
        }

        /*
         * Create a workers dir where to store: - Worker out files - Worker error files
         */
        this.workersDirPath = this.appLogDirPath + "workers" + File.separator;
        if (!new File(this.workersDirPath).mkdir()) {
            System.err.println(ERROR_WORKERS_DIR);
            System.exit(1);
        }

        // Configure worker debug level
        // Configure storage
        String storageConf = System.getProperty(COMPSsConstants.STORAGE_CONF);
        if (storageConf == null || storageConf.equals("") || storageConf.equals("null")) {
            storageConf = "null";
            LOGGER.warn("No storage configuration file passed");
        }
        this.storageConf = storageConf;

        String executionType = System.getProperty(COMPSsConstants.TASK_EXECUTION);
        if (executionType == null || executionType.equals("") || executionType.equals("null")) {
            executionType = COMPSsConstants.TaskExecution.COMPSS.toString();
            LOGGER.warn("No executionType passed");
        } else {
            executionType = executionType.toUpperCase();
        }
        this.executionType = TaskExecution.valueOf(executionType);

        this.out = new ThreadedPrintStream(SUFFIX_OUT, System.out);
        this.err = new ThreadedPrintStream(SUFFIX_ERR, System.err);
        System.setErr(this.err);
        System.setOut(this.out);

        // Get installDir classpath
        this.installDirPath = System.getenv(COMPSsConstants.COMPSS_HOME);

        // Get worker classpath
        String classPath = System.getProperty(COMPSsConstants.WORKER_CP);
        if (classPath == null || classPath.isEmpty()) {
            classPath = "";
        }

        // Get appDir classpath
        String appDir = System.getProperty(COMPSsConstants.WORKER_APPDIR);
        if (appDir == null || appDir.isEmpty()) {
            appDir = "";
        }
        this.appDirPath = appDir;

        // Get python interpreter
        String pythonInterpreter = System.getProperty(COMPSsConstants.PYTHON_INTERPRETER);
        if (pythonInterpreter == null || pythonInterpreter.isEmpty() || pythonInterpreter.equals("null")) {
            pythonInterpreter = COMPSsConstants.DEFAULT_PYTHON_INTERPRETER;
        }

        // Get python version
        String pythonVersion = System.getProperty(COMPSsConstants.PYTHON_VERSION);
        if (pythonVersion == null || pythonVersion.isEmpty() || pythonVersion.equals("null")) {
            pythonVersion = COMPSsConstants.DEFAULT_PYTHON_VERSION;
        }

        // Configure python virtual environment
        String pythonVEnv = System.getProperty(COMPSsConstants.PYTHON_VIRTUAL_ENVIRONMENT);
        if (pythonVEnv == null || pythonVEnv.isEmpty() || pythonVEnv.equals("null")) {
            pythonVEnv = COMPSsConstants.DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT;
        }
        String pythonPropagateVEnv = System.getProperty(COMPSsConstants.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT);
        if (pythonPropagateVEnv == null || pythonPropagateVEnv.isEmpty() || pythonPropagateVEnv.equals("null")) {
            pythonPropagateVEnv = COMPSsConstants.DEFAULT_PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT;
        }

        String pythonPath = System.getProperty(COMPSsConstants.WORKER_PP);
        if (pythonPath == null || pythonPath.isEmpty()) {
            pythonPath = "";
        }

        // Get Python MPI worker invocation
        String pythonMpiWorker = System.getProperty(COMPSsConstants.PYTHON_MPI_WORKER);
        if (pythonMpiWorker == null || pythonMpiWorker.isEmpty() || pythonMpiWorker.equals("null")) {
            pythonMpiWorker = COMPSsConstants.DEFAULT_PYTHON_MPI_WORKER;
        }
        JavaParams javaParams = new JavaParams(classPath);
        PythonParams pyParams = new PythonParams(pythonInterpreter, pythonVersion, pythonVEnv, pythonPropagateVEnv,
            pythonPath, pythonMpiWorker);
        CParams cParams = new CParams(classPath);

        this.langParams[Lang.JAVA.ordinal()] = javaParams;
        this.langParams[Lang.PYTHON.ordinal()] = pyParams;
        this.langParams[Lang.C.ordinal()] = cParams;

        String workerPersistentC = System.getProperty(COMPSsConstants.WORKER_PERSISTENT_C);
        if (workerPersistentC == null || workerPersistentC.isEmpty() || workerPersistentC.equals("null")) {
            workerPersistentC = COMPSsConstants.DEFAULT_PERSISTENT_C;
        }
        this.persistentEnabled = workerPersistentC.toUpperCase().compareTo("TRUE") == 0;

        this.executionManager = new ExecutionManager(this, 0, ThreadBinder.BINDER_DISABLED, 0,
            ThreadBinder.BINDER_DISABLED, 0, ThreadBinder.BINDER_DISABLED, 0);
        try {
            this.executionManager.init();
        } catch (InitializationException ie) {
            ErrorManager.error(EXECUTION_MANAGER_ERR, ie);
        }
    }

    private boolean deleteDirectory(File directory) {
        if (!directory.exists()) {
            return false;
        }

        File[] files = directory.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    if (!f.delete()) {
                        return false;
                    }
                }
            }
        }

        return directory.delete();
    }

    @Override
    public void start() {
        synchronized (this) {
            if (this.started) {
                return;
            }
            this.started = true;
        }
    }

    @Override
    public String getName() {
        return MASTER_NAME;
    }

    @Override
    public void setInternalURI(MultiURI u) {
        for (CommAdaptor adaptor : Comm.getAdaptors().values()) {
            adaptor.completeMasterURI(u);
        }
    }

    @Override
    public void stop(ShutdownListener sl) {
        // ExecutionManager was already shutdown
        sl.notifyEnd();
    }

    @Override
    public void sendData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {

        for (Resource targetRes : target.getHosts()) {
            COMPSsNode node = targetRes.getNode();
            if (node != this) {
                try {
                    node.obtainData(ld, source, target, tgtData, reason, listener);
                } catch (Exception e) {
                    // Can not copy the file.
                    // Cannot receive the file, try with the following
                    continue;
                }
                return;
            }

        }
    }

    /**
     * Retrieves a binding data.
     *
     * @param ld Source LogicalData.
     * @param source Preferred source location.
     * @param target Preferred target location.
     * @param tgtData Target LogicalData.
     * @param reason Transfer reason.
     * @param listener Transfer listener.
     */
    public void obtainBindingData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {

        BindingObject tgtBO = ((BindingObjectLocation) target).getBindingObject();
        Collection<Copy> copiesInProgress = ld.getCopiesInProgress();
        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            for (Copy copy : copiesInProgress) {
                if (copy != null) {
                    if (copy.getTargetLoc() != null && copy.getTargetLoc().getHosts().contains(Comm.getAppHost())) {
                        if (DEBUG) {
                            LOGGER.debug(
                                "Copy in progress tranfering " + ld.getName() + "to master. Waiting for finishing");
                        }
                        copy.addEventListener(new EventListener() {

                            @Override
                            public void notifyEnd(DataOperation fOp) {
                                if (DEBUG) {
                                    LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget()
                                        + " to " + tgtBO.getName());
                                }
                                try {
                                    if (COMPSsMaster.this.persistentEnabled) {
                                        manageObtainBindingObjectInCache(copy.getFinalTarget(), tgtBO, tgtData, target,
                                            reason);
                                    } else {
                                        manageObtainBindingObjectAsFile(copy.getFinalTarget(), tgtBO, tgtData, target,
                                            reason);
                                    }
                                    listener.notifyEnd(null);
                                } catch (Exception e) {
                                    LOGGER.error("ERROR: managing obtain binding object at cache", e);
                                    listener.notifyFailure(null, e);
                                }
                            }

                            @Override
                            public void notifyFailure(DataOperation fOp, Exception e) {
                                if (DEBUG) {
                                    LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget()
                                        + " to " + tgtBO.getName());
                                }
                                LOGGER.error("ERROR: managing obtain binding object at cache", e);
                                listener.notifyFailure(null, e);
                            }
                        });
                        return;
                    } else {
                        if (DEBUG) {
                            LOGGER.debug("Current copies are not transfering " + ld.getName()
                                + " to master. Ignoring at this moment");
                        }
                    }

                }
            }
        }

        // Checking if file is already in master
        if (DEBUG) {
            LOGGER.debug("Checking if " + ld.getName() + " is at master (" + Comm.getAppHost().getName() + ").");
        }

        for (MultiURI u : ld.getURIs()) {
            if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug(ld.getName() + " is at " + u.toString() + "(" + hostname + ")");
            }
            if (u.getHost() == Comm.getAppHost()) {
                if (DEBUG) {
                    LOGGER.debug("Master local copy " + ld.getName() + " from " + u.getHost().getName() + " to "
                        + tgtBO.getName());
                }
                try {
                    if (this.persistentEnabled) {
                        manageObtainBindingObjectInCache(u.getPath(), tgtBO, tgtData, target, reason);
                    } else {
                        manageObtainBindingObjectAsFile(u.getPath(), tgtBO, tgtData, target, reason);
                    }
                    listener.notifyEnd(null);
                } catch (Exception e) {
                    LOGGER.error("ERROR: managing obtain binding object at cache", e);
                    listener.notifyFailure(null, e);
                }
                return;
            } else {
                if (DEBUG) {
                    String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                    LOGGER.debug("Data " + ld.getName() + " copy in " + hostname + " not evaluated now");
                }
            }

        }

        // Ask the transfer from an specific source
        if (source != null) {
            for (Resource sourceRes : source.getHosts()) {
                COMPSsNode node = sourceRes.getNode();
                String sourcePath = source.getURIInHost(sourceRes).getPath();
                if (node != this) {
                    try {
                        if (DEBUG) {
                            LOGGER.debug("Sending data " + ld.getName() + " from (" + node.getName() + ") " + sourcePath
                                + " to (master) " + tgtBO.getName());
                        }
                        node.sendData(ld, source, target, tgtData, reason, listener);
                    } catch (Exception e) {
                        ErrorManager.warn("Not possible to sending data master to " + tgtBO.getName(), e);
                        continue;
                    }
                    LOGGER.debug("Data " + ld.getName() + " sent.");
                    return;
                } else {
                    try {
                        if (this.persistentEnabled) {
                            manageObtainBindingObjectInCache(sourcePath, tgtBO, tgtData, target, reason);
                        } else {
                            manageObtainBindingObjectAsFile(sourcePath, tgtBO, tgtData, target, reason);
                        }
                        listener.notifyEnd(null);
                    } catch (Exception e) {
                        LOGGER.error("ERROR: managing obtain binding object at cache", e);
                        listener.notifyFailure(null, e);
                    }
                    return;
                }
            }
        } else {
            LOGGER.debug("Source data location is null. Trying other alternatives");
        }

        // Preferred source is null or copy has failed. Trying to retrieve data from any host
        for (Resource sourceRes : ld.getAllHosts()) {
            COMPSsNode node = sourceRes.getNode();
            if (node != this) {
                try {
                    LOGGER.debug("Sending data " + ld.getName() + " from (" + node.getName() + ") "
                        + sourceRes.getName() + " to (master)" + tgtBO.getName());
                    node.sendData(ld, source, target, tgtData, reason, listener);
                } catch (Exception e) {
                    LOGGER.error("Error: exception sending data", e);
                    continue;
                }
                LOGGER.debug("Data " + ld.getName() + " sent.");
                return;
            } else {
                if (DEBUG) {
                    LOGGER.debug("Data " + ld.getName() + " copy in " + sourceRes.getName()
                        + " not evaluated now. Should have been evaluated before");
                }
            }
        }
        LOGGER.warn("WARN: All posibilities checked for obtaining data " + ld.getName()
            + " and nothing done. Releasing listeners and locks");
        listener.notifyEnd(null);
    }

    private void manageObtainBindingObjectInCache(String initialPath, BindingObject tgtBO, LogicalData tgtData,
        DataLocation target, Transferable reason) throws Exception {

        BindingObject bo = BindingObject.generate(initialPath);

        if (bo.getName().equals(tgtBO.getName())) {
            if (BindingDataManager.isInBinding(tgtBO.getName())) {
                LOGGER.debug(
                    "Current transfer is the same as expected. Nothing to do setting data target to " + initialPath);
                reason.setDataTarget(initialPath);
            } else {
                String tgtPath = getCompletePath(DataType.BINDING_OBJECT_T, tgtBO.getName()).getPath();
                LOGGER.debug("Data " + tgtBO.getName() + " not in cache loading from file " + tgtPath);
                if (BindingDataManager.loadFromFile(tgtBO.getName(), tgtPath, tgtBO.getType(),
                    tgtBO.getElements()) != 0) {
                    throw (new Exception("Error loading object " + tgtBO.getName() + " from " + tgtPath));
                }
                reason.setDataTarget(target.getPath());
            }
        } else {
            if (BindingDataManager.isInBinding(tgtBO.getName())) {
                LOGGER.debug("Making cache copy from " + bo.getName() + " to " + tgtBO.getName());
                if (reason.isSourcePreserved()) {
                    if (BindingDataManager.copyCachedData(bo.getName(), tgtBO.getName()) != 0) {
                        throw (new Exception("Error copying cache from " + bo.getName() + " to " + tgtBO.getName()));
                    }
                } else {
                    if (BindingDataManager.moveCachedData(bo.getName(), tgtBO.getName()) != 0) {
                        throw (new Exception("Error moved cache from " + bo.getName() + " to " + tgtBO.getName()));
                    }
                }
            } else {
                String tgtPath = getCompletePath(DataType.BINDING_OBJECT_T, tgtBO.getName()).getPath();
                LOGGER.debug("Data " + tgtBO.getName() + " not in cache loading from file " + tgtPath);
                if (BindingDataManager.loadFromFile(tgtBO.getName(), tgtPath, tgtBO.getType(),
                    tgtBO.getElements()) != 0) {
                    throw (new Exception("Error loading object " + tgtBO.getName() + " from " + tgtPath));
                }
            }
            if (tgtData != null) {
                tgtData.addLocation(target);
            }
            LOGGER.debug("BindingObject copied/moved set data target as " + target.getPath());
            reason.setDataTarget(target.getPath());
        }

    }

    private void manageObtainBindingObjectAsFile(String initialPath, BindingObject tgtBO, LogicalData tgtData,
        DataLocation target, Transferable reason) throws Exception {

        BindingObject bo = BindingObject.generate(initialPath);
        if (bo.getName().equals(tgtBO.getName())) {
            LOGGER
                .debug("Current transfer is the same as expected. Nothing to do setting data target to " + initialPath);
            reason.setDataTarget(initialPath);
        } else {
            if (bo.getId().startsWith(File.separator)) {
                String iPath = getCompletePath(DataType.BINDING_OBJECT_T, bo.getName()).getPath();
                String tPath = getCompletePath(DataType.BINDING_OBJECT_T, tgtBO.getName()).getPath();
                if (reason.isSourcePreserved()) {
                    if (DEBUG) {
                        LOGGER.debug("Master local copy of data" + bo.getName() + " from " + iPath + " to " + tPath);
                    }
                    Files.copy(new File(iPath).toPath(), new File(tPath).toPath(), StandardCopyOption.REPLACE_EXISTING);

                } else {
                    if (DEBUG) {
                        LOGGER.debug("Master local move of data " + bo.getName() + " from " + iPath + " to " + tPath);
                    }
                    Files.move(new File(iPath).toPath(), new File(tPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            } else {
                if (BindingDataManager.isInBinding(bo.getName())) {
                    String tPath = getCompletePath(DataType.BINDING_OBJECT_T, tgtBO.getName()).getPath();
                    LOGGER.debug("Storing object data " + bo.getName() + " from cache to " + tPath);
                    BindingDataManager.storeInFile(bo.getName(), tPath);
                } else {
                    throw new Exception("Data " + bo.getName() + "not a filepath and its not in cache");
                }
            }

            if (tgtData != null) {
                tgtData.addLocation(target);
            }

            LOGGER.debug("BindingObject as file copied/moved set data target as " + target.getPath());
            reason.setDataTarget(target.getPath());
        }
        // If path is relative push to cache. If not keep as file (master_in_worker)
        LOGGER.debug(" Checking if BindingObject " + tgtBO.getId() + " has relative path");
        if (!tgtBO.getId().startsWith(File.separator)) {
            LOGGER.debug("Loading BindingObject " + tgtBO.getName() + " to cache...");
            String tgtPath = getCompletePath(DataType.BINDING_OBJECT_T, tgtBO.getName()).getPath();
            if (BindingDataManager.loadFromFile(tgtBO.getName(), tgtPath, tgtBO.getType(), tgtBO.getElements()) != 0) {
                throw (new Exception("Error loading object " + tgtBO.getName() + " from " + tgtPath));
            }
        }
    }

    /**
     * Retrieves a file data.
     *
     * @param ld Source LogicalData.
     * @param source Preferred source location.
     * @param target Preferred target location.
     * @param tgtData Target LogicalData.
     * @param reason Transfer reason.
     * @param listener Transfer listener.
     */
    public void obtainFileData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {

        String targetPath = target.getURIInHost(Comm.getAppHost()).getPath();

        // Check if file is already on the Path
        List<MultiURI> uris = ld.getURIs();
        for (MultiURI u : uris) {
            if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug(ld.getName() + " is at " + u.toString() + "(" + hostname + ")");
            }
            if (u.getHost().getNode() == this) {
                if (targetPath.compareTo(u.getPath()) == 0) {
                    LOGGER.debug(ld.getName() + " is already at " + targetPath);
                    // File already in the Path
                    reason.setDataTarget(targetPath);
                    listener.notifyEnd(null);
                    return;
                }
            }
        }

        // Check if there are current copies in progress bringing it into the node.
        if (DEBUG) {
            LOGGER.debug(
                "Data " + ld.getName() + " not in memory. Checking if there is a copy to the master in progress");
        }

        Collection<Copy> copiesInProgress = ld.getCopiesInProgress();
        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            for (Copy copy : copiesInProgress) {
                if (copy != null) {
                    if (copy.getTargetLoc() != null && copy.getTargetLoc().getHosts().contains(Comm.getAppHost())) {
                        if (DEBUG) {
                            LOGGER.debug(
                                "Copy in progress tranfering " + ld.getName() + "to master. Waiting for finishing");
                        }
                        EventListener el = new EventListener() {

                            @Override
                            public void notifyEnd(DataOperation fOp) {
                                if (DEBUG) {
                                    LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget()
                                        + " to " + targetPath);
                                }
                                try {
                                    Path tgtPath = (new File(copy.getFinalTarget())).toPath();
                                    Path copyPath = (new File(copy.getFinalTarget())).toPath();
                                    if (tgtPath.compareTo(copyPath) != 0) {
                                        Files.copy(copyPath, tgtPath, StandardCopyOption.REPLACE_EXISTING);
                                    }
                                    if (tgtData != null) {
                                        tgtData.addLocation(target);
                                    }
                                    LOGGER.debug("File copied set dataTarget " + targetPath);
                                    reason.setDataTarget(targetPath);

                                    listener.notifyEnd(null);
                                } catch (IOException ex) {
                                    ErrorManager.warn("Error master local copying file " + copy.getFinalTarget()
                                        + " from master to " + targetPath + " with replacing", ex);
                                    listener.notifyFailure(null, ex);
                                }

                            }

                            @Override
                            public void notifyFailure(DataOperation fOp, Exception e) {
                                if (DEBUG) {
                                    LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget()
                                        + " to " + targetPath);
                                }
                                ErrorManager.warn("Error master local copying file " + copy.getFinalTarget()
                                    + " from master to " + targetPath + " with replacing", e);
                                listener.notifyFailure(null, e);
                            }
                        };
                        copy.addEventListener(el);
                        return;
                    }
                }
            }
        }

        // Checking if file is already in master
        if (DEBUG) {
            LOGGER.debug("Checking if " + ld.getName() + " is at master (" + Comm.getAppHost().getName() + ").");
        }

        for (MultiURI u : uris) {
            if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug(ld.getName() + " is at " + u.toString() + "(" + hostname + ")");
            }
            if (u.getHost().getNode() == this) {
                try {
                    if (DEBUG) {
                        LOGGER.debug("Data " + ld.getName() + " is already accessible at " + u.getPath());
                    }
                    if (reason.isSourcePreserved()) {
                        if (DEBUG) {
                            LOGGER.debug("Master local copy " + ld.getName() + " from " + u.getHost().getName() + " to "
                                + targetPath);
                        }
                        Files.copy((new File(u.getPath())).toPath(), new File(targetPath).toPath(),
                            StandardCopyOption.REPLACE_EXISTING);

                    } else {
                        if (DEBUG) {
                            LOGGER.debug("Master local copy " + ld.getName() + " from " + u.getHost().getName() + " to "
                                + targetPath);
                        }
                        Files.move((new File(u.getPath())).toPath(), new File(targetPath).toPath(),
                            StandardCopyOption.REPLACE_EXISTING);
                        uris.remove(u);
                    }

                    if (tgtData != null) {
                        tgtData.addLocation(target);
                    }
                    LOGGER.debug("File on path. Set data target to " + targetPath);
                    reason.setDataTarget(targetPath);

                    listener.notifyEnd(null);
                    return;
                } catch (IOException ex) {
                    ErrorManager.warn(
                        "Error master local copy file from " + u.getPath() + " to " + targetPath + " with replacing",
                        ex);
                }
            } else {
                if (DEBUG) {
                    String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                    LOGGER.debug("Data " + ld.getName() + " copy in " + hostname + " not evaluated now");
                }
            }
        }

        // Ask the transfer from an specific source
        if (source != null) {
            for (Resource sourceRes : source.getHosts()) {
                COMPSsNode node = sourceRes.getNode();
                String sourcePath = source.getURIInHost(sourceRes).getPath();
                if (node != this) {
                    try {
                        if (DEBUG) {
                            LOGGER.debug("Sending data " + ld.getName() + " from " + sourcePath + " to " + targetPath);
                        }
                        node.sendData(ld, source, target, tgtData, reason, listener);
                    } catch (Exception e) {
                        ErrorManager.warn("Not possible to sending data master to " + targetPath, e);
                        continue;
                    }
                    LOGGER.debug("Data " + ld.getName() + " sent.");
                    return;
                } else {
                    try {
                        if (DEBUG) {
                            LOGGER.debug("Local copy " + ld.getName() + " from " + sourcePath + " to " + targetPath);
                        }
                        Files.copy(new File(sourcePath).toPath(), new File(targetPath).toPath(),
                            StandardCopyOption.REPLACE_EXISTING);

                        LOGGER.debug("File copied. Set data target to " + targetPath);
                        reason.setDataTarget(targetPath);
                        listener.notifyEnd(null);
                        return;
                    } catch (IOException ex) {
                        ErrorManager.warn("Error master local copy file from " + sourcePath + " to " + targetPath, ex);
                    }
                }
            }
        } else {
            LOGGER.debug("Source data location is null. Trying other alternatives");
        }

        // Preferred source is null or copy has failed. Trying to retrieve data from any host
        for (Resource sourceRes : ld.getAllHosts()) {
            COMPSsNode node = sourceRes.getNode();
            if (node != this) {
                try {
                    LOGGER.debug("Sending data " + ld.getName() + " from " + sourceRes.getName() + " to " + targetPath);
                    node.sendData(ld, source, target, tgtData, reason, listener);
                } catch (Exception e) {
                    LOGGER.error("Error: exception sending data", e);
                    continue;
                }
                LOGGER.debug("Data " + ld.getName() + " sent.");
                return;
            } else {
                if (DEBUG) {
                    LOGGER.debug("Data " + ld.getName() + " copy in " + sourceRes.getName()
                        + " not evaluated now. Should have been evaluated before");
                }
            }
        }

        // If we have not exited before, any copy method was successful. Raise warning
        ErrorManager.warn("Error file " + ld.getName() + " not transferred to " + targetPath);
        listener.notifyEnd(null);
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {
        synchronized (ld) {
            LOGGER.info("Obtain Data " + ld.getName());
            if (DEBUG) {
                if (ld != null) {
                    LOGGER.debug("srcData: " + ld.toString());
                }
                if (reason != null) {
                    LOGGER.debug("Reason: " + reason.getType());
                }
                if (source != null) {
                    LOGGER.debug("Source Data location: " + source.getType().toString() + " "
                        + source.getProtocol().toString() + " " + source.getURIs().get(0));
                }
                if (target != null) {
                    if (target.getProtocol() != ProtocolType.PERSISTENT_URI) {
                        LOGGER.debug("Target Data location: " + target.getType().toString() + " "
                            + target.getProtocol().toString() + " " + target.getURIs().get(0));
                    } else {
                        LOGGER.debug("Target Data location: " + target.getType().toString() + " "
                            + target.getProtocol().toString());
                    }
                }
                if (tgtData != null) {
                    LOGGER.debug("tgtData: " + tgtData.toString());
                }
            }
            if (reason != null && reason.getType().equals(DataType.COLLECTION_T)) {

                String targetPath;
                if (target != null) {
                    targetPath = target.getURIInHost(Comm.getAppHost()).getPath();

                } else {
                    if (tgtData != null) {
                        targetPath = tgtData.getName();
                    } else {
                        targetPath = ld.getName();
                        LOGGER.warn(
                            "No target location neither target data available. Setting targetPath to " + ld.getName());
                    }
                }
                LOGGER
                    .debug("Data " + ld.getName() + "is COLLECTION_T nothing to tranfer. Elements already transferred."
                        + "Setting target path to " + targetPath);
                reason.setDataTarget(targetPath);
                listener.notifyEnd(null);
                return;
            }
            /*
             * Check if data is binding data
             */
            if (ld.isBindingData() || (reason != null && reason.getType().equals(DataType.BINDING_OBJECT_T))
                || (source != null && source.getType().equals(LocationType.BINDING))
                || (target != null && target.getType().equals(LocationType.BINDING))) {
                obtainBindingData(ld, source, target, tgtData, reason, listener);
                return;
            }
            /*
             * PSCO transfers are always available, if any SourceLocation is PSCO, don't transfer
             */

            for (DataLocation loc : ld.getLocations()) {
                if (loc.getProtocol().equals(ProtocolType.PERSISTENT_URI)) {
                    LOGGER.debug("Object in Persistent Storage. Set dataTarget to " + loc.getPath());
                    reason.setDataTarget(loc.getPath());
                    listener.notifyEnd(null);
                    return;
                }
            }

            /*
             * Otherwise the data is a file or an object that can be already in the master memory, in the master disk or
             * being transfered
             */
            // Check if data is in memory (no need to check if it is PSCO since previous case avoids it)
            if (ld.isInMemory()) {
                String targetPath = target.getURIInHost(Comm.getAppHost()).getPath();
                // Serialize value to file
                try {
                    Serializer.serialize(ld.getValue(), targetPath);
                } catch (IOException ex) {
                    ErrorManager.warn("Error copying file from memory to " + targetPath, ex);
                }

                if (tgtData != null) {
                    tgtData.addLocation(target);
                }
                LOGGER.debug("Object in memory. Set dataTarget to " + targetPath);
                reason.setDataTarget(targetPath);
                listener.notifyEnd(null);
                return;
            }

            obtainFileData(ld, source, target, tgtData, reason, listener);
        }
    }

    @Override
    public void enforceDataObtaining(Transferable reason, EventListener listener) {
        // Copy already done on obtainData()
        listener.notifyEnd(null);
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener) {

        return new LocalJob(taskId, taskParams, impl, res, slaveWorkersNodeNames, listener);
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        String path = null;
        switch (type) {
            case FILE_T:
                path = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + name;
                break;
            case OBJECT_T:
                path = ProtocolType.OBJECT_URI.getSchema() + name;
                break;
            case COLLECTION_T:
                path = ProtocolType.OBJECT_URI.getSchema() + Comm.getAppHost().getTempDirPath() + name;
                break;
            case STREAM_T:
                path = ProtocolType.STREAM_URI.getSchema() + name;
                break;
            case EXTERNAL_STREAM_T:
                path = ProtocolType.EXTERNAL_STREAM_URI.getSchema() + Comm.getAppHost().getTempDirPath() + name;
                break;
            case PSCO_T:
                path = ProtocolType.PERSISTENT_URI.getSchema() + name;
                break;
            case EXTERNAL_PSCO_T:
                path = ProtocolType.PERSISTENT_URI.getSchema() + name;
                break;
            case BINDING_OBJECT_T:
                path = ProtocolType.BINDING_URI.getSchema() + Comm.getAppHost().getTempDirPath() + name;
                break;
            default:
                return null;
        }

        // Switch path to URI
        return new SimpleURI(path);
    }

    @Override
    public void deleteTemporary() {
        File dir = new File(Comm.getAppHost().getTempDirPath());
        for (File f : dir.listFiles()) {
            deleteFolder(f);
        }
    }

    private void deleteFolder(File folder) {
        if (folder.isDirectory()) {
            for (File f : folder.listFiles()) {
                deleteFolder(f);
            }
        }
        if (!folder.delete()) {
            LOGGER.error("Error deleting file " + (folder == null ? "" : folder.getName()));
        }
    }

    @Override
    public boolean generatePackage() {
        // Should not be executed on a COMPSsMaster
        return false;
    }

    @Override
    public boolean generateWorkersDebugInfo() {
        // Should not be executed on a COMPSsMaster
        return false;
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
        // Should not be executed on a COMPSsMaster
        this.executionManager.stop();
        sl.notifyEnd();
    }

    @Override
    public String getUser() {
        return "";
    }

    @Override
    public String getClasspath() {
        return "";
    }

    @Override
    public String getPythonpath() {
        return "";
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void announceDestruction() throws AnnounceException {
        // No need to do it. The master no it's always up
    }

    @Override
    public void announceCreation() throws AnnounceException {
        // No need to do it. The master no it's always up
    }

    /**
     * Starts the execution of a local job.
     *
     * @param job Local job to run.
     */
    public void runJob(LocalJob job) {
        Execution exec = new Execution(job, new ExecutionListener() {

            @Override
            public void notifyEnd(Invocation invocation, boolean success, COMPSsException e) {
                for (LocalParameter p : job.getParams()) {
                    updateParameter(p);
                }
                LocalParameter targetParam = job.getTarget();
                if (targetParam != null) {
                    updateParameter(targetParam);
                }
                for (LocalParameter p : job.getResults()) {
                    updateParameter(p);
                }

                if (success) {
                    job.getListener().jobCompleted(job);
                } else {
                    if (e != null) {
                        job.getListener().jobFailed(job, JobEndStatus.EXCEPTION, e);
                    } else {
                        job.getListener().jobFailed(job, JobEndStatus.EXECUTION_FAILED, e);
                    }
                }
            }
        });
        this.executionManager.enqueue(exec);
    }

    private void updateParameter(LocalParameter lp) {
        DataType newType = lp.getType();
        String pscoId;
        switch (newType) {
            case PSCO_T:
                pscoId = ((StubItf) lp.getValue()).getID();
                break;
            case EXTERNAL_PSCO_T:
                pscoId = (String) lp.getValue();
                break;
            default:
                pscoId = null;
        }

        if (pscoId != null) {
            DataType previousType = lp.getOriginalType();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Updating parameter " + lp.getDataMgmtId() + " from type " + previousType + " to type "
                    + newType + " with id " + pscoId);
            }

            switch (previousType) {
                case PSCO_T:
                case EXTERNAL_PSCO_T:
                    if (previousType.equals(newType)) {
                        // The parameter was already a PSCO, we only update the information just in case
                        DependencyParameter dp = (DependencyParameter) lp.getParam();
                        dp.setDataTarget(pscoId);
                    } else {
                        // The parameter types do not match, log exception
                        LOGGER.warn("WARN: Cannot update parameter " + lp.getDataMgmtId()
                            + " because types are not compatible");
                    }
                    break;
                default:
                    // The parameter was an OBJECT or a FILE, we change its type and value and register its new location
                    registerUpdatedParameter(newType, pscoId, lp);
                    break;
            }
        }
    }

    private void registerUpdatedParameter(DataType newType, String pscoId, LocalParameter lp) {
        // The parameter was an OBJECT or a FILE, we change its type and value and register its new location
        String renaming = lp.getDataMgmtId();
        // Update COMM information
        switch (newType) {
            case PSCO_T:
                Comm.registerPSCO(renaming, pscoId);
                break;
            case EXTERNAL_PSCO_T:
                if (renaming.contains("/")) {
                    renaming = renaming.substring(renaming.lastIndexOf('/') + 1);
                }
                Comm.registerExternalPSCO(renaming, pscoId);
                break;
            default:
                LOGGER.warn("WARN: Invalid new type " + newType + " for parameter " + renaming);
                break;
        }

        // Update Task information
        DependencyParameter dp = (DependencyParameter) lp.getParam();
        dp.setType(newType);
        dp.setDataTarget(pscoId);
    }

    @Override
    public String getHostName() {
        return MASTER_NAME;
    }

    @Override
    public long getTracingHostID() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getAppDir() {
        return this.appDirPath;
    }

    @Override
    public String getInstallDir() {
        return this.installDirPath;
    }

    @Override
    public String getWorkingDir() {
        return this.tempDirPath;
    }

    @Override
    public COMPSsConstants.TaskExecution getExecutionType() {
        return this.executionType;
    }

    @Override
    public boolean isPersistentCEnabled() {
        return this.persistentEnabled;
    }

    @Override
    public LanguageParams getLanguageParams(COMPSsConstants.Lang language) {
        return this.langParams[language.ordinal()];
    }

    @Override
    public void registerOutputs(String path) {
        this.err.registerThread(path);
        this.out.registerThread(path);
    }

    @Override
    public void unregisterOutputs() {
        this.err.unregisterThread();
        this.out.unregisterThread();
    }

    @Override
    public String getStandardStreamsPath(Invocation invocation) {
        // Set outputs paths (Java will register them, ExternalExec will redirect processes outputs)
        return Comm.getAppHost().getJobsDirPath() + File.separator + "job" + invocation.getJobId() + "_"
            + invocation.getHistory();
    }

    @Override
    public PrintStream getThreadOutStream() {
        return this.out.getStream();
    }

    @Override
    public PrintStream getThreadErrStream() {
        return this.err.getStream();
    }

    @Override
    public String getStorageConf() {
        return this.storageConf;
    }

    @Override
    public StreamBackend getStreamingBackend() {
        return Comm.getStreamingBackend();
    }

    @Override
    public String getStreamingMasterName() {
        return MASTER_NAME;
    }

    @Override
    public int getStreamingMasterPort() {
        return Comm.getStreamingPort();
    }

    @Override
    public void loadParam(InvocationParam invParam) throws UnloadableValueException {
        LocalParameter localParam = (LocalParameter) invParam;

        switch (localParam.getType()) {
            case FILE_T:
                // No need to load anything. Value already on a file
                break;
            case OBJECT_T:
            case STREAM_T:
                DependencyParameter dpar = (DependencyParameter) localParam.getParam();
                String dataId = (String) localParam.getValue();
                LogicalData ld = Comm.getData(dataId);
                if (ld.isInMemory()) {
                    invParam.setValue(ld.getValue());
                } else {
                    try {
                        Object o = Serializer.deserialize(dpar.getDataTarget());
                        invParam.setValue(o);
                    } catch (ClassNotFoundException | IOException e) {
                        throw new UnloadableValueException(e);
                    }
                }
                break;
            case PSCO_T:
                String pscoId = (String) localParam.getValue();
                try {
                    Object o = StorageItf.getByID(pscoId);
                    invParam.setValue(o);
                } catch (StorageException se) {
                    throw new UnloadableValueException(se);
                }
                break;
            default:
                // Already contains the proper value on the param
                break;
        }
    }

    @Override
    public void storeParam(InvocationParam invParam) {
        LocalParameter localParam = (LocalParameter) invParam;
        Parameter param = localParam.getParam();
        switch (param.getType()) {
            case COLLECTION_T:
            case FILE_T:
            case EXTERNAL_STREAM_T:
                // No need to store anything. Already stored on disk
                break;
            case OBJECT_T:
            case STREAM_T:
            case PSCO_T:
                String resultName = localParam.getDataMgmtId();
                LogicalData ld = Comm.getData(resultName);
                ld.setValue(invParam.getValue());
                break;
            case BINDING_OBJECT_T:
                // No need to store anything. Already stored on the binding
                break;
            default:
                throw new UnsupportedOperationException("Not supported yet." + param.getType());
        }
    }

    public String getCOMPSsLogBaseDirPath() {
        return this.compssLogBaseDirPath;
    }

    public String getWorkingDirectory() {
        return this.tempDirPath;
    }

    public String getUserExecutionDirPath() {
        return this.userExecutionDirPath;
    }

    public String getAppLogDirPath() {
        return this.appLogDirPath;
    }

    public String getTempDirPath() {
        return this.tempDirPath;
    }

    public String getJobsDirPath() {
        return this.jobsDirPath;
    }

    public String getWorkersDirPath() {
        return this.workersDirPath;
    }

    @Override
    public void increaseComputingCapabilities(ResourceDescription descr) {
        final MethodResourceDescription description = (MethodResourceDescription) descr;
        final int cpuCount = description.getTotalCPUComputingUnits();
        final int gpuCount = description.getTotalGPUComputingUnits();
        final int fpgaCount = description.getTotalFPGAComputingUnits();
        final int otherCount = description.getTotalOTHERComputingUnits();
        this.executionManager.increaseCapabilities(cpuCount, gpuCount, fpgaCount, otherCount);
    }

    @Override
    public void reduceComputingCapabilities(ResourceDescription descr) {
        final MethodResourceDescription description = (MethodResourceDescription) descr;
        final int cpuCount = description.getTotalCPUComputingUnits();
        final int gpuCount = description.getTotalGPUComputingUnits();
        final int fpgaCount = description.getTotalFPGAComputingUnits();
        final int otherCount = description.getTotalOTHERComputingUnits();
        this.executionManager.reduceCapabilities(cpuCount, gpuCount, fpgaCount, otherCount);
    }

    @Override
    public void removeObsoletes(List<MultiURI> obsoletes) {
        // Nothing to do

    }
}
