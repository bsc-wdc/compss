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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.COMPSsConstants.TaskExecution;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.data.BindingDataManager;
import es.bsc.compss.exceptions.AnnounceException;
import es.bsc.compss.exceptions.CannotLoadException;
import es.bsc.compss.invokers.types.CParams;
import es.bsc.compss.invokers.types.JavaParams;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.invokers.types.RParams;
import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.local.LocalJob;
import es.bsc.compss.local.LocalParameter;
import es.bsc.compss.log.LoggerManager;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.LocationType;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.operation.copy.CompletedCopyException;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationExecutionRequest;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.types.execution.exceptions.NonExistentDataException;
import es.bsc.compss.types.execution.exceptions.UnloadableValueException;
import es.bsc.compss.types.execution.exceptions.UnwritableValueException;
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
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.FileOpsManager;
import es.bsc.compss.util.FileOpsManager.FileOpListener;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.utils.execution.ExecutionManager;
import es.bsc.compss.utils.execution.ThreadedPrintStream;
import es.bsc.compss.worker.COMPSsException;
import es.bsc.distrostreamlib.server.types.StreamBackend;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


/**
 * Representation of the COMPSs Master Node Only 1 instance per execution.
 */
public final class COMPSsMaster extends COMPSsWorker implements InvocationContext {

    private static final String ERROR_TEMP_DIR = "ERROR: Cannot create temp directory";

    private static final String EXECUTION_MANAGER_ERR = "Error starting ExecutionManager";

    public static final String SUFFIX_OUT = ".out";
    public static final String SUFFIX_ERR = ".err";

    private COMPSsRuntime runtimeApi;
    private LoaderAPI loaderApi;

    private final String storageConf;
    private final TaskExecution executionType;

    private final String installDirPath;
    private final String appDirPath;
    private final String tempDirPath;

    private final LanguageParams[] langParams = new LanguageParams[COMPSsConstants.Lang.values().length];
    private boolean persistentEnabled;

    private ExecutionManager executionManager;
    private final ThreadedPrintStream out;
    private final ThreadedPrintStream err;
    private boolean started = false;

    private boolean ear = false;
    private boolean dataProvenance = false;


    /**
     * New COMPSs Master.
     *
     * @param monitor element monitoring changes on the node.
     */
    public COMPSsMaster(NodeMonitor monitor) {
        super(monitor);

        // Set the environment property (for all cases) and reload logger configuration
        // Prepare all the subfolders.
        LoggerManager.init();

        /*
         * Create a tmp directory where to store: - Files whose first opened stream is an input one - Object files
         */
        String wDir = System.getProperty(COMPSsConstants.WORKING_DIR);
        if (wDir != null && !wDir.isEmpty()) {
            if (!wDir.endsWith(File.separator)) {
                wDir = wDir + File.separator;
            }
            this.tempDirPath = wDir;
        } else {
            this.tempDirPath = LoggerManager.getLogDir() + "tmpFiles" + File.separator;
        }
        // this.tempDirPath may already exist
        boolean wDirExists = new File(this.tempDirPath).exists();
        if (!wDirExists) {
            if (!new File(this.tempDirPath).mkdirs()) {
                ErrorManager.error(ERROR_TEMP_DIR);
            }
        }

        // Configure storage
        String storageConf = System.getProperty(COMPSsConstants.STORAGE_CONF);
        if (storageConf == null || storageConf.equals("") || storageConf.equals("null")) {
            storageConf = "null";
            LOGGER.warn("No storage configuration file passed");
        }
        this.storageConf = storageConf;

        // Configure execution
        String executionType = System.getProperty(COMPSsConstants.TASK_EXECUTION);
        if (executionType == null || executionType.equals("") || executionType.equals("null")) {
            executionType = COMPSsConstants.TaskExecution.COMPSS.toString();
            LOGGER.warn("No executionType passed");
        } else {
            executionType = executionType.toUpperCase();
        }
        this.executionType = TaskExecution.valueOf(executionType);

        // Nested Management variables
        this.runtimeApi = null;
        this.loaderApi = null;

        // Configure worker debug level
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
            pythonInterpreter = COMPSsDefaults.PYTHON_INTERPRETER;
        }

        // Get python version
        String pythonVersion = System.getProperty(COMPSsConstants.PYTHON_VERSION);
        if (pythonVersion == null || pythonVersion.isEmpty() || pythonVersion.equals("null")) {
            pythonVersion = COMPSsDefaults.PYTHON_VERSION;
        }

        // Configure python virtual environment
        String pythonVEnv = System.getProperty(COMPSsConstants.PYTHON_VIRTUAL_ENVIRONMENT);
        if (pythonVEnv == null || pythonVEnv.isEmpty() || pythonVEnv.equals("null")) {
            pythonVEnv = COMPSsDefaults.PYTHON_VIRTUAL_ENVIRONMENT;
        }
        String pythonPropagateVEnv = System.getProperty(COMPSsConstants.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT);
        if (pythonPropagateVEnv == null || pythonPropagateVEnv.isEmpty() || pythonPropagateVEnv.equals("null")) {
            pythonPropagateVEnv = COMPSsDefaults.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT;
        }

        String pythonPath = System.getProperty(COMPSsConstants.WORKER_PP);
        if (pythonPath == null || pythonPath.isEmpty()) {
            pythonPath = "";
        }

        // Get Python extrae config file
        String pythonExtraeFile = System.getProperty(COMPSsConstants.PYTHON_EXTRAE_CONFIG_FILE);
        if (pythonExtraeFile == null || pythonExtraeFile.isEmpty() || pythonExtraeFile.equals("null")) {
            pythonExtraeFile = COMPSsDefaults.PYTHON_CUSTOM_EXTRAE_FILE;
        }

        // Get Python MPI worker invocation
        String pythonMpiWorker = System.getProperty(COMPSsConstants.PYTHON_MPI_WORKER);
        if (pythonMpiWorker == null || pythonMpiWorker.isEmpty() || pythonMpiWorker.equals("null")) {
            pythonMpiWorker = COMPSsDefaults.PYTHON_MPI_WORKER;
        }

        // Get Python worker cache
        String pythonWorkerCache = System.getProperty(COMPSsConstants.PYTHON_WORKER_CACHE);
        if (pythonWorkerCache == null || pythonWorkerCache.isEmpty() || pythonWorkerCache.equals("null")) {
            pythonWorkerCache = COMPSsDefaults.PYTHON_WORKER_CACHE;
        }

        // Get Python worker cache
        String pythonCacheProfiler = System.getProperty(COMPSsConstants.PYTHON_CACHE_PROFILER);
        if (pythonCacheProfiler == null || pythonCacheProfiler.isEmpty() || pythonCacheProfiler.equals("null")) {
            pythonCacheProfiler = COMPSsDefaults.PYTHON_CACHE_PROFILER;
        }

        // Create Python cache profiler
        if (Boolean.parseBoolean(pythonCacheProfiler) == true) {
            String pythonCacheProfilerPath = tempDirPath + "cache_profiler.json";
            // touch file profiling.
            File pythonCacheProfilerFile = new File(pythonCacheProfilerPath);
            if (!pythonCacheProfilerFile.exists()) {
                FileOutputStream pythonCacheProfilerFOS;
                try {
                    pythonCacheProfilerFOS = new FileOutputStream(pythonCacheProfilerFile);
                    String emptyJsonContent = "{}";
                    pythonCacheProfilerFOS.write(emptyJsonContent.getBytes());
                    pythonCacheProfilerFOS.close();
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        // EAR
        String earing = System.getProperty(COMPSsConstants.EAR);
        if (earing == null || earing.isEmpty() || earing.equals("null")) {
            earing = COMPSsDefaults.EAR;
        }
        this.ear = earing.toUpperCase().compareTo("TRUE") == 0;

        // PROVENANCE
        String provenance = System.getProperty(COMPSsConstants.DATA_PROVENANCE);
        if (provenance == null || provenance.isEmpty() || provenance.equals("null")) {
            provenance = COMPSsDefaults.DP_ENABLED;
        }
        this.dataProvenance = provenance.toUpperCase().compareTo("TRUE") == 0;

        JavaParams javaParams = new JavaParams(classPath);
        PythonParams pyParams = new PythonParams(pythonInterpreter, pythonVersion, pythonVEnv, pythonPropagateVEnv,
            pythonPath, pythonExtraeFile, pythonMpiWorker, pythonWorkerCache, pythonCacheProfiler);
        CParams cParams = new CParams(classPath);

        this.langParams[Lang.JAVA.ordinal()] = javaParams;
        this.langParams[Lang.PYTHON.ordinal()] = pyParams;
        this.langParams[Lang.C.ordinal()] = cParams;
        RParams rParams = new RParams(classPath);
        this.langParams[Lang.R.ordinal()] = rParams;

        String workerPersistentC = System.getProperty(COMPSsConstants.WORKER_PERSISTENT_C);
        if (workerPersistentC == null || workerPersistentC.isEmpty() || workerPersistentC.equals("null")) {
            workerPersistentC = COMPSsDefaults.PERSISTENT_C;
        }
        this.persistentEnabled = workerPersistentC.toUpperCase().compareTo("TRUE") == 0;

        boolean reuse = Boolean.parseBoolean(System.getProperty(COMPSsConstants.REUSE_RESOURCES_ON_BLOCK));
        this.executionManager = new ExecutionManager(this, 0, ThreadBinder.BINDER_DISABLED, reuse, 0,
            ThreadBinder.BINDER_DISABLED, 0, ThreadBinder.BINDER_DISABLED, 0, 0);
        try {
            this.executionManager.init();
        } catch (InitializationException ie) {
            ErrorManager.error(EXECUTION_MANAGER_ERR, ie);
        }
    }

    @Override
    public String getName() {
        return MASTER_NAME;
    }

    @Override
    public String getAdaptor() {
        return null;
    }

    @Override
    public Object getProjectProperties() {
        return null;
    }

    @Override
    public Object getResourcesProperties() {
        return null;
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
                                    listener.notifyFailure(fOp, e);
                                }
                            }

                            @Override
                            public void notifyFailure(DataOperation fOp, Exception e) {
                                if (DEBUG) {
                                    LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget()
                                        + " to " + tgtBO.getName());
                                }
                                LOGGER.error("ERROR: managing obtain binding object at cache", e);
                                listener.notifyFailure(fOp, e);
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

    private void handleInMemoryCopy(LogicalData ld, DataLocation src, LogicalData tgtData, DataLocation tgtLoc,
        String tgtPath, Transferable reason, EventListener listener) {
        if (ld.isAlias(tgtData)) {
            LOGGER.debug("Object already in memory. Avoiding copy and setting dataTarget to " + tgtPath);
            notifyDataObtaining(tgtPath, reason, listener);
        } else {
            SimpleURI serialURI = getCompletePath(DataType.FILE_T, tgtPath);
            String serialPath = serialURI.getPath();
            LOGGER.debug("Serializing data " + ld.getName() + " to " + serialPath);
            Object o = ld.getValue();
            FileOpsManager.serializeAsync(o, serialPath, new FileOpListener() {

                @Override
                public void completed() {
                    if (tgtData != null) {
                        try {
                            DataLocation loc = DataLocation.createLocation(Comm.getAppHost(), serialURI);
                            synchronized (tgtData) {
                                tgtData.addLocation(loc);
                            }
                            LOGGER.debug("Object in memory. Set dataTarget to " + tgtPath);
                            notifyDataObtaining(tgtPath, reason, listener);
                        } catch (IOException ioe) {
                            failed(ioe);
                        }

                    }
                }

                @Override
                public void failed(IOException e) {
                    ErrorManager.warn("Error copying file from memory to " + tgtPath, e);
                    obtainDataAsynch(ld, src, tgtData, tgtLoc, tgtPath, reason, listener);
                }
            });
        }
    }

    /**
     * Retrieves a file data.
     *
     * @param srcData Source LogicalData.
     * @param srcLoc Preferred source location.
     * @param tgtLoc Preferred target location.
     * @param tgtData Target LogicalData.
     * @param reason Transfer reason.
     * @param listener Transfer listener.
     */
    private void obtainFileData(LogicalData srcData, DataLocation srcLoc, LogicalData tgtData, DataLocation tgtLoc,
        String tgtPath, Transferable reason, EventListener listener) {
        // Check if file is already on the Path
        List<MultiURI> uris = srcData.getURIs();
        for (MultiURI u : uris) {
            if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug(srcData.getName() + " is at " + u.toString() + "(" + hostname + ")");
            }
            if (u.getHost().getNode() == this) {
                String localPath = u.getPath();
                if ((reason.isTargetFlexible() && srcData.isAlias(tgtData)) || tgtPath.compareTo(localPath) == 0) {
                    LOGGER.debug(srcData.getName() + " is already at " + localPath);
                    // File already in the Path
                    notifyDataObtaining(u.getPath(), reason, listener);
                    return;
                }
            }
        }

        // Check if there are current copies in progress bringing it into the node.
        if (DEBUG) {
            LOGGER.debug(
                "Data " + srcData.getName() + " not in memory. Checking if there is a copy to the master in progress");
        }

        Collection<Copy> copiesInProgress = srcData.getCopiesInProgress();
        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            if (handleSiblingCopy(srcData, tgtData, tgtLoc, tgtPath, reason, listener, copiesInProgress)) {
                return;
            }
        }

        // Checking if file is already in master
        if (DEBUG) {
            LOGGER.debug("Checking if " + srcData.getName() + " is at master (" + Comm.getAppHost().getName() + ").");
        }

        for (MultiURI u : uris) {
            if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug(srcData.getName() + " is at " + u.toString() + "(" + hostname + ")");
            }
            if (u.getHost().getNode() == this) {
                if (handleLocalFileCopy(srcData, u, tgtData, tgtLoc, tgtPath, reason, listener)) {
                    return;
                }
            } else {
                if (DEBUG) {
                    String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                    LOGGER.debug("Data " + srcData.getName() + " copy in " + hostname + " not evaluated now");
                }
            }
        }

        // Ask the transfer from an specific source
        if (srcLoc != null) {
            if (handleTransferFromSpecificRemoteLocation(srcData, srcLoc, tgtData, tgtLoc, reason, listener)) {
                return;
            }
        } else {
            LOGGER.debug("Source data location is null. Trying other alternatives");
        }
        // Preferred source is null or copy has failed. Trying to retrieve data from any host

        Set<Resource> hosts;
        synchronized (srcData) {
            hosts = srcData.getAllHosts();
        }
        if (handleTransferFromRemote(srcData, hosts, srcLoc, tgtData, tgtLoc, reason, listener)) {
            return;
        }

        // If we have not exited before, any copy method was successful. Raise warning
        ErrorManager.warn("Error file " + srcData.getName() + " not transferred to " + tgtPath);
        listener.notifyFailure(null, null);
    }

    private boolean handleSiblingCopy(LogicalData ld, LogicalData tgtData, DataLocation tgtLoc, String tgtPath,
        Transferable reason, EventListener listener, Collection<Copy> copiesInProgress) {

        for (Copy copy : copiesInProgress) {
            if (copy != null) {
                DataLocation copyLoc = copy.getTargetLoc();
                if (copyLoc != null && copyLoc.getHosts().contains(Comm.getAppHost())) {
                    try {
                        if (tgtLoc.getProtocol() == ProtocolType.OBJECT_URI && ld.isAlias(tgtData)) {
                            reason.setDataTarget(tgtPath);
                            copy.addEventListener(listener);
                        } else {
                            copy.addSiblingCopy(tgtPath, tgtLoc, tgtData, reason, listener);
                        }
                        LOGGER.debug("Copy in progress tranferring " + ld.getName() + " to master. Wait for finish");
                        return true;
                    } catch (CompletedCopyException cce) {
                        // This copy already ended and its value may have been compromised. It is necessary to look
                        // for another value source.
                    }
                }
            }
        }

        return false;
    }

    private boolean handleLocalFileCopy(LogicalData ld, MultiURI localURI, LogicalData tgtData, DataLocation tgtLoc,
        String tgtPath, Transferable reason, EventListener listener) {
        String localPath = localURI.getPath();
        try {
            if (DEBUG) {
                LOGGER.debug("Data " + ld.getName() + " is already accessible at " + localPath);
            }

            boolean inMemory = false;
            if (tgtLoc.getProtocol() == ProtocolType.OBJECT_URI) {
                try {
                    if (DEBUG) {
                        LOGGER.debug("Deserializing from file " + localPath);
                    }
                    Object o = FileOpsManager.deserializeSync(localPath);
                    tgtData.setValue(o);
                    inMemory = true;
                } catch (Exception e) {
                    if (DEBUG) {
                        LOGGER.debug("Could not deserialize from file " + localPath);
                    }
                }
            }

            if (!inMemory) {
                DataType type;
                if (tgtLoc.getProtocol() == ProtocolType.DIR_URI) {
                    type = DataType.DIRECTORY_T;
                } else {
                    type = DataType.FILE_T;
                }
                SimpleURI serialURI = getCompletePath(type, tgtPath);
                tgtLoc = DataLocation.createLocation(Comm.getAppHost(), serialURI);

                if (reason.isSourcePreserved() || ld.countKnownAlias() > 1) {
                    if (DEBUG) {
                        LOGGER.debug("Master local copy " + ld.getName() + " from " + localURI.getHost().getName()
                            + " to " + tgtPath);
                    }
                    FileOpsManager.copySync(new File(localPath), new File(tgtPath));

                } else {
                    if (DEBUG) {
                        LOGGER.debug("Master local move " + ld.getName() + " from " + localURI.getHost().getName()
                            + " to " + tgtPath);
                    }
                    try {
                        SimpleURI deletedUri = new SimpleURI(localPath);
                        DataLocation loc = DataLocation.createLocation(Comm.getAppHost(), deletedUri);
                        synchronized (ld) {
                            ld.removeLocation(loc);
                        }
                    } catch (Exception e) {
                        ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + tgtPath, e);
                    }
                    FileOpsManager.moveSync(new File(localPath), new File(tgtPath));
                }
            }

            if (tgtData != null) {
                synchronized (tgtData) {
                    tgtData.addLocation(tgtLoc);
                }
            }

            LOGGER.debug("File on path. Set data target to " + tgtPath);
            notifyDataObtaining(tgtPath, reason, listener);
            return true;
        } catch (IOException ex) {
            ErrorManager.warn(
                "Error master local copy file from " + localURI.getPath() + " to " + tgtPath + " with replacing", ex);
        }
        return false;
    }

    private boolean handleTransferFromSpecificRemoteLocation(LogicalData ld, DataLocation srcLoc, LogicalData tgtData,
        DataLocation tgtLoc, Transferable reason, EventListener listener) {
        for (Resource srcRes : srcLoc.getHosts()) {
            if (handleTransferFromSpecificRemote(ld, srcRes, srcLoc, tgtData, tgtLoc, reason, listener)) {
                return true;
            }
        }
        return false;
    }

    private boolean handleTransferFromRemote(LogicalData ld, Set<Resource> hosts, DataLocation srcLoc,
        LogicalData tgtData, DataLocation tgtLoc, Transferable reason, EventListener listener) {
        for (Resource srcRes : hosts) {
            if (handleTransferFromSpecificRemote(ld, srcRes, srcLoc, tgtData, tgtLoc, reason, listener)) {
                return true;
            }
        }
        return false;
    }

    private boolean handleTransferFromSpecificRemote(LogicalData ld, Resource srcRes, DataLocation srcLoc,
        LogicalData tgtData, DataLocation tgtLoc, Transferable reason, EventListener listener) {
        COMPSsNode node = srcRes.getNode();
        if (node != this) {
            try {
                LOGGER.debug("Sending data " + ld.getName() + " from " + srcRes.getName() + " to " + tgtLoc);
                node.sendData(ld, srcLoc, tgtLoc, tgtData, reason, listener);
                LOGGER.debug("Data " + ld.getName() + " sent.");
                return true;
            } catch (Exception e) {
                LOGGER.error("Error: exception sending data", e);
            }

        } else {
            if (DEBUG) {
                LOGGER.debug("Data " + ld.getName() + " copy in " + srcRes.getName() + " already checked");
            }
        }
        return false;
    }

    private void notifyDataObtaining(String path, Transferable reason, EventListener listener) {
        reason.setDataTarget(path);
        listener.notifyEnd(null);
    }

    @Override
    public void obtainData(LogicalData srcData, DataLocation srcLoc, DataLocation tgtLoc, LogicalData tgtData,
        Transferable reason, EventListener listener) {
        LOGGER.info("Obtain Data " + srcData.getName());
        if (DEBUG) {
            LOGGER.debug(srcData != null ? "srcData: " + srcData.toString() : "srcData is null");
            LOGGER.debug(reason != null ? "Reason: " + reason.getType() : "Reason is null");
            LOGGER
                .debug(srcLoc != null
                    ? "Source Data location: " + srcLoc.getType().toString() + " " + srcLoc.getProtocol().toString()
                        + " " + srcLoc.getURIs().get(0)
                    : "Source Data location is null");
            if (tgtLoc != null) {
                if (tgtLoc.getProtocol() != ProtocolType.PERSISTENT_URI) {
                    LOGGER.debug("Target Data location: " + tgtLoc.getType().toString() + " "
                        + tgtLoc.getProtocol().toString() + " " + tgtLoc.getURIs().get(0));
                } else {
                    LOGGER.debug(
                        "Target Data location: " + tgtLoc.getType().toString() + " " + tgtLoc.getProtocol().toString());
                }
            } else {
                LOGGER.debug("Target Data location is null");
            }
            LOGGER.debug(tgtData != null ? "tgtData: " + tgtData.toString() : "tgtData is null");
        }

        String tgtPath = null;
        if (tgtLoc != null) {
            tgtPath = tgtLoc.getURIInHost(Comm.getAppHost()).getPath();
        }
        if (reason != null && (reason.getType().equals(DataType.COLLECTION_T)
            || reason.getType().equals(DataType.DICT_COLLECTION_T))) {
            obtainCollection(srcData, srcLoc, tgtData, tgtLoc, tgtPath, reason, listener);
            return;
        }
        /*
         * Check if data is binding data
         */
        if (srcData.isBindingData() || (reason != null && reason.getType().equals(DataType.BINDING_OBJECT_T))
            || (srcLoc != null && srcLoc.getType().equals(LocationType.BINDING))
            || (tgtLoc != null && tgtLoc.getType().equals(LocationType.BINDING))) {
            obtainBindingData(srcData, srcLoc, tgtLoc, tgtData, reason, listener);
            return;
        }
        /*
         * PSCO transfers are always available, if any SourceLocation is PSCO, don't transfer
         */
        String pscoId = srcData.getPscoId();
        if (pscoId != null) {
            obtainPSCO(pscoId, reason, listener);
            return;
        }

        /*
         * Otherwise the data is a file or an object that can be already in the master memory, in the master disk or
         * being transfered
         */

        // Check if data is in memory (no need to check if it is PSCO since previous case avoids it)
        if (srcData.isInMemory() && srcData.isAlias(tgtData)) {
            LOGGER.debug("Object already in memory. Avoiding copy and setting dataTarget to " + tgtPath);
            notifyDataObtaining(tgtPath, reason, listener);
            return;
        }
        obtainDataAsynch(srcData, srcLoc, tgtData, tgtLoc, tgtPath, reason, listener);
    }

    private void obtainCollection(LogicalData srcData, DataLocation srcLoc, LogicalData tgtData, DataLocation target,
        String tgtPath, Transferable reason, EventListener listener) {
        String targetPath;
        if (target != null) {
            targetPath = target.getURIInHost(Comm.getAppHost()).getPath();

        } else {
            if (tgtData != null) {
                targetPath = tgtData.getName();
            } else {
                targetPath = srcData.getName();
                LOGGER.warn(
                    "No target location neither target data available. Setting targetPath to " + srcData.getName());
            }
        }
        LOGGER.debug("Data " + srcData.getName()
            + "is COLLECTION_T/DICT_COLLECTION_T nothing to tranfer. Elements already transferred."
            + "Setting target path to " + targetPath);
        notifyDataObtaining(tgtPath, reason, listener);
    }

    private void obtainPSCO(String pscoId, Transferable reason, EventListener listener) {
        /*
         * PSCO transfers are always available, if any SourceLocation is PSCO, don't transfer
         */
        LOGGER.debug("Object in Persistent Storage. Set dataTarget to " + pscoId);
        notifyDataObtaining(pscoId, reason, listener);
    }

    private void obtainDataAsynch(LogicalData srcData, DataLocation srcLoc, LogicalData tgtData, DataLocation tgtLoc,
        String tgtPath, Transferable reason, EventListener listener) {
        FileOpsManager.composedOperationAsync(new Runnable() {

            @Override
            public void run() {
                if (srcData.isInMemory()) {
                    handleInMemoryCopy(srcData, srcLoc, tgtData, tgtLoc, tgtPath, reason, listener);
                    return;
                }
                obtainFileData(srcData, srcLoc, tgtData, tgtLoc, tgtPath, reason, listener);
            }
        });
    }

    @Override
    public void enforceDataObtaining(Transferable reason, EventListener listener) {
        // Copy already done on obtainData()
        listener.notifyEnd(null);
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {

        return new LocalJob(taskId, taskParams, impl, res, slaveWorkersNodeNames, listener, predecessors,
            numSuccessors);
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        String path = null;
        switch (type) {
            case DIRECTORY_T:
                if (!name.startsWith(File.separator)) {
                    name = tempDirPath + name;
                }
                path = ProtocolType.DIR_URI.getSchema() + name;
                break;
            case FILE_T:
                if (!name.startsWith(File.separator)) {
                    name = tempDirPath + name;
                }
                path = ProtocolType.FILE_URI.getSchema() + name;
                break;
            case OBJECT_T:
                path = ProtocolType.OBJECT_URI.getSchema() + name;
                break;
            case COLLECTION_T:
            case DICT_COLLECTION_T:
                path = ProtocolType.OBJECT_URI.getSchema() + tempDirPath + name;
                break;
            case STREAM_T:
                path = ProtocolType.STREAM_URI.getSchema() + name;
                break;
            case EXTERNAL_STREAM_T:
                path = ProtocolType.EXTERNAL_STREAM_URI.getSchema() + tempDirPath + name;
                break;
            case PSCO_T:
            case EXTERNAL_PSCO_T:
                path = ProtocolType.PERSISTENT_URI.getSchema() + name;
                break;
            case BINDING_OBJECT_T:
                path = ProtocolType.BINDING_URI.getSchema() + tempDirPath + name;
                break;
            default:
                LOGGER.warn("Unrecognized type: " + type);
                return null;
        }

        // Switch path to URI
        return new SimpleURI(path);
    }

    @Override
    public void deleteTemporary() {
        File dir = new File(Comm.getAppHost().getWorkingDirectory());
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
    public Set<String> generateWorkerAnalysisFiles() {
        // Should not be executed on a COMPSsMaster
        return null;
    }

    @Override
    public Set<String> generateWorkerDebugFiles() {
        // Should not be executed on a COMPSsMaster
        return null;
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
        InvocationExecutionRequest.Listener listener = new InvocationExecutionRequest.Listener() {

            @Override
            public void onResultAvailable(InvocationParam param) {
                LocalParameter lp = (LocalParameter) param;
                job.notifyResultAvailable(lp);
            }

            @Override
            public void notifyEnd(Invocation invocation, boolean success, COMPSsException e) {
                job.profileEndNotification();
                if (success) {
                    job.completed();
                } else {
                    if (e != null) {
                        job.exception(e);
                    } else {
                        job.failed(JobEndStatus.EXECUTION_FAILED);
                    }
                }
            }
        };
        InvocationExecutionRequest exec = new InvocationExecutionRequest(job, listener);
        this.executionManager.enqueue(exec);
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
    public String getLogDir() {
        return LoggerManager.getLogDir();
    }

    @Override
    public String getAnalysisDir() {
        // This decides where the files generated by the worker that are not log are stored
        return this.tempDirPath + "analysis/";
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
        return LoggerManager.getJobsLogDir() + "job" + invocation.getJobId() + "_" + invocation.getHistory();
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
                String dataId = localParam.getSourceDataId();
                LogicalData ld = Comm.getData(dataId);
                Object value = null;
                if (ld.isInMemory()) {
                    value = ld.getValue();
                } else {
                    try {
                        ld.loadFromStorage();
                        value = ld.getValue();
                    } catch (CannotLoadException cle) {
                        try {
                            value = FileOpsManager.deserializeSync(dpar.getDataTarget());
                        } catch (ClassNotFoundException | IOException e) {
                            throw new UnloadableValueException(e);
                        }
                    }
                }
                invParam.setValue(value);
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
    public void storeParam(InvocationParam invParam, boolean createIfNonExistent)
        throws UnwritableValueException, NonExistentDataException {
        LocalParameter lp = (LocalParameter) invParam;
        Parameter param = lp.getParam();
        SimpleURI resultUri = null;
        switch (param.getType()) {
            case DIRECTORY_T:
                resultUri = new SimpleURI(ProtocolType.DIR_URI.getSchema() + tempDirPath + lp.getDataMgmtId());
                try {
                    DataLocation outLoc = DataLocation.createLocation(Comm.getAppHost(), resultUri);
                    Comm.registerLocation(lp.getDataMgmtId(), outLoc);
                } catch (IOException e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + resultUri, e);
                }
                break;
            case FILE_T:
                resultUri = storeFileParam(lp, createIfNonExistent);
                break;
            case COLLECTION_T:
            case DICT_COLLECTION_T:
                resultUri = new SimpleURI(ProtocolType.OBJECT_URI.getSchema() + tempDirPath + lp.getDataMgmtId());
                try {
                    DataLocation outLoc = DataLocation.createLocation(Comm.getAppHost(), resultUri);
                    Comm.registerLocation(lp.getDataMgmtId(), outLoc);
                } catch (IOException e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + resultUri, e);
                }
                break;
            case OBJECT_T: {
                String resultName = lp.getDataMgmtId();
                Comm.registerValue(resultName, lp.getValue());
                resultUri = new SimpleURI(ProtocolType.OBJECT_URI.getSchema() + resultName);
                break;
            }
            case STREAM_T: {
                String resultName = lp.getDataMgmtId();
                Comm.registerValue(resultName, lp.getValue());
                resultUri = new SimpleURI(ProtocolType.STREAM_URI.getSchema() + resultName);
                break;
            }
            case EXTERNAL_STREAM_T: {
                String resultName = lp.getDataMgmtId();
                resultUri = new SimpleURI(ProtocolType.EXTERNAL_STREAM_URI.getSchema() + tempDirPath + resultName);
                try {
                    DataLocation outLoc = DataLocation.createLocation(Comm.getAppHost(), resultUri);
                    Comm.registerLocation(lp.getDataMgmtId(), outLoc);
                } catch (IOException e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + resultUri, e);
                }
                break;
            }
            case PSCO_T: {
                String pscoId = ((StubItf) lp.getValue()).getID();
                String resultName = lp.getDataMgmtId();
                Comm.registerPSCO(resultName, pscoId);
                resultUri = new SimpleURI(ProtocolType.PERSISTENT_URI.getSchema() + pscoId);
                break;
            }
            case EXTERNAL_PSCO_T: {
                String pscoId = (String) lp.getValue();
                String resultName = lp.getDataMgmtId();
                Comm.registerExternalPSCO(resultName, pscoId);
                resultUri = new SimpleURI(ProtocolType.PERSISTENT_URI.getSchema() + pscoId);
                break;
            }
            case BINDING_OBJECT_T:
                // No need to store anything. Already stored on the binding
                resultUri = new SimpleURI(ProtocolType.BINDING_URI.getSchema() + tempDirPath + lp.getValue());
                try {
                    DataLocation outLoc = DataLocation.createLocation(Comm.getAppHost(), resultUri);
                    Comm.registerLocation(lp.getDataMgmtId(), outLoc);
                } catch (IOException e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + resultUri, e);
                }
                break;
            default:
                throw new UnsupportedOperationException("Not supported yet." + param.getType());

        }

        // Update Task information
        DependencyParameter dp = (DependencyParameter) lp.getParam();
        dp.setDataTarget(resultUri.toString());
    }

    private SimpleURI storeFileParam(LocalParameter lp, boolean createIfNonExistent)
        throws UnwritableValueException, NonExistentDataException {
        String filepath = (String) lp.getValue();
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.CHECK_OUT_PARAM);
        }

        SimpleURI uri = new SimpleURI(ProtocolType.FILE_URI.getSchema() + filepath);
        File f = new File(filepath);
        boolean fExists = f.exists();
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.CHECK_OUT_PARAM);
        }
        if (!fExists) {
            if (createIfNonExistent) {
                System.out.println("Creating new blank file at " + filepath);
                try {
                    f.createNewFile(); // NOSONAR ignoring result. It couldn't exists.
                    try {
                        DataLocation outLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
                        Comm.registerLocation(lp.getDataMgmtId(), outLoc);
                    } catch (IOException e) {
                        ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + filepath, e);
                    }
                } catch (IOException e) {
                    LOGGER.debug("ERROR creating new blank file at " + filepath);
                    throw new UnwritableValueException(e);
                }
            }
            throw new NonExistentDataException(filepath);
        } else {
            try {
                DataLocation outLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
                Comm.registerLocation(lp.getDataMgmtId(), outLoc);
            } catch (IOException e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + filepath, e);
            }
        }
        return uri;
    }

    public String getWorkingDirectory() {
        return this.tempDirPath;
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

    @Override
    public void verifyNodeIsRunning() {
        // No need to verify, it's not possible to loose the connection with your own process.
    }

    @Override
    public COMPSsRuntime getRuntimeAPI() {
        return this.runtimeApi;
    }

    public void setRuntimeApi(COMPSsRuntime runtimeApi) {
        this.runtimeApi = runtimeApi;
    }

    @Override
    public LoaderAPI getLoaderAPI() {
        return this.loaderApi;
    }

    @Override
    public String getEnvironmentScript() {
        return null;
    }

    @Override
    public boolean getEar() {
        return this.ear;
    }

    @Override
    public boolean getDataProvenance() {
        return this.dataProvenance;
    }

    public void setLoaderApi(LoaderAPI loaderApi) {
        this.loaderApi = loaderApi;
    }
}
