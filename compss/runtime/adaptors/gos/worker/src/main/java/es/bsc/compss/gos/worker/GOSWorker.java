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
package es.bsc.compss.gos.worker;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.gos.executor.types.ExecutionEnd;
import es.bsc.compss.gos.master.utils.ForbiddenCharacters;
import es.bsc.compss.invokers.types.CParams;
import es.bsc.compss.invokers.types.JavaParams;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.invokers.types.RParams;
import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.ErrorHandler;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationExecutionRequest;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.types.execution.exceptions.NonExistentDataException;
import es.bsc.compss.types.execution.exceptions.UnloadableValueException;
import es.bsc.compss.types.execution.exceptions.UnwritableValueException;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.definition.AbstractMethodImplementationDefinition;
import es.bsc.compss.types.implementations.definition.BinaryDefinition;
import es.bsc.compss.types.implementations.definition.COMPSsDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDefinition;
import es.bsc.compss.types.implementations.definition.DecafDefinition;
import es.bsc.compss.types.implementations.definition.MPIDefinition;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.implementations.definition.MultiNodeDefinition;
import es.bsc.compss.types.implementations.definition.OmpSsDefinition;
import es.bsc.compss.types.implementations.definition.OpenCLDefinition;
import es.bsc.compss.types.implementations.definition.PythonMPIDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.util.serializers.Serializer;
import es.bsc.compss.utils.execution.ExecutionManager;
import es.bsc.compss.worker.COMPSsException;
import es.bsc.distrostreamlib.client.DistroStreamClient;
import es.bsc.distrostreamlib.exceptions.DistroStreamClientInitException;
import es.bsc.distrostreamlib.requests.StopRequest;
import es.bsc.distrostreamlib.server.types.StreamBackend;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import storage.StorageException;
import storage.StorageItf;


public class GOSWorker implements InvocationContext {

    private static final String ERROR_STREAMING_INIT = "ERROR: Cannot load Streaming Client";
    private static final String ERROR_STREAMING_FINISH = "ERROR: Cannot stop Streaming Client";

    private static final String ERROR_STORAGE_CONF_INIT = "ERROR: Cannot load storage configuration file: ";
    private static final String ERROR_STORAGE_CONF_FINISH = "ERROR: Cannot stop StorageItf";

    private static final String EXECUTION_MANAGER_ERR = "Error starting ExecutionManager";
    private static final String WARN_UNSUPPORTED_METHOD_TYPE = "WARNING: Unsupported method type";
    private static final int HOSTS_FLAGS_SIZE = 10;
    private static final int TRACING_FLAGS_SIZE = 7;
    private static final int LANG_SIZE_FLAGS = 11;
    private static final int EXTRA_FLAG_SIZE = 0;
    private static final int HOST_INX = 0;
    private static final int TRACING_INX = HOST_INX + HOSTS_FLAGS_SIZE;
    private static final int LANG_INX = HOSTS_FLAGS_SIZE + TRACING_FLAGS_SIZE;
    private static final int EXTRA_FLAG_INX = LANG_INX + LANG_SIZE_FLAGS;
    private static final int DEFAULT_FLAGS_SIZE =
        HOSTS_FLAGS_SIZE + TRACING_FLAGS_SIZE + LANG_SIZE_FLAGS + EXTRA_FLAG_SIZE;
    private final String hostName;
    private final String appDir;
    private final String installDir;
    private final String workingDir;
    private final String storageConf;
    private final StreamBackend streamBackend;
    private final String streamMasterName;
    private final int streamMasterPort;
    private final ExecutionManager executionManager;
    private final boolean debug;
    private final boolean tracing;
    private final int tracingSlot;
    private final Lang lang;
    private String[] tracingParams;
    private final LanguageParams[] langParams;
    private String envScriptPath;


    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {

        // Character replacement for correct transfer through scripts
        ForbiddenCharacters.init();
        ForbiddenCharacters.decode(args);

        int i = 0;
        final boolean debug = Boolean.parseBoolean(args[HOST_INX + i++]);

        final String workerName = args[HOST_INX + i++];
        final String workingDir = args[HOST_INX + i++];
        final String installDir = args[HOST_INX + i++];
        final String appDir = args[HOST_INX + i++];
        final String envScriptPath = args[HOST_INX + i++];
        // Configures storage API if necessary
        final String storageConfArg = args[HOST_INX + i++];
        // Configures streaming if necessary
        final String streamingArg = args[HOST_INX + i++];
        String streamMasterName = args[HOST_INX + i++];
        int streamMasterPort = Integer.parseInt(args[HOST_INX + i++]);

        /*
         * tracingFlags=( "${tracing}" "${runtimeEventType}" "${sandBoxCreationId}" "${sandBoxRemovalId}"
         * "${taskEventType}" "${tracingTaskId}" "${slot}" )
         */
        final boolean tracing = Boolean.parseBoolean(args[TRACING_INX]);
        final String[] tracingParams = Arrays.copyOfRange(args, TRACING_INX + 1, TRACING_INX + TRACING_FLAGS_SIZE);
        final int tracingSlot = Integer.parseInt(args[TRACING_INX + 6]);

        /*
         * langFlags=("${lang}" "${taskSandboxWorkingDir}" "${cp}" "${pythonpath}" "${pythonInterpreter}"
         * "${pythonVersion}" "${pythonVirtualEnvironment}" "${pythonPropagateVirtualEnvironment}"
         * "${pythonExtraeFile}")
         */
        i = 0;
        boolean persistentC = Boolean.parseBoolean(args[LANG_INX + i++]);
        final String lang = args[LANG_INX + i++];
        final String taskSandboxWorkingDir = args[LANG_INX + i++];
        final String classpath = args[LANG_INX + i++];
        final String pythonpath = args[LANG_INX + i++];
        final String pythonInterpreter = args[LANG_INX + i++];
        final String pythonVersion = args[LANG_INX + i++];
        final String pythonVirtualEnvironment = args[LANG_INX + i++];
        final String pythonPropagateVirtualEnvironment = args[LANG_INX + i++];
        final String pythonExtraeFile = args[LANG_INX + i++];
        final String pythonMpiWorker = "null";
        final String pythonWorkerCache = "null";
        final String pythonCacheProfiler = "null";

        final JavaParams javaParams = new JavaParams(classpath);
        final PythonParams pyParams = new PythonParams(pythonInterpreter, pythonVersion, pythonVirtualEnvironment,
            pythonPropagateVirtualEnvironment, pythonpath, pythonExtraeFile, pythonMpiWorker, pythonWorkerCache,
            pythonCacheProfiler);
        final CParams cParams = new CParams(classpath);
        final RParams rParams = new RParams(classpath);
        LanguageParams[] langParams = new LanguageParams[COMPSsConstants.Lang.values().length];
        langParams[Lang.JAVA.ordinal()] = javaParams;
        langParams[Lang.PYTHON.ordinal()] = pyParams;
        langParams[Lang.C.ordinal()] = cParams;
        langParams[Lang.R.ordinal()] = rParams;

        // Initialize errorHandler for shutting down worker in case of errro.
        ErrorHandler errorHandlerGOS = new ErrorHandler() {

            Logger log = LogManager.getLogger(Loggers.ERROR_MANAGER);


            @Override
            public boolean handleError() {
                return handleFatalError();
            }

            @Override
            public boolean handleFatalError() {
                log.info("Shutting down remote COMPSs...");
                log.error("Error detected. Shutting down Remote Worker");
                System.exit(1);
                return true;
            }

        };
        ErrorManager.init(errorHandlerGOS);

        // Retrieve task arguments
        // GOSInvocation implDef = parseArguments(args);

        String streaming = (streamingArg == null || streamingArg.isEmpty() || streamingArg.equals("null")) ? "NONE"
            : streamingArg.toUpperCase();
        StreamBackend streamBackend = StreamBackend.valueOf(streaming);

        if (!streamBackend.equals(StreamBackend.NONE)) {
            try {
                DistroStreamClient.initAndStart(streamMasterName, streamMasterPort);
            } catch (DistroStreamClientInitException dscie) {
                ErrorManager.fatal(ERROR_STREAMING_INIT, dscie);
            }
        }

        final GOSInvocation implDef = parseArguments(args, debug, COMPSsConstants.Lang.valueOf(lang.toUpperCase()));

        String storageConf = (storageConfArg == null || storageConfArg.isEmpty()) ? "null" : storageConfArg;

        System.setProperty(COMPSsConstants.STORAGE_CONF, storageConf);
        if (storageConf != null && !storageConf.equals("null")) {
            try {
                StorageItf.init(storageConf);
            } catch (StorageException se) {
                ErrorManager.fatal(ERROR_STORAGE_CONF_INIT + storageConf, se);
            }
        }

        // Initialize GOSWorker
        GOSWorker worker = new GOSWorker(workerName, workingDir, debug, installDir, appDir, storageConf, streamBackend,
            streamMasterName, streamMasterPort, implDef.getComputingUnits(), implDef.getCPUMap(),
            implDef.getGPUComputingUnits(), implDef.getGPUMAp(), implDef.getFPGAUnits(), implDef.getFPGAMap(), tracing,
            tracingSlot, tracingParams, lang, langParams, envScriptPath);

        // Run task
        boolean success = worker.runTask(implDef);

        // Stop streaming if necessary
        if (!streamBackend.equals(StreamBackend.NONE)) {
            StopRequest stopRequest = new StopRequest();
            DistroStreamClient.request(stopRequest);
            stopRequest.waitProcessed();
            int errorCode = stopRequest.getErrorCode();
            if (errorCode != 0) {
                System.err.println(ERROR_STREAMING_FINISH);
                System.err.println("Error Code: " + errorCode);
                System.err.println("Error Message: " + stopRequest.getErrorMessage());
            }
        }

        // Stop storage if necessary
        if (storageConf != null && !storageConf.equals("null")) {
            try {
                StorageItf.finish();
            } catch (StorageException se) {
                System.err.println(ERROR_STORAGE_CONF_FINISH);
                se.printStackTrace(); // NOSONAR need to be printed in job out/err
            }
        }

        // System exit if a failure was found while executing the task, normal exit otherwise
        if (!success) {
            System.exit(7);
        }
    }

    /**
     * Instantiates a new Gos worker.
     *
     * @param workerName the worker name
     * @param workingDir the working dir
     * @param debug the debug
     * @param installDir the install dir
     * @param appDir the app dir
     * @param storageConf the storage conf
     * @param streamBackend the stream backend
     * @param streamMasterName the stream master name
     * @param streamMasterPort the stream master port
     * @param computingUnitsCPU the computing units cpu
     * @param cpuMap the cpu map
     * @param computingUnitsGPU the computing units gpu
     * @param gpuMap the gpu map
     * @param computingUnitsFPGA the computing units fpga
     * @param fpgaMap the fpga map
     * @param tracing the tracing
     * @param tracingSlot the tracing slot
     * @param tracingParams the tracing Params
     * @param lang lang
     * @param langParams the lang params
     */
    public GOSWorker(String workerName, String workingDir, boolean debug, String installDir, String appDir,
        String storageConf, StreamBackend streamBackend, String streamMasterName, int streamMasterPort,
        int computingUnitsCPU, String cpuMap, int computingUnitsGPU, String gpuMap, int computingUnitsFPGA,
        String fpgaMap, boolean tracing, int tracingSlot, String[] tracingParams, String lang,
        LanguageParams[] langParams, String envScriptPath) {

        this.hostName = workerName;
        this.workingDir = workingDir;
        this.debug = debug;
        this.installDir = installDir;
        this.appDir = appDir;
        this.storageConf = storageConf;
        this.streamBackend = streamBackend;
        this.streamMasterName = streamMasterName;
        this.streamMasterPort = streamMasterPort;
        this.tracing = tracing;
        this.tracingParams = tracingParams;
        this.tracingSlot = tracingSlot;
        this.lang = COMPSsConstants.Lang.valueOf(lang.toUpperCase());

        this.langParams = langParams;

        this.envScriptPath = envScriptPath;

        // Prepare execution Manager
        this.executionManager = new ExecutionManager(this, computingUnitsCPU, cpuMap, false, computingUnitsGPU, gpuMap,
            computingUnitsFPGA, fpgaMap, 0, 1);

        if (debug) {
            System.out.println("Initializing ExecutionManager");
        }
        try {
            this.executionManager.init();
        } catch (InitializationException ie) {
            ErrorManager.error(EXECUTION_MANAGER_ERR, ie);
        }

    }

    private static GOSInvocation parseArguments(String[] args, boolean debug, Lang lang) {
        // Default flags
        int argPosition = DEFAULT_FLAGS_SIZE;
        MethodType methodType = MethodType.valueOf(args[argPosition++]);
        switch (methodType) {
            case METHOD:
                return genImplemenationDefinition(new MethodDefinition(args, argPosition), debug, args,
                    argPosition + MethodDefinition.NUM_PARAMS, lang);
            case BINARY:
                return genImplemenationDefinition(new BinaryDefinition(args, argPosition, null), debug, args,
                    argPosition + BinaryDefinition.NUM_PARAMS, lang);
            // why there is no MPMD MPI case?
            case MPI:
                return genImplemenationDefinition(new MPIDefinition(args, argPosition), debug, args,
                    argPosition + MPIDefinition.NUM_PARAMS, lang);
            case COMPSs:
                return genImplemenationDefinition(new COMPSsDefinition(args, argPosition), debug, args,
                    argPosition + COMPSsDefinition.NUM_PARAMS, lang);
            case DECAF:
                return genImplemenationDefinition(new DecafDefinition(args, argPosition), debug, args,
                    argPosition + DecafDefinition.NUM_PARAMS, lang);
            case MULTI_NODE:
                return genImplemenationDefinition(new MultiNodeDefinition(args, argPosition), debug, args,
                    argPosition + MultiNodeDefinition.NUM_PARAMS, lang);
            case OMPSS:
                return genImplemenationDefinition(new OmpSsDefinition(args, argPosition), debug, args,
                    argPosition + OmpSsDefinition.NUM_PARAMS, lang);
            case OPENCL:
                return genImplemenationDefinition(new OpenCLDefinition(args, argPosition), debug, args,
                    argPosition + OpenCLDefinition.NUM_PARAMS, lang);
            case PYTHON_MPI:
                PythonMPIDefinition pyMPIDef = new PythonMPIDefinition(args, argPosition);
                return genImplemenationDefinition(pyMPIDef, debug, args,
                    argPosition + PythonMPIDefinition.NUM_PARAMS + pyMPIDef.getCollectionLayouts().length * 4, lang);
            case CONTAINER:
                return genImplemenationDefinition(new ContainerDefinition(args, argPosition), debug, args,
                    argPosition + ContainerDefinition.NUM_PARAMS, lang);
        }
        // If we reach this point means that the methodType was unrecognized
        ErrorManager.error(WARN_UNSUPPORTED_METHOD_TYPE + methodType);
        return null;
    }

    private static GOSInvocation genImplemenationDefinition(AbstractMethodImplementationDefinition implDef,
        boolean debug, String[] args, int argPosition, Lang lang) {
        ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition> implDesc =
            new ImplementationDescription<>(implDef, "", false, null, null, null);
        AbstractMethodImplementation impl = new AbstractMethodImplementation(0, 0, implDesc);
        System.out.println("Implementation:" + impl.toString());
        return new GOSInvocation(debug, lang, impl, args, argPosition);
    }

    @Override
    public String getHostName() {
        return this.hostName;
    }

    @Override
    public long getTracingHostID() {
        return Integer.parseInt(tracingParams[tracingParams.length - 1]);
    }

    @Override
    public String getAppDir() {
        return this.appDir;
    }

    @Override
    public String getInstallDir() {
        return this.installDir;
    }

    @Override
    public String getWorkingDir() {
        return this.workingDir;
    }

    @Override
    public String getLogDir() {
        return this.workingDir + "log/";
    }

    @Override
    public String getAnalysisDir() {
        // This decides where the files generated by the worker that are not log are stored
        return this.workingDir + "analysis/";
    }

    @Override
    public PrintStream getThreadOutStream() {
        return System.out;
    }

    @Override
    public PrintStream getThreadErrStream() {
        return System.err;
    }

    @Override
    public COMPSsConstants.TaskExecution getExecutionType() {
        return COMPSsConstants.TaskExecution.COMPSS;
    }

    @Override
    public boolean isPersistentCEnabled() {
        return false;
    }

    @Override
    public LanguageParams getLanguageParams(COMPSsConstants.Lang lang) {
        return this.langParams[lang.ordinal()];
    }

    @Override
    public void registerOutputs(String outputsBasename) {
        // Do nothing. It uses the stdout and stderr
    }

    @Override
    public void unregisterOutputs() {
        // Do nothing.
    }

    @Override
    public String getStandardStreamsPath(Invocation invocation) {
        return this.getWorkingDir() + "jobs" + File.separator + "job" + invocation.getJobId() + "_"
            + invocation.getHistory();
    }

    @Override
    public String getStorageConf() {
        return this.storageConf;
    }

    @Override
    public StreamBackend getStreamingBackend() {
        return this.streamBackend;
    }

    @Override
    public String getStreamingMasterName() {
        return this.streamMasterName;
    }

    @Override
    public int getStreamingMasterPort() {
        return this.streamMasterPort;
    }

    @Override
    public void loadParam(InvocationParam np) throws UnloadableValueException {
        switch (np.getType()) {
            case OBJECT_T:
            case STREAM_T:
                String fileLocation = (String) np.getValue();
                np.setOriginalName(fileLocation);
                try {
                    Object o = Serializer.deserialize(fileLocation);
                    np.setValue(o);
                } catch (ClassNotFoundException | IOException e) {
                    throw new UnloadableValueException(e);
                }
                break;
            case PSCO_T: // fetch stage already set the value on the param, but we make sure to collect the last version
                String pscoId = (String) np.getValue();
                try {
                    StorageItf.getByID(pscoId);
                } catch (StorageException se) {
                    throw new UnloadableValueException(se);
                }
                break;
            case FILE_T: // value already contains the path
            case EXTERNAL_STREAM_T: // value already contains the path
            case BINDING_OBJECT_T: // value corresponds to the ID of the object on the binding (already set)
            case EXTERNAL_PSCO_T: // value corresponds to the ID of the
                break;
            default:
                // Nothing to do since basic type parameters require no action
        }
    }

    @Override
    public void storeParam(InvocationParam np, boolean createifNonExistent)
        throws UnwritableValueException, NonExistentDataException {

        switch (np.getType()) {
            case FILE_T:
                String filepath = (String) np.getValue();
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(TraceEvent.CHECK_OUT_PARAM);
                }
                File f = new File(filepath);
                boolean fExists = f.exists();
                if (Tracer.isActivated()) {
                    Tracer.emitEventEnd(TraceEvent.CHECK_OUT_PARAM);
                }
                if (!fExists) {
                    if (createifNonExistent) {
                        System.out.println("Creating new blank file at " + filepath);
                        try {
                            f.createNewFile(); // NOSONAR ignoring result. It couldn't exists.
                        } catch (IOException e) {
                            if (this.debug) {
                                System.err.println("ERROR creating new blank file at " + filepath);
                            }
                            throw new UnwritableValueException(e);
                        }
                    }
                    throw new NonExistentDataException(filepath);
                }
                break;
            case OBJECT_T:
            case STREAM_T:
                String fileLocation = np.getOriginalName();
                System.out.println("Storing parameter " + np.getName() + " in " + fileLocation);
                try {
                    Serializer.serialize(np.getValue(), fileLocation);
                } catch (IOException ioe) {
                    throw new UnwritableValueException(ioe);
                }
                break;
            case PSCO_T: // fetch stage already set the value on the param, but we make sure to collect the last version
                throw new UnsupportedOperationException("Output PSCOs are not suported with the GAT adaptor");
            case EXTERNAL_STREAM_T: // value already contains the path
            case BINDING_OBJECT_T: // value corresponds to the ID of the object on the binding (already set)
            case EXTERNAL_PSCO_T: // value corresponds to the ID of the
                break;
            default:
                // Nothing to do since basic type parameters require no action
        }
    }

    private boolean runTask(GOSInvocation task) {
        // Execute the job
        final ExecutionEnd status = new ExecutionEnd();
        final Semaphore sem = new Semaphore(0);
        InvocationExecutionRequest.Listener listener = new InvocationExecutionRequest.Listener() {

            @Override
            public void onResultAvailable(InvocationParam param) {
                // Ignore. Results are not notified until end of execution.
            }

            @Override
            public void notifyEnd(Invocation invocation, boolean success, COMPSsException e) {
                status.setSuccess(success);
                sem.release();
            }
        };
        InvocationExecutionRequest e = new InvocationExecutionRequest(task, listener);
        this.executionManager.enqueue(e);

        // Wait for completion
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        // Stop and log execution result
        this.executionManager.stop();
        return status.getSuccess();
    }

    @Override
    public COMPSsRuntime getRuntimeAPI() {
        return null;
    }

    @Override
    public LoaderAPI getLoaderAPI() {
        return null;
    }

    @Override
    public void idleReservedResourcesDetected(ResourceDescription resources) {
        // GOS Adaptor does not support remote resource updates
    }

    @Override
    public void reactivatedReservedResourcesDetected(ResourceDescription resources) {
        // GOS Adaptor does not support remote resource updates
    }

    @Override
    public String getEnvironmentScript() {
        return envScriptPath;
    }

    @Override
    public boolean getEar() {
        return false;
    }

    @Override
    public boolean getDataProvenance() {
        return false;
    }
}
