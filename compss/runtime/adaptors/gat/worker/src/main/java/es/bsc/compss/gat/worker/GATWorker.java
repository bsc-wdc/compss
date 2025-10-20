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
package es.bsc.compss.gat.worker;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.COMPSsConstants.TaskExecution;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.gat.executor.types.ExecutionEnd;
import es.bsc.compss.loader.LoaderAPI;
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
import java.util.concurrent.Semaphore;

import storage.StorageException;
import storage.StorageItf;


/**
 * The worker class is executed on the remote resources in order to execute the tasks.
 */
public class GATWorker implements InvocationContext {

    private static final String ERROR_STREAMING_INIT = "ERROR: Cannot load Streaming Client";
    private static final String ERROR_STREAMING_FINISH = "ERROR: Cannot stop Streaming Client";

    private static final String ERROR_STORAGE_CONF_INIT = "ERROR: Cannot load storage configuration file: ";
    private static final String ERROR_STORAGE_CONF_FINISH = "ERROR: Cannot stop StorageItf";

    private static final String EXECUTION_MANAGER_ERR = "Error starting ExecutionManager";
    private static final String WARN_UNSUPPORTED_METHOD_TYPE = "WARNING: Unsupported method type";

    // FLAGS IDX
    private static final int DEFAULT_FLAGS_SIZE = 9;

    private static final int WORKER_NAME_IDX = 0;
    private static final int WORKING_DIR_IDX = 1;
    private static final int DEBUG_IDX = 2;
    private static final int INSTALL_DIR_IDX = 3;
    private static final int APP_DIR_IDX = 4;
    private static final int STORAGE_CONF_IDX = 5;
    private static final int STREAMING_IDX = 6;
    private static final int STREAMING_MASTER_IDX = 7;
    private static final int STREAMING_PORT_IDX = 8;

    // Internal components
    private final String hostName;
    private final boolean debug;
    private final String appDir;
    private final String installDir;
    private final String workingDir;
    private final String storageConf;
    private final StreamBackend streamBackend;
    private final String streamMasterName;
    private final int streamMasterPort;

    private final ExecutionManager executionManager;


    /**
     * Executes a method taking into account the parameters. First it parses the parameters assigning values and
     * deserializing Read/creating empty ones for Write. Invokes the desired method by reflection. and serializes all
     * the objects that has been modified and the result.
     *
     * @param args System arguments.
     * @throws Exception If any error occurs executing the worker or the task.
     */
    public static void main(String[] args) throws Exception {
        final String workerName = args[WORKER_NAME_IDX];
        final String workingDir = args[WORKING_DIR_IDX];
        final boolean debug = Boolean.valueOf(args[DEBUG_IDX]);
        final String installDir = args[INSTALL_DIR_IDX];
        final String appDir = args[APP_DIR_IDX];

        // Prepares the Loggers according to the worker debug parameter
        GATLog.init(debug);

        // Configures streaming if necessary
        String streamingArg = args[STREAMING_IDX];
        String streaming = (streamingArg == null || streamingArg.isEmpty() || streamingArg.equals("null")) ? "NONE"
            : streamingArg.toUpperCase();
        StreamBackend streamBackend = StreamBackend.valueOf(streaming);
        String streamMasterName = args[STREAMING_MASTER_IDX];
        int streamMasterPort = Integer.parseInt(args[STREAMING_PORT_IDX]);

        if (!streamBackend.equals(StreamBackend.NONE)) {
            try {
                DistroStreamClient.initAndStart(streamMasterName, streamMasterPort);
            } catch (DistroStreamClientInitException dscie) {
                ErrorManager.fatal(ERROR_STREAMING_INIT, dscie);
            }
        }

        // Configures storage API if necessary
        String storageConfArg = args[STORAGE_CONF_IDX];
        String storageConf =
            (storageConfArg == null || storageConfArg.isEmpty() || storageConfArg.equals("null")) ? "" : storageConfArg;

        System.setProperty(COMPSsConstants.STORAGE_CONF, storageConf);
        if (!storageConf.isEmpty()) {
            try {
                StorageItf.init(storageConf);
            } catch (StorageException se) {
                ErrorManager.fatal(ERROR_STORAGE_CONF_INIT + storageConf, se);
            }
        }

        // Retrieve task arguments
        GATInvocation implDef = parseArguments(args);

        // Initialize GAT Worker
        GATWorker worker = new GATWorker(workerName, workingDir, debug, installDir, appDir, storageConf, streamBackend,
            streamMasterName, streamMasterPort, implDef.getComputingUnits());

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
        if (!storageConf.isEmpty()) {
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
     * Creates a new GATWorker instance with the given parameters.
     * 
     * @param workerName Worker name.
     * @param workingDir Worker working directory.
     * @param debug Whether the debug mode is enabled or not.
     * @param installDir Worker installation directory.
     * @param appDir Application directory in the worker machine.
     * @param storageConf Path to the storage configuration.
     * @param streamBackend Streaming backend.
     * @param streamMasterName Streaming master name.
     * @param streamMasterPort Streaming master port.
     * @param computingUnitsCPU Number of worker computing units.
     */
    public GATWorker(String workerName, String workingDir, boolean debug, String installDir, String appDir,
        String storageConf, StreamBackend streamBackend, String streamMasterName, int streamMasterPort,
        int computingUnitsCPU) {

        this.hostName = workerName;
        this.debug = debug;
        this.appDir = appDir;
        this.installDir = installDir;
        this.workingDir = workingDir;
        this.storageConf = storageConf;
        this.streamBackend = streamBackend;
        this.streamMasterName = streamMasterName;
        this.streamMasterPort = streamMasterPort;

        // Prepare execution Manager
        this.executionManager = new ExecutionManager(this, computingUnitsCPU, ThreadBinder.BINDER_DISABLED, false, 0,
            ThreadBinder.BINDER_DISABLED, 0, ThreadBinder.BINDER_DISABLED, 0, 1);

        if (this.debug) {
            System.out.println("Initializing ExecutionManager");
        }
        try {
            this.executionManager.init();
        } catch (InitializationException ie) {
            ErrorManager.error(EXECUTION_MANAGER_ERR, ie);
        }
    }

    /**
     * Parses the all the arguments except the application parameters.
     *
     * @param args args for the execution: arg[0]: boolean enable debug arg[1]: String with Storage configuration
     *            arg[2]: Number of nodes for multi-node tasks (N) arg[3,N]: N strings with multi-node hostnames
     *            arg[3+N+1]: Number of computing units arg[3+N+2]: Method type (M=3+N+2) arg[M,M - M+1]: Method
     *            dependant parameters Others
     */
    private static GATInvocation parseArguments(String[] args) {
        // Default flags
        int argPosition = DEFAULT_FLAGS_SIZE;
        boolean debug = Boolean.valueOf(args[DEBUG_IDX]);
        MethodType methodType = MethodType.valueOf(args[argPosition++]);
        switch (methodType) {
            case METHOD:
                return genImplemenationDefinition(new MethodDefinition(args, argPosition), debug, args,
                    argPosition + MethodDefinition.NUM_PARAMS);
            case BINARY:
                return genImplemenationDefinition(new BinaryDefinition(args, argPosition, null), debug, args,
                    argPosition + BinaryDefinition.NUM_PARAMS);
            case MPI:
                return genImplemenationDefinition(new MPIDefinition(args, argPosition), debug, args,
                    argPosition + MPIDefinition.NUM_PARAMS);
            case COMPSs:
                return genImplemenationDefinition(new COMPSsDefinition(args, argPosition), debug, args,
                    argPosition + COMPSsDefinition.NUM_PARAMS);
            case DECAF:
                return genImplemenationDefinition(new DecafDefinition(args, argPosition), debug, args,
                    argPosition + DecafDefinition.NUM_PARAMS);
            case MULTI_NODE:
                return genImplemenationDefinition(new MultiNodeDefinition(args, argPosition), debug, args,
                    argPosition + MultiNodeDefinition.NUM_PARAMS);
            case OMPSS:
                return genImplemenationDefinition(new OmpSsDefinition(args, argPosition), debug, args,
                    argPosition + OmpSsDefinition.NUM_PARAMS);
            case OPENCL:
                return genImplemenationDefinition(new OpenCLDefinition(args, argPosition), debug, args,
                    argPosition + OpenCLDefinition.NUM_PARAMS);
            case PYTHON_MPI:
                PythonMPIDefinition pyMPIDef = new PythonMPIDefinition(args, argPosition);
                return genImplemenationDefinition(pyMPIDef, debug, args,
                    argPosition + PythonMPIDefinition.NUM_PARAMS + pyMPIDef.getCollectionLayouts().length * 4);
            case CONTAINER:
                return genImplemenationDefinition(new ContainerDefinition(args, argPosition), debug, args,
                    argPosition + ContainerDefinition.NUM_PARAMS);
        }
        // If we reach this point means that the methodType was unrecognized
        ErrorManager.error(WARN_UNSUPPORTED_METHOD_TYPE + methodType);
        return null;
    }

    private static GATInvocation genImplemenationDefinition(AbstractMethodImplementationDefinition implDef,
        boolean debug, String[] args, int argPosition) {
        ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition> implDesc =
            new ImplementationDescription<>(implDef, "", false, null, null, null);
        AbstractMethodImplementation impl = new AbstractMethodImplementation(0, 0, implDesc);
        return new GATInvocation(debug, impl, args, argPosition);
    }

    @Override
    public String getHostName() {
        return this.hostName;
    }

    @Override
    public long getTracingHostID() {
        return 0;
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
        return this.workingDir;
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
    public TaskExecution getExecutionType() {
        return TaskExecution.COMPSS;
    }

    @Override
    public boolean isPersistentCEnabled() {
        return false;
    }

    @Override
    public LanguageParams getLanguageParams(Lang language) {
        // Only Java methods are executed on this worker. No additional parameters required
        return null;
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
        return null;
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

    private boolean runTask(GATInvocation task) {
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
        // GAT Adaptor does not support remote resource updates
    }

    @Override
    public void reactivatedReservedResourcesDetected(ResourceDescription resources) {
        // GAT Adaptor does not support remote resource updates
    }

    @Override
    public String getEnvironmentScript() {
        return null;
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
