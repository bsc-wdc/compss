/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.executor.ExecutionManager;
import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.types.ExecutionListener;
import es.bsc.compss.gat.worker.implementations.JavaMethodDefinition;
import es.bsc.compss.gat.executor.types.ExecutionEnd;
import es.bsc.compss.gat.worker.implementations.BinaryDefinition;
import es.bsc.compss.gat.worker.implementations.MPIDefinition;
import es.bsc.compss.gat.worker.implementations.COMPSsDefinition;
import es.bsc.compss.gat.worker.implementations.DecafDefinition;
import es.bsc.compss.gat.worker.implementations.OMPSsDefinition;
import es.bsc.compss.gat.worker.implementations.OpenCLDefinition;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Serializer;
import java.io.PrintStream;
import java.util.concurrent.Semaphore;

import storage.StorageException;
import storage.StorageItf;


/**
 * The worker class is executed on the remote resources in order to execute the tasks.
 *
 */
public class GATWorker implements InvocationContext {

    private static final String EXECUTION_MANAGER_ERR = "Error starting ExecutionManager";
    private static final String WARN_UNSUPPORTED_METHOD_TYPE = "WARNING: Unsupported method type";
    private static final String ERROR_STORAGE_CONF = "ERROR: Cannot load storage configuration file: ";

    // FLAGS IDX
    private static final int DEFAULT_FLAGS_SIZE = 6;
    private static final int WORKER_NAME_IDX = 0;
    private static final int WORKING_DIR_IDX = 1;
    private static final int DEBUG_IDX = 2;
    private static final int INSTALL_DIR_IDX = 3;
    private static final int APP_DIR_IDX = 4;
    private static final int STORAGE_CONF_IDX = 5;

    private final String hostName;
    private final boolean debug;
    private final String appDir;
    private final String installDir;
    private final String libPath;
    private final String workingDir;
    private final String storageConf;

    // Internal components
    private final ExecutionManager executionManager;


    /**
     * Executes a method taking into account the parameters. First it parses the parameters assigning values and
     * deserializing Read/creating empty ones for Write. Invokes the desired method by reflection. and serializes all
     * the objects that has been modified and the result.
     *
     * @param args
     * @throws java.lang.Exception
     */
    public static void main(String args[]) throws Exception {
        String workerName = args[WORKER_NAME_IDX];
        String workingDir = args[WORKING_DIR_IDX];
        boolean debug = Boolean.valueOf(args[DEBUG_IDX]);
        // Prepares the Loggers according to the worker debug parameter
        GATLog.init(debug);

        String installDir = args[INSTALL_DIR_IDX];
        String appDir = args[APP_DIR_IDX];
        String libPath = "";

        // Configures storage API, if necessary
        String storageConf = args[STORAGE_CONF_IDX];
        // Check if we must enable the storage
        System.setProperty(COMPSsConstants.STORAGE_CONF, storageConf);
        if (storageConf != null && !storageConf.equals("") && !storageConf.equals("null")) {
            try {
                StorageItf.init(storageConf);
            } catch (StorageException e) {
                ErrorManager.fatal(ERROR_STORAGE_CONF + storageConf, e);
            }
        }

        // Retrieve arguments
        ImplementationDefinition implDef = parseArguments(args);

        GATWorker worker = new GATWorker(workerName, workingDir, debug, installDir, appDir, storageConf, libPath,
                implDef.getComputingUnits());
        if (!worker.runTask(implDef)) {
            System.exit(7);
        }
    }

    public GATWorker(String workerName, String workingDir, boolean debug, String installDir, String appDir, String storageConf,
            String libPath, int computingUnitsCPU) {

        this.hostName = workerName;
        this.debug = debug;
        this.appDir = appDir;
        this.installDir = installDir;
        this.libPath = libPath;
        this.workingDir = workingDir;
        this.storageConf = storageConf;

        // Prepare execution Manager
        this.executionManager = new ExecutionManager(this, computingUnitsCPU, ThreadBinder.BINDER_DISABLED, 0, ThreadBinder.BINDER_DISABLED,
                0, ThreadBinder.BINDER_DISABLED, 1);

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
     * Parses the all the arguments except the application parameters
     *
     * @param args
     *            args for the execution: arg[0]: boolean enable debug arg[1]: String with Storage configuration arg[2]:
     *            Number of nodes for multi-node tasks (N) arg[3,N]: N strings with multi-node hostnames arg[3+N+1]:
     *            Number of computing units arg[3+N+2]: Method type (M=3+N+2) arg[M,M - M+1]: Method dependant
     *            parameters Others
     *
     */
    private static ImplementationDefinition parseArguments(String args[]) {
        // Default flags
        int argPosition = DEFAULT_FLAGS_SIZE;
        boolean debug = Boolean.valueOf(args[DEBUG_IDX]);
        MethodType methodType = MethodType.valueOf(args[argPosition++]);
        switch (methodType) {
            case METHOD:
                return new JavaMethodDefinition(debug, args, argPosition);
            case BINARY:
                return new BinaryDefinition(debug, args, argPosition);
            case MPI:
                return new MPIDefinition(debug, args, argPosition);
            case COMPSs:
                return new COMPSsDefinition(debug, args, argPosition);
            case DECAF:
                return new DecafDefinition(debug, args, argPosition);
            case OMPSS:
                return new OMPSsDefinition(debug, args, argPosition);
            case OPENCL:
                return new OpenCLDefinition(debug, args, argPosition);
        }
        // If we reach this point means that the methodType was unrecognised
        ErrorManager.error(WARN_UNSUPPORTED_METHOD_TYPE + methodType);
        return null;
    }

    @Override
    public String getHostName() {
        return hostName;
    }

    @Override
    public long getTracingHostID() {
        return 0;
    }

    @Override
    public String getAppDir() {
        return appDir;
    }

    @Override
    public String getInstallDir() {
        return installDir;
    }

    @Override
    public String getLibPath() {
        return this.libPath;
    }

    @Override
    public String getWorkingDir() {
        return this.workingDir;
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
    public boolean isPersistentEnabled() {
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
    public void loadParam(InvocationParam np) throws Exception {
        switch (np.getType()) {
            case OBJECT_T:
                String fileLocation = (String) np.getValue();
                np.setOriginalName(fileLocation);
                np.setValue(Serializer.deserialize(fileLocation));
                break;
            case PSCO_T: // fetch stage already set the value on the param, but we make sure to collect the last version
                String pscoId = (String) np.getValue();
                StorageItf.getByID(pscoId);
                break;
            case FILE_T: // value already contains the path
            case BINDING_OBJECT_T: // value corresponds to the ID of the object on the binding (already set)
            case EXTERNAL_PSCO_T: // value corresponds to the ID of the
                break;
            default:
                // Nothing to do since basic type parameters require no action
        }
    }

    @Override
    public void storeParam(InvocationParam np) throws Exception {
        switch (np.getType()) {
            case OBJECT_T:
                String fileLocation = np.getOriginalName();
                Serializer.serialize(np.getValue(), fileLocation);
                break;
            case PSCO_T: // fetch stage already set the value on the param, but we make sure to collect the last version
                throw new UnsupportedOperationException("Output PSCOs are not suported with the GAT adaptor");
            case FILE_T: // value already contains the path
            case BINDING_OBJECT_T: // value corresponds to the ID of the object on the binding (already set)
            case EXTERNAL_PSCO_T: // value corresponds to the ID of the
                break;
            default:
                // Nothing to do since basic type parameters require no action
        }
    }

    private boolean runTask(ImplementationDefinition task) {
        // Execute the job
        final ExecutionEnd status = new ExecutionEnd();
        final Semaphore sem = new Semaphore(0);
        Execution e = new Execution(task, new ExecutionListener() {

            @Override
            public void notifyEnd(Invocation invocation, boolean success) {
                status.setSuccess(success);
                sem.release();
            }
        });
        this.executionManager.enqueue(e);

        // Wait for completion
        try {
            sem.acquire();
        } catch (InterruptedException ex) {

        }

        // Stop and log execution result
        this.executionManager.stop();
        return status.getSuccess();
    }

}
