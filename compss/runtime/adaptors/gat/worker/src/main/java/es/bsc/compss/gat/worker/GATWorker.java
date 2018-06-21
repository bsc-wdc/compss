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
import es.bsc.compss.exceptions.JobExecutionException;
import es.bsc.compss.gat.worker.implementations.BinaryDefinition;
import es.bsc.compss.gat.worker.implementations.DecafDefinition;
import es.bsc.compss.gat.worker.implementations.MPIDefinition;
import es.bsc.compss.gat.worker.implementations.MethodDefinition;
import es.bsc.compss.gat.worker.implementations.OMPSsDefinition;
import es.bsc.compss.gat.worker.implementations.OpenCLDefinition;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Serializer;
import es.bsc.compss.util.Tracer;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import storage.StorageException;
import storage.StorageItf;


/**
 * The worker class is executed on the remote resources in order to execute the tasks.
 *
 */
public class GATWorker implements InvocationContext {
    
    private static final String WARN_UNSUPPORTED_METHOD_TYPE = "WARNING: Unsupported method type";
    private static final String WARN_UNSUPPORTED_DATA_TYPE = "WARNING: Unsupported data type";
    private static final String WARN_UNSUPPORTED_STREAM = "WARNING: Unsupported data stream";
    private static final String ERROR_APP_PARAMETERS = "ERROR: Incorrect number of parameters";
    private static final String ERROR_STORAGE_CONF = "ERROR: Cannot load storage configuration file: ";
    private static final String ERROR_SERIALIZE_RETURN = "Error serializing object return value with renaming ";
    private static final String ERROR_OUTPUT_FILES = "ERROR: One or more OUT files have not been created by task '";

    //FLAGS IDX
    private static final int DEFAULT_FLAGS_SIZE = 5;
    private static final int WORKING_DIR_IDX = 0;
    private static final int DEBUG_IDX = 1;
    private static final int INSTALL_DIR_IDX = 2;
    private static final int APP_DIR_IDX = 3;
    private static final int STORAGE_CONF_IDX = 4;
    
    private final boolean debug;
    private final String appDir;
    private final String installDir;
    private final File sandBoxDir;
    
    private final ImplementationDefinition implDef;

    /**
     * Executes a method taking into account the parameters. First it parses the parameters assigning values and
     * deserializing Read/creating empty ones for Write. Invokes the desired method by reflection. and serializes all
     * the objects that has been modified and the result.
     *
     * @param args
     * @throws java.lang.Exception
     */
    public static void main(String args[]) throws Exception {
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
        
        GATWorker worker = new GATWorker(args);
        worker.runTask();
    }
    
    public GATWorker(String[] args) {
        sandBoxDir = new File(args[WORKING_DIR_IDX]);
        debug = Boolean.valueOf(args[DEBUG_IDX]);
        installDir = args[INSTALL_DIR_IDX];
        appDir = args[APP_DIR_IDX];

        // Retrieve arguments
        implDef = parseArguments(args);
    }
    
    public void runTask() throws JobExecutionException {
        System.out.println("[JAVA EXECUTOR] executeTask - Begin task execution");
        try {
            Invoker invoker = implDef.getInvoker(this, debug, sandBoxDir);
            invoker.processTask();
        } catch (JobExecutionException jee) {
            System.out.println("[JAVA EXECUTOR] executeTask - Error in task execution");
            System.err.println("[JAVA EXECUTOR] executeTask - Error in task execution");
            jee.printStackTrace();
            throw jee;
        } finally {
            System.out.println("[JAVA EXECUTOR] executeTask - End task execution");
        }

        // Post task execution
        checkOutputFiles();

        // We don't stop the storage because the master does it
    }

    /**
     * Parses the all the arguments except the application parameters
     *
     * @param args args for the execution: arg[0]: boolean enable debug arg[1]: String with Storage configuration
     * arg[2]: Number of nodes for multi-node tasks (N) arg[3,N]: N strings with multi-node hostnames arg[3+N+1]: Number
     * of computing units arg[3+N+2]: Method type (M=3+N+2) arg[M,M - M+1]: Method dependant parameters Others
     *
     */
    private ImplementationDefinition parseArguments(String args[]) {
        // Default flags
        int argPosition = DEFAULT_FLAGS_SIZE;
        MethodType methodType = MethodType.valueOf(args[argPosition++]);
        switch (methodType) {
            case METHOD:
                return new MethodDefinition(args, argPosition);
            case MPI:
                return new MPIDefinition(args, argPosition);
            case DECAF:
                return new DecafDefinition(args, argPosition);
            case OMPSS:
                return new OMPSsDefinition(args, argPosition);
            case OPENCL:
                return new OpenCLDefinition(args, argPosition);
            case BINARY:
                return new BinaryDefinition(args, argPosition);
            default:
                ErrorManager.error(WARN_UNSUPPORTED_METHOD_TYPE + methodType);
                return null;
            
        }
    }

    /**
     * Checks that all the output files have been generated
     *
     */
    private void checkOutputFiles() {
        // Check if all the output files have been actually created (in case user has forgotten)
        // No need to distinguish between IN or OUT files, because IN files will
        // exist, and if there's one or more missing, they will be necessarily out.
        boolean allOutFilesCreated = true;
        for (InvocationParam param : implDef.getParams()) {
            if (param.getType() == DataType.FILE_T) {
                String filepath = (String) param.getValue();
                File f = new File(filepath);
                if (!f.exists()) {
                    StringBuilder errMsg = new StringBuilder();
                    errMsg.append("ERROR: File with path '").append(param.getValue()).append("' has not been generated by task '");
                    errMsg.append(implDef.toCommandString());
                    ErrorManager.warn(errMsg.toString());
                    allOutFilesCreated = false;
                }
            }
        }
        
        if (!allOutFilesCreated) {
            StringBuilder errMsg = new StringBuilder();
            errMsg.append(ERROR_OUTPUT_FILES);
            errMsg.append(implDef.toCommandString());
            errMsg.append("'");
            ErrorManager.error(errMsg.toString());
        }
    }
    
    @Override
    public String getHostName() {
        return "localhost";
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
    public PrintStream getThreadOutStream() {
        return System.out;
    }
    
    @Override
    public PrintStream getThreadErrStream() {
        return System.err;
    }
    
    @Override
    public Object getObject(String renaming) {
        Object o = null;
        try {
            o = Serializer.deserialize(renaming);
        } catch (IOException | ClassNotFoundException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Error deserializing object parameter with renaming ").append(renaming);
            sb.append(", at");
            sb.append(implDef.toCommandString());
            ErrorManager.error(sb.toString());
        }

        // Check retrieved object
        if (o == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("Object with renaming ").append(renaming);
            sb.append(", at");
            sb.append(implDef.toCommandString());
            sb.append("is null!");
            ErrorManager.error(sb.toString());
        }
        return o;
    }
    
    @Override
    public Object getPersistentObject(String renaming) throws StorageException {
        String id = null;
        
        try {
            id = (String) Serializer.deserialize(renaming);
        } catch (IOException | ClassNotFoundException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Error deserializing PSCO id for parameter with renaming ").append(renaming);
            sb.append(", at");
            sb.append(implDef.toCommandString());
            ErrorManager.error(sb.toString());
            return null;
        }

        // Check retrieved id
        if (id == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("PSCO Id with renaming ").append(renaming);
            sb.append(", at");
            sb.append(implDef.toCommandString());
            sb.append("is null!");
            ErrorManager.error(sb.toString());
            return null;
        }
        
        Object obj = null;
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.STORAGE_GETBYID.getId(), Tracer.Event.STORAGE_GETBYID.getType());
        }
        try {
            obj = StorageItf.getByID(id);
        } catch (StorageException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Cannot getByID with PSCOId ").append(id);
            sb.append(", at");
            sb.append(implDef.toCommandString());
            ErrorManager.error(sb.toString());
            return null;
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_GETBYID.getType());
            }
        }

        // Check retrieved object
        if (obj == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("PSCO with id ").append(id);
            sb.append(", at");
            sb.append(implDef.toCommandString());
            sb.append("is null!");
            ErrorManager.error(sb.toString());
            return obj;
        }
        
        return obj;
    }
    
    @Override
    public void storeObject(String renaming, Object value) {
        try {
            Serializer.serialize(value, renaming);
            File f = new File(renaming);
            System.out.println(f.getCanonicalPath());
        } catch (Exception e) {
            StringBuilder errMsg = new StringBuilder();
            errMsg.append("Error serializing object with renaming ").append(renaming);
            errMsg.append(", at ");
            errMsg.append(implDef.toCommandString());
            ErrorManager.warn(errMsg.toString());
        }
    }
    
    @Override
    public void storePersistentObject(String id, Object value) {
        System.out.println("Aqui s'ha de persistir l'objecte " + value + "amb el id" + id);
    }
    
}
