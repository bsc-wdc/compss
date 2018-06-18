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
import es.bsc.compss.gat.worker.implementations.BinaryDefinition;
import es.bsc.compss.gat.worker.implementations.DecafDefinition;
import es.bsc.compss.gat.worker.implementations.MPIDefinition;
import es.bsc.compss.gat.worker.implementations.MethodDefinition;
import es.bsc.compss.gat.worker.implementations.OMPSsDefinition;
import es.bsc.compss.gat.worker.implementations.OpenCLDefinition;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Serializer;
import es.bsc.compss.util.Tracer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


/**
 * The worker class is executed on the remote resources in order to execute the tasks.
 *
 */
public class GATWorker {

    private static final String WARN_UNSUPPORTED_TYPE = "WARNING: Unsupported data type";
    private static final String WARN_UNSUPPORTED_STREAM = "WARNING: Unsupported data stream";
    private static final String ERROR_APP_PARAMETERS = "ERROR: Incorrect number of parameters";
    private static final String ERROR_STORAGE_CONF = "ERROR: Cannot load storage configuration file: ";
    private static final String ERROR_SERIALIZE_RETURN = "Error serializing object return value with renaming ";
    private static final String ERROR_OUTPUT_FILES = "ERROR: One or more OUT files have not been created by task '";

    private static final int DEFAULT_FLAGS_SIZE = 3;

    private static File taskSandboxWorkingDir;
    public static boolean debug;
    private static String storageConf;

    private static int numNodes;
    private static List<String> hostnames;
    private static int cus;

    private static ImplementationDefinition implDef;
    private static boolean hasTarget;
    private static boolean hasReturn;
    private static int numReturns;
    private static int numParams;
    private static int initialAppParamsPosition;

    private static Class<?> types[];
    private static Object values[];
    private static Stream streams[];
    private static String prefixes[];
    private static boolean isFile[];
    private static boolean mustWrite[];
    private static String renamings[];
    private static Object target;
    private static String retRenaming;
    private static Object retValue;

    /**
     * Executes a method taking into account the parameters. First it parses the parameters assigning values and
     * deserializing Read/creating empty ones for Write. Invokes the desired method by reflection. and serializes all
     * the objects that has been modified and the result.
     *
     */
    public static void main(String args[]) {
        // Retrieve arguments
        parseArguments(args);
        parseApplicationParameters(args);

        // Log information
        if (GATWorker.debug) {
            logArguments();
        }

        // Set environment variables for MPI/OMPSs tasks
        setEnvironmentVariables();

        // Invoke method depending on its type
        invokeMethod();

        // Post task execution
        serializeResults();
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
    private static void parseArguments(String args[]) {
        // Default flags
        GATWorker.taskSandboxWorkingDir = new File(args[0]);
        GATWorker.debug = Boolean.valueOf(args[1]);
        GATWorker.storageConf = args[2];

        int argPosition = DEFAULT_FLAGS_SIZE;
        MethodType methodType = MethodType.valueOf(args[argPosition++]);
        GATWorker.implDef = null;
        switch (methodType) {
            case METHOD:
                // classname, methodname
                GATWorker.implDef = new MethodDefinition(args[argPosition], args[argPosition + 1]);
                argPosition += 2;
                break;
            case MPI:
                // mpiRunner, mpiBinary
                GATWorker.implDef = new MPIDefinition(args[argPosition], args[argPosition + 1]);
                argPosition += 2;
                break;
            case DECAF:
                GATWorker.implDef = new DecafDefinition(args[argPosition], args[argPosition + 1], args[argPosition + 2],
                        args[argPosition + 3], args[argPosition + 4]);
                argPosition += 5;
                break;
            case OMPSS:
                // binary
                GATWorker.implDef = new OMPSsDefinition(args[argPosition]);
                argPosition += 1;
                break;
            case OPENCL:
                // kernel
                GATWorker.implDef = new OpenCLDefinition(args[argPosition]);
                argPosition += 1;
                break;
            case BINARY:
                // binary
                GATWorker.implDef = new BinaryDefinition(args[argPosition]);
                argPosition += 1;
                break;
        }

        // Execution information for multi-node tasks
        GATWorker.numNodes = Integer.parseInt(args[argPosition++]);
        GATWorker.hostnames = new ArrayList<>();
        for (int i = 0; i < GATWorker.numNodes; ++i) {
            GATWorker.hostnames.add(args[argPosition++]);
        }
        GATWorker.cus = Integer.parseInt(args[argPosition++]);

        // Get if has target or not
        GATWorker.hasTarget = Boolean.parseBoolean(args[argPosition++]);

        // Get return type if specified
        String returnType = args[argPosition++];
        if (returnType == null || returnType.equals("null") || returnType.isEmpty()) {
            GATWorker.hasReturn = false;
        } else {
            GATWorker.hasReturn = true;
        }
        GATWorker.numReturns = Integer.parseInt(args[argPosition++]);

        // Get application number of parameters
        GATWorker.numParams = Integer.parseInt(args[argPosition++]);
        GATWorker.initialAppParamsPosition = argPosition;

        // Check received arguments
        if (args.length < 2 * GATWorker.numParams + GATWorker.initialAppParamsPosition) {
            ErrorManager.error(ERROR_APP_PARAMETERS);
        }

        // Check if we must enable the storage
        System.setProperty(COMPSsConstants.STORAGE_CONF, GATWorker.storageConf);
        if (GATWorker.storageConf != null && !GATWorker.storageConf.equals("") && !GATWorker.storageConf.equals("null")) {
            try {
                StorageItf.init(GATWorker.storageConf);
            } catch (StorageException e) {
                ErrorManager.fatal(ERROR_STORAGE_CONF + GATWorker.storageConf, e);
            }
        }
    }

    /**
     * Parses the application parameters
     *
     * @param args arg[L]: boolean is the method executed on a certain instance arg[L]: integer amount of parameters of
     * the method arg[L+]: parameters of the method For each parameter: type: 0-10 (file, boolean, char, string, byte,
     * short, int, long, float, double, object) [substrings: amount of substrings (only used when the type is string)]
     * value: value for the parameter or the file where it is contained (for objects and files) [Direction: R/W (only
     * used when the type is object)]
     */
    private static void parseApplicationParameters(String[] args) {
        // Variables
        if (GATWorker.hasTarget) {
            // The target object of the last parameter before the return value (if any)
            GATWorker.types = new Class[GATWorker.numParams - 1];
            GATWorker.values = new Object[GATWorker.numParams - 1];
        } else {
            GATWorker.types = new Class[GATWorker.numParams];
            GATWorker.values = new Object[GATWorker.numParams];
        }

        GATWorker.streams = new Stream[GATWorker.numParams];
        GATWorker.prefixes = new String[GATWorker.numParams];
        GATWorker.isFile = new boolean[GATWorker.numParams];
        GATWorker.mustWrite = new boolean[GATWorker.numParams];
        GATWorker.renamings = new String[GATWorker.numParams];

        // Parse the parameter types and values
        DataType[] dataTypesEnum = DataType.values();
        Stream[] dataStream = Stream.values();
        int argPosition = GATWorker.initialAppParamsPosition;
        for (int i = 0; i < GATWorker.numParams; i++) {
            // We need to use wrapper classes for basic types, reflection will unwrap automatically
            int argType_index = Integer.parseInt(args[argPosition]);
            if (argType_index >= dataTypesEnum.length) {
                ErrorManager.error(WARN_UNSUPPORTED_TYPE + argType_index);
            }
            DataType argType = dataTypesEnum[argType_index];
            argPosition++;

            int argStream_index = Integer.parseInt(args[argPosition]);
            if (argStream_index >= dataStream.length) {
                ErrorManager.error(WARN_UNSUPPORTED_STREAM + argStream_index);
            }
            GATWorker.streams[i] = dataStream[argStream_index];
            argPosition++;

            String prefix = args[argPosition];
            if (prefix == null || prefix.isEmpty()) {
                prefix = Constants.PREFIX_EMTPY;
            }
            GATWorker.prefixes[i] = prefix;
            argPosition++;

            switch (argType) {
                case FILE_T:
                    GATWorker.types[i] = String.class;
                    GATWorker.values[i] = args[argPosition++];
                    break;
                case OBJECT_T:
                    GATWorker.renamings[i] = (String) args[argPosition++];
                    GATWorker.mustWrite[i] = ((String) args[argPosition++]).equals("W");
                    retrieveObject(renamings[i], i);
                    break;
                case BINDING_OBJECT_T:
                    GATWorker.renamings[i] = (String) args[argPosition++];
                    GATWorker.mustWrite[i] = ((String) args[argPosition++]).equals("W");
                    retrieveBindingObject(renamings[i], i);
                    break;
                case PSCO_T:
                    GATWorker.renamings[i] = (String) args[argPosition++];
                    GATWorker.mustWrite[i] = ((String) args[argPosition++]).equals("W");
                    retrievePSCO(renamings[i], i);
                    break;
                case EXTERNAL_PSCO_T:
                    GATWorker.types[i] = String.class;
                    GATWorker.values[i] = args[argPosition++];
                    break;
                case BOOLEAN_T:
                    GATWorker.types[i] = boolean.class;
                    GATWorker.values[i] = new Boolean(args[argPosition++]);
                    break;
                case CHAR_T:
                    GATWorker.types[i] = char.class;
                    GATWorker.values[i] = new Character(args[argPosition++].charAt(0));
                    break;
                case STRING_T:
                    GATWorker.types[i] = String.class;
                    int numSubStrings = Integer.parseInt(args[argPosition++]);
                    String aux = "";
                    for (int j = 0; j < numSubStrings; j++) {
                        if (j != 0) {
                            aux += " ";
                        }
                        aux += args[argPosition++];
                    }
                    GATWorker.values[i] = aux;
                    break;
                case BYTE_T:
                    GATWorker.types[i] = byte.class;
                    GATWorker.values[i] = new Byte(args[argPosition++]);
                    break;
                case SHORT_T:
                    GATWorker.types[i] = short.class;
                    GATWorker.values[i] = new Short(args[argPosition++]);
                    break;
                case INT_T:
                    GATWorker.types[i] = int.class;
                    GATWorker.values[i] = new Integer(args[argPosition++]);
                    break;
                case LONG_T:
                    GATWorker.types[i] = long.class;
                    GATWorker.values[i] = new Long(args[argPosition++]);
                    break;
                case FLOAT_T:
                    GATWorker.types[i] = float.class;
                    GATWorker.values[i] = new Float(args[argPosition++]);
                    break;
                case DOUBLE_T:
                    GATWorker.types[i] = double.class;
                    GATWorker.values[i] = new Double(args[argPosition++]);
                    break;
                default:
                    ErrorManager.error(WARN_UNSUPPORTED_TYPE + argType);
                    return;
            }

            GATWorker.isFile[i] = argType.equals(DataType.FILE_T);
        }

        // Retrieve return renaming if existing
        if (GATWorker.hasReturn) {
            // +1 = StreamType, +2 = prefix, +3 = value
            GATWorker.retRenaming = args[argPosition + 3];
        }
    }

    private static void retrieveBindingObject(String string, int i) {
        // TODO: Add retreive binding object at

    }

    /**
     * Retrieves an object from its renaming
     *
     * @param renaming
     * @param position
     */
    private static void retrieveObject(String renaming, int position) {
        Object o = null;
        try {
            o = Serializer.deserialize(renaming);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Error deserializing object parameter ").append(position);
            sb.append(" with renaming ").append(renaming);
            sb.append(", at");
            sb.append(GATWorker.implDef.toCommandString());
            ErrorManager.error(sb.toString());
        }

        // Check retrieved object
        if (o == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("Object with renaming ").append(renaming);
            sb.append(", at");
            sb.append(GATWorker.implDef.toCommandString());
            sb.append("is null!");
            ErrorManager.error(sb.toString());
            return;
        }

        // Store retrieved object
        if (GATWorker.hasTarget && position == GATWorker.numParams - 1) { // last parameter is the target object
            GATWorker.target = o;
        } else {
            GATWorker.types[position] = o.getClass();
            GATWorker.values[position] = o;
        }
    }

    /**
     * Retrieves a PSCO from its renaming
     *
     * @param renaming
     * @param position
     */
    private static void retrievePSCO(String renaming, int position) {
        String id = null;
        try {
            id = (String) Serializer.deserialize(renaming);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Error deserializing PSCO id parameter ").append(position);
            sb.append(" with renaming ").append(renaming);
            sb.append(", at");
            sb.append(GATWorker.implDef.toCommandString());
            ErrorManager.error(sb.toString());
            return;
        }

        // Check retrieved id
        if (id == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("PSCO Id with renaming ").append(renaming);
            sb.append(", at");
            sb.append(GATWorker.implDef.toCommandString());
            sb.append("is null!");
            ErrorManager.error(sb.toString());
            return;
        }

        Object obj = null;
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.STORAGE_GETBYID.getId(), Tracer.Event.STORAGE_GETBYID.getType());
        }
        try {
            obj = StorageItf.getByID(id);
        } catch (StorageException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Cannot getByID parameter ").append(position);
            sb.append(" with PSCOId ").append(id);
            sb.append(", at");
            sb.append(GATWorker.implDef.toCommandString());
            ErrorManager.error(sb.toString());
            return;
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
            sb.append(GATWorker.implDef.toCommandString());
            sb.append("is null!");
            ErrorManager.error(sb.toString());
            return;
        }

        // Store retrieved object
        if (GATWorker.hasTarget && position == GATWorker.numParams - 1) { // last parameter is the target object
            GATWorker.target = obj;
        } else {
            GATWorker.types[position] = obj.getClass();
            GATWorker.values[position] = obj;
        }
    }

    /**
     * Logs the parsed arguments
     *
     */
    private static void logArguments() {
        // Print arguments information
        System.out.println("");
        System.out.println("[GAT WORKER] ------------------------------------");
        System.out.println("[GAT WORKER] Parameters of execution:");
        System.out.println("  * Method type: " + GATWorker.implDef.getType().toString());
        System.out.println("  * Method definition: " + printMethodDefinition());

        System.out.print("  * Parameter types:");
        for (Class<?> c : GATWorker.types) {
            System.out.print(" " + c.getName());
        }
        System.out.println("");

        System.out.print("  * Parameter values:");
        for (Object v : GATWorker.values) {
            System.out.print(" " + v);
        }
        System.out.println("");

        System.out.print("  * Parameter streams:");
        for (Stream s : GATWorker.streams) {
            System.out.print(" " + s.name());
        }
        System.out.println("");

        if (GATWorker.hasReturn) {
            System.out.println("  * Has " + String.valueOf(GATWorker.numReturns) + " return with renaming " + GATWorker.retRenaming);
        } else {
            System.out.println("  * Has NO return");
        }
    }

    private static String printMethodDefinition() {
        String methodDefStr = null;
        if (GATWorker.implDef != null) {
            methodDefStr = GATWorker.implDef.toLogString();
        } else {
            methodDefStr = new String();
        }
        return methodDefStr;
    }

    /**
     * Sets MPI / OMPSs environment variables
     *
     */
    private static void setEnvironmentVariables() {
        String hostname = "localhost";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            ErrorManager.warn("Cannot obtain hostname. Loading default value " + hostname);
        }
        GATWorker.hostnames.add(hostname);
        ++GATWorker.numNodes;

        boolean firstElement = true;
        StringBuilder hostnamesSTR = new StringBuilder();
        for (String nodeName : GATWorker.hostnames) {
            if (nodeName.endsWith("-ib0")) {
                nodeName = nodeName.substring(0, nodeName.lastIndexOf("-ib0"));
            }

            // Add one host name per process to launch
            if (firstElement) {
                firstElement = false;
                hostnamesSTR.append(nodeName);
                for (int i = 1; i < GATWorker.cus; ++i) {
                    hostnamesSTR.append(",").append(nodeName);
                }
            } else {
                for (int i = 0; i < GATWorker.cus; ++i) {
                    hostnamesSTR.append(",").append(nodeName);
                }
            }
        }

        if (GATWorker.debug) {
            System.out.println("  * HOSTNAMES: " + hostnamesSTR.toString());
            System.out.println("  * NUM_NODES: " + GATWorker.numNodes);
            System.out.println("  * CPU_COMPUTING_UNITS: " + GATWorker.cus);
        }

        System.setProperty(Constants.COMPSS_HOSTNAMES, hostnamesSTR.toString());
        System.setProperty(Constants.COMPSS_NUM_NODES, String.valueOf(GATWorker.numNodes));
        System.setProperty(Constants.COMPSS_NUM_THREADS, String.valueOf(GATWorker.cus));
    }

    /**
     * Invokes the task method by reflection depending on the invoker type
     *
     */
    private static void invokeMethod() {
        System.out.println("");
        System.out.println("[GAT WORKER] ------------------------------------");
        System.out.println("[GAT WORKER] Invoking task method");
        GATWorker.retValue = GATWorker.implDef.process(target, types, values, isFile, streams, prefixes, taskSandboxWorkingDir);
        System.out.println("");
        System.out.println("[GAT WORKER] ------------------------------------");
    }

    /**
     * Serializes the required results produced by the task
     *
     */
    private static void serializeResults() {
        // Write to disk the updated object parameters, if any (including the target)
        for (int i = 0; i < GATWorker.numParams; i++) {
            if (GATWorker.mustWrite[i]) {
                try {
                    // Check if we must serialize a parameter or the target object
                    Object toSerialize = null;
                    if (GATWorker.hasTarget && i == GATWorker.numParams - 1) {
                        toSerialize = GATWorker.target;
                    } else {
                        toSerialize = GATWorker.values[i];
                    }

                    // Check if its a PSCO and it's persisted
                    if (toSerialize instanceof StubItf) {
                        String id = ((StubItf) toSerialize).getID();
                        if (id != null) {
                            toSerialize = id;
                        }
                    }

                    // Serialize
                    Serializer.serialize(toSerialize, GATWorker.renamings[i]);
                } catch (Exception e) {
                    StringBuilder errMsg = new StringBuilder();
                    errMsg.append("Error serializing object parameter ").append(i);
                    errMsg.append(" with renaming ").append(GATWorker.renamings[i]);
                    errMsg.append(", at ");
                    errMsg.append(GATWorker.implDef.toCommandString());
                    ErrorManager.warn(errMsg.toString());
                }
            }
        }

        // Serialize the return value if existing
        if (GATWorker.hasReturn && GATWorker.retValue != null) {
            // If the retValue is a PSCO and it is persisted, we only send the ID
            // Otherwise we treat the PSCO as a normal object
            if (GATWorker.retValue instanceof StubItf) {
                String id = ((StubItf) GATWorker.retValue).getID();
                if (id != null) {
                    GATWorker.retValue = id;
                }
            }

            // Serialize return value to its location
            try {
                Serializer.serialize(GATWorker.retValue, GATWorker.retRenaming);
            } catch (IOException ioe) {
                StringBuilder errMsg = new StringBuilder();
                errMsg.append(ERROR_SERIALIZE_RETURN).append(GATWorker.retRenaming);
                errMsg.append(", at ");
                errMsg.append(GATWorker.implDef.toCommandString());
                ErrorManager.warn(errMsg.toString());
            }
        }
    }

    /**
     * Serializes the binary exit value when required
     *
     */
    public static void serializeBinaryExitValue() {
        System.out.println("Checking binary exit value serialization");

        boolean isFile = GATWorker.isFile[GATWorker.isFile.length - 1];
        String lastParamPrefix = GATWorker.prefixes[GATWorker.prefixes.length - 1];
        String lastParamName = (String) GATWorker.values[GATWorker.values.length - 1];
        if (GATWorker.debug) {
            System.out.println("- Param isFile: " + isFile);
            System.out.println("- Prefix: " + lastParamPrefix);
        }

        // Last parameter is a FILE with skip prefix => return in Python
        // We cannot check it is OUT direction in GAT
        if (isFile && lastParamPrefix.equals(Constants.PREFIX_SKIP)) {
            // Write exit value to the file
            System.out.println("Writing Binary Exit Value (" + GATWorker.retValue.toString() + ") to " + lastParamName);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(lastParamName))) {
                String value = "0000I" + GATWorker.retValue.toString() + "\n.\n";
                writer.write(value);
                writer.flush();
            } catch (IOException ioe) {
                System.err.println("ERROR: Cannot serialize binary exit value for bindings");
                ioe.printStackTrace();
            }
        }
    }

    /**
     * Checks that all the output files have been generated
     *
     */
    private static void checkOutputFiles() {
        // Check if all the output files have been actually created (in case user has forgotten)
        // No need to distinguish between IN or OUT files, because IN files will
        // exist, and if there's one or more missing, they will be necessarily out.
        boolean allOutFilesCreated = true;
        for (int i = 0; i < GATWorker.numParams; i++) {
            if (GATWorker.isFile[i]) {
                String filepath = (String) GATWorker.values[i];
                File f = new File(filepath);
                if (!f.exists()) {
                    StringBuilder errMsg = new StringBuilder();
                    errMsg.append("ERROR: File with path '").append(GATWorker.values[i]).append("' has not been generated by task '");
                    errMsg.append(GATWorker.implDef.toCommandString());
                    ErrorManager.warn(errMsg.toString());
                    allOutFilesCreated = false;
                }
            }
        }

        if (!allOutFilesCreated) {
            StringBuilder errMsg = new StringBuilder();
            errMsg.append(ERROR_OUTPUT_FILES);
            errMsg.append(GATWorker.implDef.toCommandString());
            errMsg.append("'");
            ErrorManager.error(errMsg.toString());
        }
    }

}
