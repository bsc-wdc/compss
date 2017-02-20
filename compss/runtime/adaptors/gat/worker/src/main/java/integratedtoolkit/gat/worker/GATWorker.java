package integratedtoolkit.gat.worker;

import integratedtoolkit.ITConstants;

import integratedtoolkit.gat.worker.utils.Invokers;

import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.annotations.parameter.Stream;

import integratedtoolkit.types.implementations.AbstractMethodImplementation.MethodType;

import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Serializer;
import integratedtoolkit.util.Tracer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
    private static boolean debug;
    private static String storageConf;

    private static int numNodes;
    private static Set<String> hostnames;
    private static int cus;

    private static MethodType methodType;
    private static String[] methodDefinition;
    private static boolean hasTarget;
    private static boolean hasReturn;
    private static int numParams;
    private static int initialAppParamsPosition;

    private static Class<?> types[];
    private static Stream streams[];
    private static String prefixes[];
    private static Object values[];
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
     * @param args
     *            args for the execution: arg[0]: boolean enable debug arg[1]: String with Storage configuration arg[2]:
     *            Number of nodes for multi-node tasks (N) arg[3,N]: N strings with multi-node hostnames arg[3+N+1]:
     *            Number of computing units arg[3+N+2]: Method type (M=3+N+2) arg[M,M - M+1]: Method dependant
     *            parameters Others
     * 
     */
    private static void parseArguments(String args[]) {
        // Default flags
        GATWorker.taskSandboxWorkingDir = new File(args[0]);
        GATWorker.debug = Boolean.valueOf(args[1]);
        GATWorker.storageConf = args[2];

        int argPosition = DEFAULT_FLAGS_SIZE;
        GATWorker.methodType = MethodType.valueOf(args[argPosition++]);
        GATWorker.methodDefinition = null;
        switch (GATWorker.methodType) {
            case METHOD:
                // classname, methodname
                GATWorker.methodDefinition = new String[] { args[argPosition], args[argPosition + 1] };
                argPosition += 2;
                break;
            case MPI:
                // mpiRunner, mpiBinary
                GATWorker.methodDefinition = new String[] { args[argPosition], args[argPosition + 1] };
                argPosition += 2;
                break;
            case OMPSS:
                // binary
                GATWorker.methodDefinition = new String[] { args[argPosition] };
                argPosition += 1;
                break;
            case OPENCL:
                // kernel
                GATWorker.methodDefinition = new String[] { args[argPosition] };
                argPosition += 1;
                break;
            case BINARY:
                // binary
                GATWorker.methodDefinition = new String[] { args[argPosition] };
                argPosition += 1;
                break;
        }

        // Execution information for multi-node tasks
        GATWorker.numNodes = Integer.parseInt(args[argPosition++]);
        GATWorker.hostnames = new HashSet<String>();
        for (int i = 0; i < numNodes; ++i) {
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

        // Get application number of parameters
        GATWorker.numParams = Integer.parseInt(args[argPosition++]);
        GATWorker.initialAppParamsPosition = argPosition;

        // Check received arguments
        if (args.length < 2 * GATWorker.numParams + GATWorker.initialAppParamsPosition) {
            ErrorManager.error(ERROR_APP_PARAMETERS);
        }

        // Check if we must enable the storage
        System.setProperty(ITConstants.IT_STORAGE_CONF, GATWorker.storageConf);
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
     * @param args
     *            arg[L]: boolean is the method executed on a certain instance arg[L]: integer amount of parameters of
     *            the method arg[L+]: parameters of the method For each parameter: type: 0-10 (file, boolean, char,
     *            string, byte, short, int, long, float, double, object) [substrings: amount of substrings (only used
     *            when the type is string)] value: value for the parameter or the file where it is contained (for
     *            objects and files) [Direction: R/W (only used when the type is object)]
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
        DataType[] dataTypes = DataType.values();
        Stream[] dataStream = Stream.values();
        int argPosition = GATWorker.initialAppParamsPosition;
        for (int i = 0; i < GATWorker.numParams; i++) {
            // We need to use wrapper classes for basic types, reflection will unwrap automatically
            int argType_index = Integer.parseInt(args[argPosition]);
            if (argType_index >= dataTypes.length) {
                ErrorManager.error(WARN_UNSUPPORTED_TYPE + argType_index);
            }
            DataType argType = dataTypes[argType_index];
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
            for (String info : GATWorker.methodDefinition) {
                sb.append(info).append(" ");
            }
            ErrorManager.error(sb.toString());
        }

        // Check retrieved object
        if (o == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("Object with renaming ").append(renaming);
            sb.append(", at");
            for (String info : GATWorker.methodDefinition) {
                sb.append(info).append(" ");
            }
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
            for (String info : GATWorker.methodDefinition) {
                sb.append(info).append(" ");
            }
            ErrorManager.error(sb.toString());
            return;
        }

        // Check retrieved id
        if (id == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("PSCO Id with renaming ").append(renaming);
            sb.append(", at");
            for (String info : GATWorker.methodDefinition) {
                sb.append(info).append(" ");
            }
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
            for (String info : GATWorker.methodDefinition) {
                sb.append(info).append(" ");
            }
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
            for (String info : GATWorker.methodDefinition) {
                sb.append(info).append(" ");
            }
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
        for (int j = 0; j < GATWorker.methodDefinition.length; ++j) {
            System.out.println("  * Method Description " + j + ": " + GATWorker.methodDefinition[j]);
        }

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
            System.out.println("  * Has return with renaming " + GATWorker.retRenaming);
        } else {
            System.out.println("  * Has NO return");
        }
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
        for (Iterator<String> it = GATWorker.hostnames.iterator(); it.hasNext();) {
            String nodeName = it.next();
            if (firstElement) {
                firstElement = false;
                hostnamesSTR.append(nodeName);
            } else {
                hostnamesSTR.append(",").append(nodeName);
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

        GATWorker.retValue = null;

        switch (GATWorker.methodType) {
            case METHOD:
                GATWorker.retValue = Invokers.invokeJavaMethod(GATWorker.methodDefinition[0], GATWorker.methodDefinition[1],
                        GATWorker.target, GATWorker.types, GATWorker.values);
                break;
            case MPI:
                GATWorker.retValue = Invokers.invokeMPIMethod(GATWorker.methodDefinition[0], GATWorker.methodDefinition[1],
                        GATWorker.target, GATWorker.values, GATWorker.hasReturn, GATWorker.streams, GATWorker.prefixes,
                        GATWorker.taskSandboxWorkingDir);
                break;
            case OMPSS:
                GATWorker.retValue = Invokers.invokeOmpSsMethod(GATWorker.methodDefinition[0], GATWorker.target, GATWorker.values,
                        GATWorker.hasReturn, GATWorker.streams, GATWorker.prefixes, GATWorker.taskSandboxWorkingDir);
                break;
            case OPENCL:
                GATWorker.retValue = Invokers.invokeOpenCLMethod(GATWorker.methodDefinition[0], GATWorker.target, GATWorker.values,
                        GATWorker.hasReturn, GATWorker.streams, GATWorker.prefixes, GATWorker.taskSandboxWorkingDir);
                break;
            case BINARY:
                GATWorker.retValue = Invokers.invokeBinaryMethod(GATWorker.methodDefinition[0], GATWorker.target, GATWorker.values,
                        GATWorker.hasReturn, GATWorker.streams, GATWorker.prefixes, GATWorker.taskSandboxWorkingDir);
                break;
        }

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
                    for (String info : GATWorker.methodDefinition) {
                        errMsg.append(info).append(" ");
                    }
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
                for (String info : GATWorker.methodDefinition) {
                    errMsg.append(info).append(" ");
                }
                ErrorManager.warn(errMsg.toString());
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
                    for (String info : GATWorker.methodDefinition) {
                        errMsg.append(info).append(" ");
                    }
                    ErrorManager.warn(errMsg.toString());
                    allOutFilesCreated = false;
                }
            }
        }

        if (!allOutFilesCreated) {
            StringBuilder errMsg = new StringBuilder();
            errMsg.append(ERROR_OUTPUT_FILES);
            for (String info : GATWorker.methodDefinition) {
                errMsg.append(info).append(" ");
            }
            errMsg.append("'");
            ErrorManager.error(errMsg.toString());
        }
    }

}
