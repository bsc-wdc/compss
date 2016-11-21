package integratedtoolkit.gat.worker;

import integratedtoolkit.ITConstants;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.implementations.AbstractMethodImplementation.MethodType;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Serializer;
import integratedtoolkit.util.Tracer;
import integratedtoolkit.worker.invokers.GenericInvoker;
import integratedtoolkit.worker.invokers.InvokeExecutionException;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;

import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


/**
 * The worker class is executed on the remote resources in order to execute the tasks.
 */
public class GATWorker {

    private static final String WARN_UNSUPPORTED_TYPE = "WARNING: Unsupported data type";
    private static final String ERROR_INVOKE = "Error invoking requested method";


    /**
     * Executes a method taking into account the parameters. First it parses the parameters assigning values and
     * deserializing Read/creating empty ones for Write. Invokes the desired method by reflection and serializes all the
     * objects that has been modified and the result.
     *
     * @param args
     *            args for the execution: 
     *            arg[0]: boolean enable debug 
     *            arg[1]: String with Storage configuration
     *            arg[2]: Method type
     *            arg[3,3-4]: Method dependant parameters
     *            arg[5]: boolean is the method executed on a certain instance 
     *            arg[6]: integer amount of parameters of the method 
     *            arg[7+]: parameters of the method For each parameter: type:
     *            0-10 (file, boolean, char, string, byte, short, int, long, float, double, object) [substrings: amount
     *            of substrings (only used when the type is string)] value: value for the parameter or the file where it
     *            is contained (for objects and files) [Direction: R/W (only used when the type is object)]
     */
    public static void main(String args[]) {        
        boolean debug = Boolean.valueOf(args[0]);
        String storageConf = args[1];
        
        MethodType methodType = MethodType.valueOf(args[2]);
        String[] methodDefinition = null;
        int argPosition = 3;
        switch(methodType) {
            case METHOD:
                // classname, methodname
                methodDefinition = new String[] { args[3], args[4] };
                argPosition += 2;
                break;
            case MPI:
                // mpiRunner, mpiBinary
                methodDefinition = new String[] { args[3], args[4] };
                argPosition += 2;
                break;
            case OMPSS:
                // binary
                methodDefinition = new String[] { args[3] };
                argPosition += 1;
                break;
            case OPENCL:
                // kernel
                methodDefinition = new String[] { args[3] };
                argPosition += 1;
                break;
            case BINARY:
                // binary
                methodDefinition = new String[] { args[3] };
                argPosition += 1;
                break;
        }

        boolean hasTarget = Boolean.parseBoolean(args[argPosition++]);
        int numParams = Integer.parseInt(args[argPosition++]);

        // Check received arguments
        if (args.length < 2 * numParams + argPosition) {
            ErrorManager.error("Incorrect number of parameters");
        }

        // Check if we must enable the storage
        System.setProperty(ITConstants.IT_STORAGE_CONF, storageConf);
        if (storageConf != null && !storageConf.equals("") && !storageConf.equals("null")) {
            try {
                StorageItf.init(storageConf);
            } catch (StorageException e) {
                ErrorManager.fatal("Error loading storage configuration file: " + storageConf, e);
            }
        }

        // Variables
        Class<?> types[];
        Object values[];
        if (hasTarget) {
            // The target object of the last parameter before the return value (if any)
            types = new Class[numParams - 1];
            values = new Object[numParams - 1];
        } else {
            types = new Class[numParams];
            values = new Object[numParams];
        }
        boolean isFile[] = new boolean[numParams];
        boolean mustWrite[] = new boolean[numParams];
        String renamings[] = new String[numParams];

        // Parse the parameter types and values
        Object target = null;
        DataType[] dataTypes = DataType.values();
        for (int i = 0; i < numParams; i++) {
            // We need to use wrapper classes for basic types, reflection will unwrap automatically
            int argType_index = Integer.parseInt(args[argPosition]);
            if (argType_index >= dataTypes.length) {
                ErrorManager.error(WARN_UNSUPPORTED_TYPE + argType_index);
            }
            DataType argType = DataType.values()[argType_index];
            switch (argType) {
                case FILE_T:
                    types[i] = String.class;
                    values[i] = args[argPosition + 1];
                    break;
                case OBJECT_T:
                    renamings[i] = (String) args[argPosition + 1];
                    mustWrite[i] = ((String) args[argPosition + 2]).equals("W");

                    String renaming = renamings[i];
                    Object o = null;
                    try {
                        o = Serializer.deserialize(renaming);
                    } catch (Exception e) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Error deserializing object parameter ").append(i);
                        sb.append(" with renaming ").append(renaming);
                        sb.append(", at");
                        for (String info : methodDefinition) {
                            sb.append(info).append(" ");
                        }
                        ErrorManager.error(sb.toString());                        
                    }

                    // Check retrieved object
                    if (o == null) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Object with renaming ").append(renaming);
                        sb.append(", at");
                        for (String info : methodDefinition) {
                            sb.append(info).append(" ");
                        }
                        sb.append("is null!");
                        ErrorManager.error(sb.toString());
                        return;
                    }

                    // Store retrieved object
                    if (hasTarget && i == numParams - 1) { // last parameter is the target object
                        target = o;
                    } else {
                        types[i] = o.getClass();
                        values[i] = o;
                    }
                    argPosition++;
                    break;
                case PSCO_T:
                    renamings[i] = (String) args[argPosition + 1];
                    mustWrite[i] = ((String) args[argPosition + 2]).equals("W");

                    renaming = renamings[i];
                    String id = null;
                    try {
                        id = (String) Serializer.deserialize(renaming);
                    } catch (Exception e) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Error deserializing PSCO id parameter ").append(i);
                        sb.append(" with renaming ").append(renaming);
                        sb.append(", at");
                        for (String info : methodDefinition) {
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
                        for (String info : methodDefinition) {
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
                        sb.append("Cannot getByID parameter ").append(i);
                        sb.append(" with PSCOId ").append(id);
                        sb.append(", at");
                        for (String info : methodDefinition) {
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
                        for (String info : methodDefinition) {
                            sb.append(info).append(" ");
                        }
                        sb.append("is null!");
                        ErrorManager.error(sb.toString());
                        return;
                    }

                    // Store retrieved object
                    if (hasTarget && i == numParams - 1) { // last parameter is the target object
                        target = obj;
                    } else {
                        types[i] = obj.getClass();
                        values[i] = obj;
                    }
                    argPosition++;
                    break;
                case BOOLEAN_T:
                    types[i] = boolean.class;
                    values[i] = new Boolean(args[argPosition + 1]);
                    break;
                case CHAR_T:
                    types[i] = char.class;
                    values[i] = new Character(args[argPosition + 1].charAt(0));
                    break;
                case STRING_T:
                    types[i] = String.class;
                    int numSubStrings = Integer.parseInt(args[argPosition + 1]);
                    String aux = "";
                    for (int j = 2; j <= numSubStrings + 1; j++) {
                        aux += args[argPosition + j];
                        if (j < numSubStrings + 1) {
                            aux += " ";
                        }
                    }
                    values[i] = aux;
                    argPosition += numSubStrings;
                    break;
                case BYTE_T:
                    types[i] = byte.class;
                    values[i] = new Byte(args[argPosition + 1]);
                    break;
                case SHORT_T:
                    types[i] = short.class;
                    values[i] = new Short(args[argPosition + 1]);
                    break;
                case INT_T:
                    types[i] = int.class;
                    values[i] = new Integer(args[argPosition + 1]);
                    break;
                case LONG_T:
                    types[i] = long.class;
                    values[i] = new Long(args[argPosition + 1]);
                    break;
                case FLOAT_T:
                    types[i] = float.class;
                    values[i] = new Float(args[argPosition + 1]);
                    break;
                case DOUBLE_T:
                    types[i] = double.class;
                    values[i] = new Double(args[argPosition + 1]);
                    break;
                default:
                    ErrorManager.error(WARN_UNSUPPORTED_TYPE + argType);
                    return;
            }
            isFile[i] = argType.equals(DataType.FILE_T);
            argPosition += 2;
        }

        if (debug) {
            // Print request information
            System.out.println("WORKER - Parameters of execution:");
            for (int j = 0; j < methodDefinition.length; ++j) {
                System.out.println("  * Method Description " + j + ": " + methodDefinition[j]);
            }
            System.out.print("  * Parameter types:");
            for (Class<?> c : types) {
                System.out.print(" " + c.getName());
            }
            System.out.println("");
            System.out.print("  * Parameter values:");
            for (Object v : values) {
                System.out.print(" " + v);
            }
            System.out.println("");
        }
        
        // Set environment variables
        // TODO: Add useful values for MPI / OmpSs tasks
        String hostname = "localhost";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            ErrorManager.warn("Cannot obtain hostname. Loading default value " + hostname);
        }
        setEnvironmentVariables(hostname, 1, 1);
        
        // Invoke method depending on its type
        Object retValue = null;
        switch(methodType) {
            case METHOD:
                retValue = invokeJavaMethod(methodDefinition[0], methodDefinition[1], target, types, values);
                break;
            case MPI:
                retValue = invokeMPIMethod(methodDefinition[0], methodDefinition[1], target, types, values);
                break;
            case OMPSS:
                retValue = invokeOmpSsMethod(methodDefinition[0], target, types, values);
                break;
            case OPENCL:
                retValue = invokeOpenCLMethod(methodDefinition[0], target, types, values);
                break;
            case BINARY:
                retValue = invokeBinaryMethod(methodDefinition[0], target, types, values);
                break;
        }

        // Check if all the output files have been actually created (in case user has forgotten)
        // No need to distinguish between IN or OUT files, because IN files will
        // exist, and if there's one or more missing, they will be necessarily out.
        boolean allOutFilesCreated = true;

        for (int i = 0; i < numParams; i++) {
            if (isFile[i]) {
                String filepath = (String) values[i];
                File f = new File(filepath);
                if (!f.exists()) {
                    StringBuilder errMsg = new StringBuilder();
                    errMsg.append("ERROR: File with path '").append(values[i]).append("' has not been generated by task '");
                    for (String info : methodDefinition) {
                        errMsg.append(info).append(" ");
                    }
                    ErrorManager.warn(errMsg.toString());
                    allOutFilesCreated = false;
                }
            }
        }

        // ////////////////////////
        // Write to disk the updated object parameters, if any (including the target)
        for (int i = 0; i < numParams; i++) {
            if (mustWrite[i]) {
                try {
                    // Check if we must serialize a parameter or the target object
                    Object toSerialize = null;
                    if (hasTarget && i == numParams - 1) {
                        toSerialize = target;
                    } else {
                        toSerialize = values[i];
                    }

                    // Check if its a PSCO and it's persisted
                    if (toSerialize instanceof StubItf) {
                        String id = ((StubItf) toSerialize).getID();
                        if (id != null) {
                            toSerialize = id;
                        }
                    }

                    // Serialize
                    Serializer.serialize(toSerialize, renamings[i]);
                } catch (Exception e) {
                    StringBuilder errMsg = new StringBuilder();
                    errMsg.append("Error serializing object parameter ").append(i);
                    errMsg.append(" with renaming ").append(renamings[i]);
                    errMsg.append(", at ");
                    for (String info : methodDefinition) {
                        errMsg.append(info).append(" ");
                    }
                    ErrorManager.warn(errMsg.toString());
                }
            }
        }

        // Serialize the return value if existing
        if (retValue != null) {
            // If the retValue is a PSCO and it is persisted, we only send the ID
            // Otherwise we treat the PSCO as a normal object
            if (retValue instanceof StubItf) {
                String id = ((StubItf) retValue).getID();
                if (id != null) {
                    retValue = id;
                }
            }

            // Serialize return value to its location
            String renaming = (String) args[argPosition + 1];
            try {
                Serializer.serialize(retValue, renaming);
            } catch (Exception e) {
                StringBuilder errMsg = new StringBuilder();
                errMsg.append("Error serializing object return value with renaming ").append(renaming);
                errMsg.append(", at ");
                for (String info : methodDefinition) {
                    errMsg.append(info).append(" ");
                }
                ErrorManager.warn(errMsg.toString());
            }
        }

        if (!allOutFilesCreated) {
            StringBuilder errMsg = new StringBuilder();
            errMsg.append("ERROR: One or more OUT files have not been created by task '");
            for (String info : methodDefinition) {
                errMsg.append(info).append(" ");
            }
            errMsg.append("'");
            ErrorManager.warn(errMsg.toString());            
        }

        // Stop the storage if needed
        // WARN: Currently the master does and its no needed
        /*
         * if (storageConf != null && !storageConf.equals("") && !storageConf.equals("null")) { try {
         * StorageItf.finish(); } catch (StorageException e) { ErrorManager.fatal("Error releasing storage library: " +
         * e.getMessage()); } }
         */
    }
    
    private static void setEnvironmentVariables(String hostnames, int numNodes, int cus) {
        System.setProperty(Constants.COMPSS_HOSTNAMES, hostnames);
        System.setProperty(Constants.COMPSS_NUM_NODES, String.valueOf(numNodes));
        System.setProperty(Constants.COMPSS_NUM_THREADS, String.valueOf(cus));
    }
    
    private static Object invokeJavaMethod(String className, String methodName, Object target, Class<?>[] types, Object[] values) {
        // Use reflection to get the requested method
        Method method = null;
        try {
            Class<?> methodClass = Class.forName(className);
            method = methodClass.getMethod(methodName, types);
        } catch (ClassNotFoundException e) {
            ErrorManager.error("Application class not found");
        } catch (SecurityException e) {
            ErrorManager.error("Security exception");
        } catch (NoSuchMethodException e) {
            ErrorManager.error("Requested method not found");
        }

        // Invoke the requested method
        Object retValue = null;
        try {
            retValue = method.invoke(target, values);
        } catch (Exception e) {
            ErrorManager.error(ERROR_INVOKE, e);
        }
        
        return retValue;
    }
    
    private static Object invokeMPIMethod(String mpiRunner, String mpiBinary, Object target, Class<?>[] types, Object[] values) {
        Object retValue = null;
        try {
            retValue = GenericInvoker.invokeMPIMethod(mpiRunner, mpiBinary, values);
        } catch(InvokeExecutionException iee) {
            ErrorManager.error(ERROR_INVOKE, iee);
        }
        
        return retValue;
    }

    private static Object invokeOmpSsMethod(String ompssBinary, Object target, Class<?>[] types, Object[] values) {
        Object retValue = null;
        try {
            retValue = GenericInvoker.invokeOmpSsMethod(ompssBinary, values);
        } catch(InvokeExecutionException iee) {
            ErrorManager.error(ERROR_INVOKE, iee);
        }
        
        return retValue;
    }

    private static Object invokeOpenCLMethod(String kernel, Object target, Class<?>[] types, Object[] values) {
        ErrorManager.error("ERROR: OpenCL is not supported");

        return null;
    }

    private static Object invokeBinaryMethod(String binary, Object target, Class<?>[] types, Object[] values) {
        Object retValue = null;
        try {
            retValue = GenericInvoker.invokeBinaryMethod(binary, values);
        } catch(InvokeExecutionException iee) {
            ErrorManager.error(ERROR_INVOKE, iee);
        }
        
        return retValue;
    }

}
