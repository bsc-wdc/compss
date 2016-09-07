package integratedtoolkit.gat.worker;

import integratedtoolkit.ITConstants;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Serializer;
import integratedtoolkit.util.Tracer;

import java.io.File;
import java.lang.reflect.Method;

import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


/**
 * The worker class is executed on the remote resources in order to execute the tasks.
 */
public class GATWorker {

    protected static final int NUM_HEADER_PARS = 6;

    private static final String WARN_UNSUPPORTED_TYPE = "WARNING: Unsupported data type";


    /**
     * Executes a method taking into account the parameters. First it parses the parameters assigning values and
     * deserializing Read/creating empty ones for Write. Invokes the desired method by reflection and serializes all the
     * objects that has been modified and the result.
     *
     * @param args
     *            args for the execution: 
     *            arg[0]: boolean enable debug 
     *            arg[1]: String implementing core class name
     *            arg[2]: String core method name 
     *            arg[3]: boolean is the method executed on a certain instance 
     *            arg[4]: integer amount of parameters of the method 
     *            arg[5+]: parameters of the method For each parameter: type:
     *            0-10 (file, boolean, char, string, byte, short, int, long, float, double, object) [substrings: amount
     *            of substrings (only used when the type is string)] value: value for the parameter or the file where it
     *            is contained (for objects and files) [Direction: R/W (only used when the type is object)]
     */
    public static void main(String args[]) {
        boolean debug = Boolean.valueOf(args[0]);
        String storageConf = args[1];
        String className = args[2];
        String methodName = args[3];
        boolean hasTarget = Boolean.parseBoolean(args[4]);
        int numParams = Integer.parseInt(args[5]);

        // Check received arguments
        if (args.length < 2 * numParams + NUM_HEADER_PARS) {
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
        int pos = NUM_HEADER_PARS;
        Object target = null;
        DataType[] dataTypes = DataType.values();
        for (int i = 0; i < numParams; i++) {
            // We need to use wrapper classes for basic types, reflection will unwrap automatically
            int argType_index = Integer.parseInt(args[pos]);
            if (argType_index >= dataTypes.length) {
                ErrorManager.error(WARN_UNSUPPORTED_TYPE + argType_index);
            }
            DataType argType = DataType.values()[argType_index];
            switch (argType) {
                case FILE_T:
                    types[i] = String.class;
                    values[i] = args[pos + 1];
                    break;
                case OBJECT_T:
                    renamings[i] = (String) args[pos + 1];
                    mustWrite[i] = ((String) args[pos + 2]).equals("W");

                    String renaming = renamings[i];
                    Object o = null;
                    try {
                        o = Serializer.deserialize(renaming);
                    } catch (Exception e) {
                        ErrorManager.error("Error deserializing object parameter " + i + " with renaming " + renaming + ", method "
                                + methodName + ", class " + className);
                    }

                    // Check retrieved object
                    if (o == null) {
                        ErrorManager.error(
                                "Object with renaming " + renaming + ", method " + methodName + ", class " + className + " is null!");
                    }

                    // Store retrieved object
                    if (hasTarget && i == numParams - 1) { // last parameter is the target object
                        target = o;
                    } else {
                        types[i] = o.getClass();
                        values[i] = o;
                    }
                    pos++;
                    break;
                case PSCO_T:
                    renamings[i] = (String) args[pos + 1];
                    mustWrite[i] = ((String) args[pos + 2]).equals("W");

                    renaming = renamings[i];
                    String id = null;
                    try {
                        id = (String) Serializer.deserialize(renaming);
                    } catch (Exception e) {
                        ErrorManager.error("Error deserializing PSCO id parameter " + i + " with renaming " + renaming + ", method "
                                + methodName + ", class " + className);
                    }

                    // Check retrieved id
                    if (id == null) {
                        ErrorManager.error(
                                "PSCO Id with renaming " + renaming + ", method " + methodName + ", class " + className + " is null!");
                    }

                    Object obj = null;
                    if (Tracer.isActivated()) {
                        Tracer.emitEvent(Tracer.Event.STORAGE_GETBYID.getId(), Tracer.Event.STORAGE_GETBYID.getType());
                    }
                    try {
                        obj = StorageItf.getByID(id);
                    } catch (StorageException e) {
                        ErrorManager.error("Cannot getByID parameter " + i + " with PSCOId " + id 
                                + ", method " + methodName + ", class " + className, e);
                    } finally {
                        if (Tracer.isActivated()) {
                            Tracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_GETBYID.getType());
                        }
                    }

                    // Check retrieved object
                    if (obj == null) {
                        ErrorManager.error("PSCO with id " + id + ", method " + methodName + ", class " + className + " is null!");
                    }

                    // Store retrieved object
                    if (hasTarget && i == numParams - 1) { // last parameter is the target object
                        target = obj;
                    } else {
                        types[i] = obj.getClass();
                        values[i] = obj;
                    }
                    pos++;
                    break;
                case BOOLEAN_T:
                    types[i] = boolean.class;
                    values[i] = new Boolean(args[pos + 1]);
                    break;
                case CHAR_T:
                    types[i] = char.class;
                    values[i] = new Character(args[pos + 1].charAt(0));
                    break;
                case STRING_T:
                    types[i] = String.class;
                    int numSubStrings = Integer.parseInt(args[pos + 1]);
                    String aux = "";
                    for (int j = 2; j <= numSubStrings + 1; j++) {
                        aux += args[pos + j];
                        if (j < numSubStrings + 1) {
                            aux += " ";
                        }
                    }
                    values[i] = aux;
                    pos += numSubStrings;
                    break;
                case BYTE_T:
                    types[i] = byte.class;
                    values[i] = new Byte(args[pos + 1]);
                    break;
                case SHORT_T:
                    types[i] = short.class;
                    values[i] = new Short(args[pos + 1]);
                    break;
                case INT_T:
                    types[i] = int.class;
                    values[i] = new Integer(args[pos + 1]);
                    break;
                case LONG_T:
                    types[i] = long.class;
                    values[i] = new Long(args[pos + 1]);
                    break;
                case FLOAT_T:
                    types[i] = float.class;
                    values[i] = new Float(args[pos + 1]);
                    break;
                case DOUBLE_T:
                    types[i] = double.class;
                    values[i] = new Double(args[pos + 1]);
                    break;
                default:
                    ErrorManager.error(WARN_UNSUPPORTED_TYPE + argType);
                    break;
            }
            isFile[i] = argType.equals(DataType.FILE_T);
            pos += 2;
        }

        if (debug) {
            // Print request information
            System.out.println("WORKER - Parameters of execution:");
            System.out.println("  * Method class: " + className);
            System.out.println("  * Method name: " + methodName);
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
            ErrorManager.error("Error invoking requested method");
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
                    String errMsg = "ERROR: File with path '" + values[i] + "' has not been generated by task '" + methodName + "'"
                            + "' (in class '" + className + "' at method '" + methodName + "', parameter number: " + (i + 1) + " )";
                    ErrorManager.warn(errMsg);
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
                    ErrorManager.error("Error serializing object parameter " + i + " with renaming " + renamings[i] + ", method "
                            + methodName + ", class " + className);
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
            String renaming = (String) args[pos + 1];
            try {
                Serializer.serialize(retValue, renaming);
            } catch (Exception e) {
                ErrorManager.error("Error serializing object return value " + "with renaming " + renaming + ", method " + methodName
                        + ", class " + className);
            }
        }

        if (!allOutFilesCreated) {
            ErrorManager.error("ERROR: One or more OUT files have not been created by task '" + methodName + "'");
        }

        // Stop the storage if needed
        // WARN: Currently the master does and its no needed
        /*
         * if (storageConf != null && !storageConf.equals("") && !storageConf.equals("null")) { try {
         * StorageItf.finish(); } catch (StorageException e) { ErrorManager.fatal("Error releasing storage library: " +
         * e.getMessage()); } }
         */
    }

}
