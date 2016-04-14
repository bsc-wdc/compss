package integratedtoolkit.gat.worker;

import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Serializer;

import java.io.File;
import java.lang.reflect.Method;


/**
 * The worker class is executed on the remote resources in order to execute the
 * tasks.
 */
public class GATWorker {

    
    protected static final int NUM_HEADER_PARS = 5;
    
    // Parameter type constants
    protected static final int FILE_PAR = 0;
    protected static final int BOOL_PAR = 1;
    protected static final int CHAR_PAR = 2;
    protected static final int STRING_PAR = 3;
    protected static final int BYTE_PAR = 4;
    protected static final int SHORT_PAR = 5;
    protected static final int INT_PAR = 6;
    protected static final int LONG_PAR = 7;
    protected static final int FLOAT_PAR = 8;
    protected static final int DOUBLE_PAR = 9;
    protected static final int OBJECT_PAR = 10;
    
    /**
     * Executes a method taking into account the parameters. First it parses the
     * parameters assigning values and deserializing Read/creating empty ones
     * for Write. Invokes the desired method by reflection and serializes all the
     * objects that has been modified and the result.
     *
     * @param args args for the execution: arg[0]: boolean enable debug arg[1]:
     * String implementing core class name arg[2]: String core method name
     * arg[3]: boolean is the method executed on a certain instance arg[4]:
     * integer amount of parameters of the method arg[5+]: parameters of the
     * method For each parameter: type: 0-10 (file, boolean, char, string, byte,
     * short, int, long, float, double, object) [substrings: amount of
     * substrings (only used when the type is string)] value: value for the
     * parameter or the file where it is contained (for objects and files)
     * [Direction: R/W (only used when the type is object)]
     */
    public static void main(String args[]) {
        boolean debug = Boolean.parseBoolean(args[0]);
        String className = args[1];
        String methodName = args[2];
        boolean hasTarget = Boolean.parseBoolean(args[3]);
        int numParams = Integer.parseInt(args[4]);

        if (args.length < 2 * numParams + NUM_HEADER_PARS) {
        	ErrorManager.error("Application class not found");
        }

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
        for (int i = 0; i < numParams; i++) {
            // We need to use wrapper classes for basic types, reflection will unwrap automatically
        	int argType = Integer.parseInt(args[pos]);
            switch (argType) {
                case FILE_PAR:
                    types[i] = String.class;
                    values[i] = args[pos + 1];
                    break;
                case OBJECT_PAR:
                    String renaming = renamings[i] = (String) args[pos + 1];
                    mustWrite[i] = ((String) args[pos + 2]).equals("W");
                    Object o = null;
                    try {
                        o = Serializer.deserialize(renaming);
                    } catch (Exception e) {
                    	ErrorManager.error("Error deserializing object parameter " + i + " with renaming " + renaming + ", method " + methodName + ", class " + className);
                    }
                    if (hasTarget && i == numParams - 1) // last parameter is the target object
                    {
                        if (o == null) {
                        	ErrorManager.error("Target object with renaming " + renaming + ", method " + methodName + ", class " + className + " is null!");
                        }
                        target = o;
                    } else {
                        if (o == null) {
                        	ErrorManager.error("Object parameter " + i + " with renaming " + renaming + ", method " + methodName + ", class " + className + " is null!");
                        }
                        types[i] = o.getClass();
                        values[i] = o;
                    }
                    pos++;
                    break;
                case BOOL_PAR:
                    types[i] = boolean.class;
                    values[i] = new Boolean(args[pos + 1]);
                    break;
                case CHAR_PAR:
                    types[i] = char.class;
                    values[i] = new Character(args[pos + 1].charAt(0));
                    break;
                case STRING_PAR:
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
                case BYTE_PAR:
                    types[i] = byte.class;
                    values[i] = new Byte(args[pos + 1]);
                    break;
                case SHORT_PAR:
                    types[i] = short.class;
                    values[i] = new Short(args[pos + 1]);
                    break;
                case INT_PAR:
                    types[i] = int.class;
                    values[i] = new Integer(args[pos + 1]);
                    break;
                case LONG_PAR:
                    types[i] = long.class;
                    values[i] = new Long(args[pos + 1]);
                    break;
                case FLOAT_PAR:
                    types[i] = float.class;
                    values[i] = new Float(args[pos + 1]);
                    break;
                case DOUBLE_PAR:
                    types[i] = double.class;
                    values[i] = new Double(args[pos + 1]);
                    break;
            }
            isFile[i] = (argType == FILE_PAR);
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
        
        //Check if all the output files have been actually created (in case user has forgotten)
        //No need to distinguish between IN or OUT files, because IN files will exist, and if there's one or more missing,
        //they will be necessarily out.
        boolean allOutFilesCreated = true;

        for (int i = 0; i < numParams; i++) {
        	if(isFile[i]) {
        		String filepath = (String) values[i];
        		File f = new File(filepath);
        		if(!f.exists()) {
        			String errMsg = "ERROR: File with path '" + values[i] + 
								    "' has not been generated by task '" + methodName + "'" + 
		        					"' (in class '" + className + "' at method '" + 
        							 methodName + "', parameter number: " + (i+1) + " )";
        			System.out.println(errMsg);
        			allOutFilesCreated = false;
        		}
        	}
        }
        //////////////////////////
        
        // Write to disk the updated object parameters, if any (including the target)
        for (int i = 0; i < numParams; i++) {
            if (mustWrite[i]) {
                try {
                    if (hasTarget && i == numParams - 1) {
                        Serializer.serialize(target, renamings[i]);
                    } else {
                        Serializer.serialize(values[i], renamings[i]);
                    }
                } catch (Exception e) {
                	ErrorManager.error("Error serializing object parameter " +
                			 		   i + " with renaming " + renamings[i] +
                			 		   ", method " + methodName +
                			 		   ", class " + className);
                }
            }
        }
        
        // Serialize the return value if existing
        if (retValue != null) {
            String renaming = (String) args[pos + 1];
            try {
                Serializer.serialize(retValue, renaming);
            } catch (Exception e) {
            	ErrorManager.error("Error serializing object return value " +
            					   "with renaming " + renaming + 
            					   ", method " + methodName + 
            					   ", class " + className);
            }
        }
        
        if(!allOutFilesCreated) {
        	ErrorManager.error("ERROR: One or more OUT files have not been created by task '" + methodName + "'");
        }
    }
}
