package integratedtoolkit.loader;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.annotations.Service;
import integratedtoolkit.types.parameter.PSCOId;
import integratedtoolkit.util.ErrorManager;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Random;

import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;
import javassist.expr.MethodCall;

import org.apache.log4j.Logger;

import storage.StubItf;


public class LoaderUtils {

	private static final Logger logger = Logger.getLogger(Loggers.LOADER_UTILS);
	
	// Storage: Check whether Persistent Self-Contained Object or not
	public static Object checkSCOPersistent(Object o) {   	
    	if (o instanceof StubItf) {
    		// Cast to SCO Stub
    		StubItf sco = (StubItf) o;
    		// Check whether persisted
    		String id = null;
    		try {
    			id = sco.getID();
    		} catch (Exception e) {                 			
    			logger.debug("SCO with hashcode " + o.hashCode() + " is not persisted yet");
    		}
    		if (id != null) {
    			PSCOId pscoID = new PSCOId(o, id);
    			return pscoID;
    		}    		
    	}   		
   		return o;
	}
	
	// Storage: Check object type
	public static DataType checkSCOType(Object o) {
    	if (o instanceof PSCOId) {
    		return DataType.PSCO_T;
    	}
    	
    	if (o instanceof StubItf) {
    		// Cast to SCO Stub
    		StubItf sco = (StubItf) o;
    		// Check whether persisted
    		String id = null;
    		try {
    			id = sco.getID();
    		} catch (Exception e) {                 			
    			logger.debug("SCO with hashcode " + o.hashCode() + " is not persisted yet");
    		}

    		if (id == null) {
    			return DataType.SCO_T;
    		} else {
    			return DataType.PSCO_T;
    		}    		
    	}   		
   		return DataType.OBJECT_T;
	}	

    // Return the called method if it is in the remote list
    public static Method checkRemote(CtMethod method, Method[] remoteMethods) throws NotFoundException {
        for (Method remote : remoteMethods) {
            if (remote.isAnnotationPresent(integratedtoolkit.types.annotations.Method.class)) {
                if (isSelectedMethod(method, remote)) {
                    return remote;
                }
            } else if (remote.isAnnotationPresent(integratedtoolkit.types.annotations.Service.class)) { // Service
                if (isSelectedService(method, remote)) {
                    return remote;
                }
            } else {
            	ErrorManager.error("Task '" + remote.getName() + "' does not have @Method or @Service annotation.\n" + 
            					   "Check the COMPSs manual for more information.");
            }
        }

        // The method is not in the remote list
        return null;
    }

    private static boolean isSelectedMethod(CtMethod method, Method remote) throws NotFoundException {
    	
        integratedtoolkit.types.annotations.Method methodAnnot = remote.getAnnotation(integratedtoolkit.types.annotations.Method.class);

        // Check if methods have the same name
        String nameRemote = methodAnnot.name();
        if (nameRemote.equals("[unassigned]")) {
            nameRemote = remote.getName();
        }

        if (!nameRemote.equals(method.getName())) {
            return false;
        }

        // Check if methods belong to the same class
        boolean matchesClass = false;
        String[] remoteDeclaringClasses = methodAnnot.declaringClass();
        for (int i = 0; i < remoteDeclaringClasses.length && !matchesClass; i++) {
            String remoteDeclaringClass = remoteDeclaringClasses[i];
            matchesClass = remoteDeclaringClass.compareTo(method.getDeclaringClass().getName()) == 0;
        }
        if (!matchesClass) {
            return false;
        }

        // Check that methods have the same number of parameters
        CtClass[] paramClassCurrent = method.getParameterTypes();
        Class<?>[] paramClassRemote = remote.getParameterTypes();
        if (paramClassCurrent.length != paramClassRemote.length) {
            return false;
        }

        // Check that parameter types match
        for (int i = 0; i < paramClassCurrent.length; i++) {
            if (!paramClassCurrent[i].getName().equals(paramClassRemote[i].getCanonicalName())) {
                return false;
            }
        }

        // Methods match!
        return true;
    }

    private static boolean isSelectedService(CtMethod method, Method remote) throws NotFoundException {
        Service serviceAnnot = remote.getAnnotation(Service.class);

        // Check if methods have the same name
        String nameRemote = serviceAnnot.operation();
        if (nameRemote.equals("[unassigned]")) {
            nameRemote = remote.getName();
        }

        if (!nameRemote.equals(method.getName())) {
            return false;
        }

        // Check that methods have the same number of parameters
        CtClass[] paramClassCurrent = method.getParameterTypes();
        Class<?>[] paramClassRemote = remote.getParameterTypes();
        if (paramClassCurrent.length != paramClassRemote.length) {
            return false;
        }

        // Check that parameter types match
        for (int i = 0; i < paramClassCurrent.length; i++) {
            if (!paramClassCurrent[i].getName()
                    .equals(paramClassRemote[i].getCanonicalName())) {
                return false;
            }
        }

        // Check that return types match
        //if (!method.getReturnType().getName().equals(remote.getReturnType().getName()))
        //	return false;
        // Check if the package of the class which implements the called method matches the pattern namespace.service.port of the interface method
        String packName = method.getDeclaringClass().getPackageName();
        String nsp = combineServiceMetadata(serviceAnnot);
        if (!packName.equals(nsp)) {
            return false;
        }

        // Methods match!
        return true;
    }

    private static String combineServiceMetadata(Service annot) {
        String namespace = annot.namespace();
        String service = annot.name()/*.toLowerCase()*/;
        String port = annot.port()/*.toLowerCase()*/;

        int startIndex = namespace.indexOf("//www.");
        if (startIndex < 0) {
            startIndex = namespace.indexOf("http://");
            if (startIndex >= 0) {
                startIndex += "http://".length();
            } else {
                startIndex = 0;
            }
        } else {
            startIndex += "//www.".length();
        }

        namespace = namespace//.substring(0, namespace.indexOf(".xsd")) 	  // remove .xsd at the end
                .substring(startIndex) // remove http://www.
                .replace('/', '.') // replace / by .
                .replace('-', '.') // replace - by .
                .replace(':', '.'); 						  // replace : by .

        return "dummy." + namespace + '.' + service + '.' + port;
    }

    // Check whether the method call is a close of a stream
    public static boolean isStreamClose(MethodCall mc) {
        if (mc.getMethodName().equals("close")) {
            String fullName = mc.getClassName();
            if (fullName.startsWith("java.io.")) {
                String className = fullName.substring(8);
                if (LoaderConstants.SUPPORTED_STREAM_TYPES.contains(className)) {
                    return true;
                }
            }
        }
        return false;
    }

    // Return a random numeric string
    public static String randomName(int length, String prefix) {
        if (length < 1) {
            return prefix;
        }

        Random r = new Random();
        StringBuilder buffer = new StringBuilder();
        int gap = ('9' + 1) - '0';

        for (int i = 0; i < length; i++) {
            char c = (char) (r.nextInt(gap) + '0');
            buffer.append(c);
        }

        return prefix + buffer.toString();
    }

    // Check if the method is the main method
    public static boolean isMainMethod(CtMethod m) throws NotFoundException {
        return (m.getName().equals("main")
                && m.getParameterTypes().length == 1
                && m.getParameterTypes()[0].getName().equals(String[].class.getCanonicalName()));
    }

    public static boolean isOrchestration(CtMethod m) {
        return m.hasAnnotation(integratedtoolkit.types.annotations.Orchestration.class);
    }

    public static boolean contains(CtMethod[] methods, CtMethod method) {
    	for (CtMethod m : methods) {
            if (m.equals(method)) {
                if (method.getDeclaringClass().equals(m.getDeclaringClass())){
                	return true;
                }
            }
        }
        return false;
    }

    // Add WithUR to the method name parameter of the executeTask call
    public static StringBuilder replaceMethodName(StringBuilder executeTask, String methodName) {
        String patternStr = ",\"" + methodName + "\",";
        int start = executeTask.toString().indexOf(patternStr);
        int end = start + patternStr.length();
        return executeTask.replace(start, end, ",\"" + methodName + "WithUR\",");
    }

    // Add SLA params to the executeTask call
    public static StringBuilder modifyString(StringBuilder executeTask,
            int numParams,
            String appNameParam,
            String slaIdParam,
            String urNameParam,
            String primaryHostParam,
            String transferIdParam) {

        // Number of new params we add
        int newParams = 5;

        // String of new params
        StringBuilder params = new StringBuilder(appNameParam);
        params.append(",");
        params.append(slaIdParam);
        params.append(",");
        params.append(urNameParam);
        params.append(",");
        params.append(primaryHostParam);
        params.append(",");
        params.append(transferIdParam);
        params.append("});");

        String patternStr;

        if (numParams == 0) {
            patternStr = 0 + ",null);";
        } else {
            patternStr = "," + numParams + ",";
        }

        int start = executeTask.toString().indexOf(patternStr);
        int end = start + patternStr.length();

        if (numParams == 0) {
            return executeTask.replace(start, end, newParams + ",new Object[]{" + params);
        } else {
            executeTask.replace(start + 1, end - 1, Integer.toString(numParams + newParams));
            executeTask.replace(executeTask.length() - 3, executeTask.length(), "");
            return executeTask.append("," + params);
        }
    }

    public static Object runMethodOnObject(Object o, Class<?> methodClass, String methodName, Object[] values, Class<?>[] types) throws Throwable {
        // Use reflection to get the requested method
        Method method = null;
        try {
            method = methodClass.getMethod(methodName, types);
        } catch (SecurityException e) {
        	ErrorManager.error("Error writing the instrumented class file");
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
        	String errMsg = "Requested method " + methodName + " of " + methodClass + " not found\n" +
        					"Types length is " + types.length + "\n";
            for (Class<?> type : types) {
            	errMsg += "Type is " + type;
            }
        	ErrorManager.error(errMsg);            
        }

        // Invoke the requested method
        Object retValue = null;
        try {
            retValue = method.invoke(o, values);
        } catch (IllegalArgumentException e) {
        	ErrorManager.error("Wrong argument passed to method " + methodName, e);
        } catch (IllegalAccessException e) {
        	ErrorManager.error("Cannot access method " + methodName, e);
        } catch (InvocationTargetException e) {
            throw e.getCause(); // re-throw the user exception thrown by the method
        }

        return retValue;
    }

    public static boolean isFileDelete(MethodCall mc) {
        if (mc.getMethodName().equals("delete")) {
            return (mc.getClassName().compareTo("java.io.File") == 0);
        }
        return false;
    }
}
