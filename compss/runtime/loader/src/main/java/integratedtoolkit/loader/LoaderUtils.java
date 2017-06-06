package integratedtoolkit.loader;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.task.Binary;
import integratedtoolkit.types.annotations.task.Decaf;
import integratedtoolkit.types.annotations.task.MPI;
import integratedtoolkit.types.annotations.task.Method;
import integratedtoolkit.types.annotations.task.OmpSs;
import integratedtoolkit.types.annotations.task.OpenCL;
import integratedtoolkit.types.annotations.task.Service;
import integratedtoolkit.types.annotations.task.repeatables.Binaries;
import integratedtoolkit.types.annotations.task.repeatables.Decafs;
import integratedtoolkit.types.annotations.task.repeatables.MPIs;
import integratedtoolkit.types.annotations.task.repeatables.Methods;
import integratedtoolkit.types.annotations.task.repeatables.MultiOmpSs;
import integratedtoolkit.types.annotations.task.repeatables.OpenCLs;
import integratedtoolkit.types.annotations.task.repeatables.Services;
import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.exceptions.NonInstantiableException;
import integratedtoolkit.util.ErrorManager;

import java.lang.reflect.InvocationTargetException;
import java.util.Random;

import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;
import javassist.expr.MethodCall;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StubItf;


public class LoaderUtils {

    public static final String MPI_SIGNATURE = "mpi.MPI";
    public static final String OMPSS_SIGNATURE = "ompss.OMPSS";
    public static final String OPENCL_SIGNATURE = "opencl.OPPENCL";
    public static final String BINARY_SIGNATURE = "binary.BINARY";
    public static final String DECAF_SIGNATURE = "decaf.DECAF";

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER_UTILS);


    private LoaderUtils() {
        throw new NonInstantiableException("LoaderUtils");
    }

    // Storage: Check object type
    public static DataType checkSCOType(Object o) {
        if (o instanceof StubItf && ((StubItf) o).getID() != null) {
            // Persisted Object
            return DataType.PSCO_T;
        }

        // The Data is an Object or a non persisted PSCO
        return DataType.OBJECT_T;
    }

    // Return the called method if it is in the remote list
    public static java.lang.reflect.Method checkRemote(CtMethod method, java.lang.reflect.Method[] remoteMethods) throws NotFoundException {
        LOGGER.info("Checking Method " + method.getName());

        for (java.lang.reflect.Method remoteMethod_javaLang : remoteMethods) {
            LOGGER.info("   ** To remote Method " + remoteMethod_javaLang.getName());
            if (remoteMethod_javaLang.isAnnotationPresent(Method.class)) {
                // METHOD
                Method remoteMethodAnnotation = remoteMethod_javaLang.getAnnotation(Method.class);
                if (isSelectedMethod(method, remoteMethod_javaLang, remoteMethodAnnotation)) {
                    return remoteMethod_javaLang;
                }
            }
            if (remoteMethod_javaLang.isAnnotationPresent(Service.class)) {
                // SERVICE
                Service remoteMethodAnnotation = remoteMethod_javaLang.getAnnotation(Service.class);
                if (isSelectedService(method, remoteMethod_javaLang, remoteMethodAnnotation)) {
                    return remoteMethod_javaLang;
                }
            }
            if (remoteMethod_javaLang.isAnnotationPresent(MPI.class)) {
                // MPI
                if (isSelectedNonNativeMethod(method, remoteMethod_javaLang, MPI_SIGNATURE)) {
                    return remoteMethod_javaLang;
                }
            }
            if (remoteMethod_javaLang.isAnnotationPresent(OmpSs.class)) {
                // OMPSS
                if (isSelectedNonNativeMethod(method, remoteMethod_javaLang, OMPSS_SIGNATURE)) {
                    return remoteMethod_javaLang;
                }
            }
            if (remoteMethod_javaLang.isAnnotationPresent(OpenCL.class)) {
                // OPENCL
                if (isSelectedNonNativeMethod(method, remoteMethod_javaLang, OPENCL_SIGNATURE)) {
                    return remoteMethod_javaLang;
                }
            }
            if (remoteMethod_javaLang.isAnnotationPresent(Binary.class)) {
                // BINARY
                if (isSelectedNonNativeMethod(method, remoteMethod_javaLang, BINARY_SIGNATURE)) {
                    return remoteMethod_javaLang;
                }
            }
            if (remoteMethod_javaLang.isAnnotationPresent(Decaf.class)) {
                // BINARY
                if (isSelectedNonNativeMethod(method, remoteMethod_javaLang, DECAF_SIGNATURE)) {
                    return remoteMethod_javaLang;
                }
            }
            // REPEATABLES
            if (remoteMethod_javaLang.isAnnotationPresent(Methods.class)) {
                // METHODS
                Methods methodsAnnotation = remoteMethod_javaLang.getAnnotation(Methods.class);
                for (Method remoteMethodAnnotation : methodsAnnotation.value()) {
                    if (isSelectedMethod(method, remoteMethod_javaLang, remoteMethodAnnotation)) {
                        return remoteMethod_javaLang;
                    }
                }

            }
            if (remoteMethod_javaLang.isAnnotationPresent(Services.class)) {
                // SERVICES
                Services servicesAnnotation = remoteMethod_javaLang.getAnnotation(Services.class);
                for (Service remoteServiceAnnotation : servicesAnnotation.value()) {
                    if (isSelectedService(method, remoteMethod_javaLang, remoteServiceAnnotation)) {
                        return remoteMethod_javaLang;
                    }
                }
            }
            if (remoteMethod_javaLang.isAnnotationPresent(MPIs.class)) {
                // MPIS
                if (isSelectedNonNativeMethod(method, remoteMethod_javaLang, MPI_SIGNATURE)) {
                    return remoteMethod_javaLang;
                }
            }
            if (remoteMethod_javaLang.isAnnotationPresent(MultiOmpSs.class)) {
                // MULTI-OMPSS
                if (isSelectedNonNativeMethod(method, remoteMethod_javaLang, OMPSS_SIGNATURE)) {
                    return remoteMethod_javaLang;
                }
            }
            if (remoteMethod_javaLang.isAnnotationPresent(OpenCLs.class)) {
                // OPENCLS
                if (isSelectedNonNativeMethod(method, remoteMethod_javaLang, OPENCL_SIGNATURE)) {
                    return remoteMethod_javaLang;
                }
            }
            if (remoteMethod_javaLang.isAnnotationPresent(Binaries.class)) {
                // BINARIES
                if (isSelectedNonNativeMethod(method, remoteMethod_javaLang, BINARY_SIGNATURE)) {
                    return remoteMethod_javaLang;
                }
            }
            if (remoteMethod_javaLang.isAnnotationPresent(Decafs.class)) {
                // BINARIES
                if (isSelectedNonNativeMethod(method, remoteMethod_javaLang, DECAF_SIGNATURE)) {
                    return remoteMethod_javaLang;
                }
            }
        }

        // The method is not in the remote list
        return null;
    }

    private static boolean isSelectedMethod(CtMethod method, java.lang.reflect.Method remote, Method methodAnnot) throws NotFoundException {
        // Check if methods have the same name
        String nameRemote = methodAnnot.name();
        if (nameRemote.equals(Constants.UNASSIGNED)) {
            nameRemote = remote.getName();
        }

        LOGGER.debug("  - Checking " + method.getName() + " against " + nameRemote);
        if (!nameRemote.equals(method.getName())) {
            return false;
        }

        // Check if methods belong to the same class
        LOGGER.debug("  - Checking classes " + method.getDeclaringClass().getName() + " against " + methodAnnot.declaringClass());
        boolean matchesClass = methodAnnot.declaringClass().equals(method.getDeclaringClass().getName());
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

    private static boolean isSelectedNonNativeMethod(CtMethod method, java.lang.reflect.Method remote, String signature)
            throws NotFoundException {

        LOGGER.debug("  - Checking " + method.getName() + " against " + remote.getName());

        // Must have the same task name
        boolean matchesMethodname = method.getName().equals(remote.getName());
        if (!matchesMethodname) {
            return false;
        }

        // Non-native methods must contain the valid signature signature
        LOGGER.debug("  - Checking classes " + method.getDeclaringClass().getName() + " against " + signature);
        boolean matchesClass = method.getDeclaringClass().getName().equals(signature);
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

    private static boolean isSelectedService(CtMethod method, java.lang.reflect.Method remote, Service serviceAnnot)
            throws NotFoundException {

        // Check if methods have the same name
        String nameRemote = serviceAnnot.operation();
        if (nameRemote.equals(Constants.UNASSIGNED)) {
            nameRemote = remote.getName();
        }

        LOGGER.debug("  - Checking " + method.getName() + " against " + nameRemote);

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
            if (!paramClassCurrent[i].getName().equals(paramClassRemote[i].getCanonicalName())) {
                return false;
            }
        }

        // Check that return types match
        // if (!method.getReturnType().getName().equals(remote.getReturnType().getName()))
        // return false;
        // Check if the package of the class which implements the called method matches the pattern
        // namespace.service.port of the interface method
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
        String service = annot.name()/* .toLowerCase() */;
        String port = annot.port()/* .toLowerCase() */;

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

        namespace = namespace// .substring(0, namespace.indexOf(".xsd")) // remove .xsd at the end
                .substring(startIndex) // remove http://www.
                .replace('/', '.') // replace / by .
                .replace('-', '.') // replace - by .
                .replace(':', '.'); // replace : by .

        return "dummy." + namespace + '.' + service + '.' + port;
    }

    // Check whether the method call is a close of a stream
    public static boolean isStreamClose(MethodCall mc) {
        if ("close".equals(mc.getMethodName())) {
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
        return ("main".equals(m.getName()) && m.getParameterTypes().length == 1
                && m.getParameterTypes()[0].getName().equals(String[].class.getCanonicalName()));
    }

    public static boolean isOrchestration(CtMethod m) {
        return m.hasAnnotation(integratedtoolkit.types.annotations.Orchestration.class);
    }

    public static boolean contains(CtMethod[] methods, CtMethod method) {
        for (CtMethod m : methods) {
            if (m.equals(method) && method.getDeclaringClass().equals(m.getDeclaringClass())) {
                return true;
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
    public static StringBuilder modifyString(StringBuilder executeTask, int numParams, String appNameParam, String slaIdParam,
            String urNameParam, String primaryHostParam, String transferIdParam) {

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

    public static Object runMethodOnObject(Object o, Class<?> methodClass, String methodName, Object[] values, Class<?>[] types)
            throws Throwable {
        // Use reflection to get the requested method
        java.lang.reflect.Method method = null;
        try {
            method = methodClass.getMethod(methodName, types);
        } catch (SecurityException se) {
            ErrorManager.error("Error writing the instrumented class file", se);
            return null;
        } catch (NoSuchMethodException nsme) {
            String errMsg = "Requested method " + methodName + " of " + methodClass + " not found\n" + "Types length is " + types.length
                    + "\n";
            for (Class<?> type : types) {
                errMsg += "Type is " + type;
            }
            ErrorManager.error(errMsg, nsme);
            return null;
        }

        // Invoke the requested method
        Object retValue = null;
        try {
            retValue = method.invoke(o, values);
        } catch (IllegalArgumentException iae) {
            ErrorManager.error("Wrong argument passed to method " + methodName, iae);
        } catch (IllegalAccessException iae) {
            ErrorManager.error("Cannot access method " + methodName, iae);
        } catch (InvocationTargetException e) {
            throw e.getCause(); // re-throw the user exception thrown by the method
        }

        return retValue;
    }

    public static boolean isFileDelete(MethodCall mc) {
        if ("delete".equals(mc.getMethodName())) {
            return mc.getClassName().compareTo("java.io.File") == 0;
        }
        return false;
    }

}
