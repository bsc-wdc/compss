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
package es.bsc.compss.loader;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.task.Binary;
import es.bsc.compss.types.annotations.task.COMPSs;
import es.bsc.compss.types.annotations.task.Container;
import es.bsc.compss.types.annotations.task.Decaf;
import es.bsc.compss.types.annotations.task.HTTP;
import es.bsc.compss.types.annotations.task.MPI;
import es.bsc.compss.types.annotations.task.Method;
import es.bsc.compss.types.annotations.task.MultiNode;
import es.bsc.compss.types.annotations.task.OmpSs;
import es.bsc.compss.types.annotations.task.OpenCL;
import es.bsc.compss.types.annotations.task.repeatables.Binaries;
import es.bsc.compss.types.annotations.task.repeatables.Containers;
import es.bsc.compss.types.annotations.task.repeatables.Decafs;
import es.bsc.compss.types.annotations.task.repeatables.MPIs;
import es.bsc.compss.types.annotations.task.repeatables.Methods;
import es.bsc.compss.types.annotations.task.repeatables.MultiCOMPSs;
import es.bsc.compss.types.annotations.task.repeatables.MultiMultiNode;
import es.bsc.compss.types.annotations.task.repeatables.MultiOmpSs;
import es.bsc.compss.types.annotations.task.repeatables.OpenCLs;
import es.bsc.compss.types.exceptions.NonInstantiableException;
import es.bsc.compss.types.implementations.definition.BinaryDefinition;
import es.bsc.compss.types.implementations.definition.COMPSsDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDefinition;
import es.bsc.compss.types.implementations.definition.DecafDefinition;
import es.bsc.compss.types.implementations.definition.MPIDefinition;
import es.bsc.compss.types.implementations.definition.OmpSsDefinition;
import es.bsc.compss.types.implementations.definition.OpenCLDefinition;
import es.bsc.compss.util.ErrorManager;
import java.lang.reflect.InvocationTargetException;
import java.security.SecureRandom;
import java.util.Random;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;
import javassist.expr.MethodCall;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import storage.StubItf;


public class LoaderUtils {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER_UTILS);

    public static final String CONTAINER_SIGNATURE = ContainerDefinition.SIGNATURE;
    public static final String BINARY_SIGNATURE = BinaryDefinition.SIGNATURE;
    public static final String MPI_SIGNATURE = MPIDefinition.SIGNATURE;
    public static final String DECAF_SIGNATURE = DecafDefinition.SIGNATURE;
    public static final String COMPSS_SIGNATURE = COMPSsDefinition.SIGNATURE;
    public static final String OMPSS_SIGNATURE = OmpSsDefinition.SIGNATURE;
    public static final String OPENCL_SIGNATURE = OpenCLDefinition.SIGNATURE;


    /**
     * Private constructor to avoid instantiation.
     */
    private LoaderUtils() {
        throw new NonInstantiableException("LoaderUtils");
    }

    /**
     * Returns the DataType of the given object {@code o} checked with the Storage interface.
     * 
     * @param o Object
     * @return The DataType of the Object.
     */
    public static DataType checkSCOType(Object o) {
        if (o instanceof StubItf && ((StubItf) o).getID() != null) {
            // Persisted Object
            return DataType.PSCO_T;
        }

        // The Data is an Object or a non persisted PSCO
        return DataType.OBJECT_T;
    }

    /**
     * Checks whether the given method is a remote invocation or not. Returns the remote method if it is a remote method
     * or {@code null} otherwise.
     * 
     * @param method Method to check.
     * @param remoteMethods List of detected remote methods.
     * @return The remote method if it is a remote method or {@code null} otherwise.
     * @throws NotFoundException When the parameter types of a CtClass cannot be found.
     */
    public static java.lang.reflect.Method checkRemote(CtMethod method, java.lang.reflect.Method[] remoteMethods,
        String originalClassName, String renamedClassName) throws NotFoundException {
        LOGGER.info("Checking Method " + method.getName());

        for (java.lang.reflect.Method remoteMethod : remoteMethods) {
            LOGGER.info("   ** To remote Method " + remoteMethod.getName());
            if (remoteMethod.isAnnotationPresent(Method.class)) {
                // METHOD
                Method remoteMethodAnnotation = remoteMethod.getAnnotation(Method.class);
                if (isSelectedMethod(method, remoteMethod, remoteMethodAnnotation.declaringClass(),
                    remoteMethodAnnotation.name(), originalClassName, renamedClassName)) {
                    return remoteMethod;
                }
            }

            if (remoteMethod.isAnnotationPresent(HTTP.class)) {
                // HTTP
                HTTP remoteMethodAnnotation = remoteMethod.getAnnotation(HTTP.class);
                if (isSelectedHTTP(method, remoteMethod, remoteMethodAnnotation)) {
                    return remoteMethod;
                }
            }

            if (remoteMethod.isAnnotationPresent(Container.class)) {
                // BINARY
                if (isSelectedNonNativeMethod(method, remoteMethod, CONTAINER_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(Binary.class)) {
                // BINARY
                if (isSelectedNonNativeMethod(method, remoteMethod, BINARY_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(MPI.class)) {
                // MPI
                if (isSelectedNonNativeMethod(method, remoteMethod, MPI_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(COMPSs.class)) {
                // COMPSs
                if (isSelectedNonNativeMethod(method, remoteMethod, COMPSS_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(MultiNode.class)) {
                // MultiNode
                MultiNode remoteMethodAnnotation = remoteMethod.getAnnotation(MultiNode.class);
                if (isSelectedMethod(method, remoteMethod, remoteMethodAnnotation.declaringClass(),
                    remoteMethodAnnotation.name(), originalClassName, renamedClassName)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(Decaf.class)) {
                // DECAF
                if (isSelectedNonNativeMethod(method, remoteMethod, DECAF_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(OmpSs.class)) {
                // OMPSS
                if (isSelectedNonNativeMethod(method, remoteMethod, OMPSS_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(OpenCL.class)) {
                // OPENCL
                if (isSelectedNonNativeMethod(method, remoteMethod, OPENCL_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            // REPEATABLES
            if (remoteMethod.isAnnotationPresent(Methods.class)) {
                // METHODS
                Methods methodsAnnotation = remoteMethod.getAnnotation(Methods.class);
                for (Method remoteMethodAnnotation : methodsAnnotation.value()) {
                    if (isSelectedMethod(method, remoteMethod, remoteMethodAnnotation.declaringClass(),
                        remoteMethodAnnotation.name(), originalClassName, renamedClassName)) {
                        return remoteMethod;
                    }
                }
            }
            if (remoteMethod.isAnnotationPresent(Containers.class)) {
                // BINARIES
                if (isSelectedNonNativeMethod(method, remoteMethod, CONTAINER_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(Binaries.class)) {
                // BINARIES
                if (isSelectedNonNativeMethod(method, remoteMethod, BINARY_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(MPIs.class)) {
                // MPIS
                if (isSelectedNonNativeMethod(method, remoteMethod, MPI_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(MultiCOMPSs.class)) {
                // MULTI-COMPSs
                if (isSelectedNonNativeMethod(method, remoteMethod, COMPSS_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(Decafs.class)) {
                // DECAFS
                if (isSelectedNonNativeMethod(method, remoteMethod, DECAF_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(MultiMultiNode.class)) {
                // MULTI-MULTINODE
                MultiMultiNode methodsAnnotation = remoteMethod.getAnnotation(MultiMultiNode.class);
                for (MultiNode remoteMethodAnnotation : methodsAnnotation.value()) {
                    if (isSelectedMethod(method, remoteMethod, remoteMethodAnnotation.declaringClass(),
                        remoteMethodAnnotation.name(), originalClassName, renamedClassName)) {
                        return remoteMethod;
                    }
                }
            }
            if (remoteMethod.isAnnotationPresent(MultiOmpSs.class)) {
                // MULTI-OMPSS
                if (isSelectedNonNativeMethod(method, remoteMethod, OMPSS_SIGNATURE)) {
                    return remoteMethod;
                }
            }
            if (remoteMethod.isAnnotationPresent(OpenCLs.class)) {
                // OPENCLS
                if (isSelectedNonNativeMethod(method, remoteMethod, OPENCL_SIGNATURE)) {
                    return remoteMethod;
                }
            }
        }

        // The method is not in the remote list
        return null;
    }

    private static boolean isSelectedMethod(CtMethod method, java.lang.reflect.Method remote, String remoteMethodClass,
        String remoteMethodName, String originalClassName, String renamedClassName) throws NotFoundException {
        // Patch remote method name if required
        if (remoteMethodName == null || remoteMethodName.isEmpty() || remoteMethodName.equals(Constants.UNASSIGNED)) {
            remoteMethodName = remote.getName();
        }

        // Check if method names are equal
        LOGGER.debug("  - Checking " + method.getName() + " against " + remoteMethodName);
        if (!remoteMethodName.equals(method.getName())) {
            return false;
        }

        // Check if methods belong to the same class
        LOGGER.debug("  - Checking classes " + method.getDeclaringClass().getName() + " against " + remoteMethodClass);
        if (renamedClassName != null && renamedClassName.equals(method.getDeclaringClass().getName())) {
            if (!remoteMethodClass.equals(originalClassName)) {
                return false;
            }
        } else {
            boolean matchesClass = remoteMethodClass.equals(method.getDeclaringClass().getName());
            if (!matchesClass) {
                return false;
            }
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

    private static boolean isSelectedHTTP(CtMethod method, java.lang.reflect.Method remote, HTTP httpAnnotation)
        throws NotFoundException {

        // Check if methods have the same name
        String nameRemote = remote.getName();

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

        String packName = method.getDeclaringClass().getName();
        String namespace = httpAnnotation.declaringClass();

        return packName.equals(namespace);
    }

    /**
     * Checks whether the method call is a close of a stream or not.
     * 
     * @param mc Method call.
     * @return {@code true} if the call is a close of a stream, {@code false} otherwise.
     */
    public static boolean isStreamClose(MethodCall mc) {
        if ("close".equals(mc.getMethodName())) {
            String fullName = mc.getClassName();
            if (fullName.startsWith("java.io.")) {
                String className = fullName.substring(8);
                if (LoaderConstants.getSupportedStreamTypes().contains(className)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns a random numeric String of the given length {@code length} and starting with the given prefix
     * {@code prefix}.
     * 
     * @param length String length.
     * @param prefix Prefix.
     * @return Random numeric String of the given length {@code length} and starting with the given prefix
     *         {@code prefix}.
     */
    public static String randomName(int length, String prefix) {
        if (length < 1) {
            return prefix;
        }

        Random r = new SecureRandom();
        StringBuilder buffer = new StringBuilder();
        int gap = ('9' + 1) - '0';

        for (int i = 0; i < length; i++) {
            char c = (char) (r.nextInt(gap) + '0');
            buffer.append(c);
        }

        return prefix + buffer.toString();
    }

    /**
     * Checks whether the method is the main method or not.
     * 
     * @param m Method.
     * @return {@code true} if the method is the main method, {@code false} otherwise.
     * @throws NotFoundException When the CtClass parameter types cannot be found.
     */
    public static boolean isMainMethod(CtMethod m) throws NotFoundException {
        return ("main".equals(m.getName()) && m.getParameterTypes().length == 1
            && m.getParameterTypes()[0].getName().equals(String[].class.getCanonicalName()));
    }

    /**
     * Returns whether the method is an orchestration element or not.
     * 
     * @param m Method.
     * @return {@code true} if the method is annotated as orchestration, {@code false} otherwise.
     */
    public static boolean isOrchestration(CtMethod m) {
        return m.hasAnnotation(es.bsc.compss.types.annotations.Orchestration.class);
    }

    /**
     * Returns whether the given method {@code method} is contained in the given list of methods {@code methods}.
     * 
     * @param methods List of methods.
     * @param method Method to check.
     * @return {@code true} if method is inside the list of methods, {@code false} otherwise.
     */
    public static boolean contains(CtMethod[] methods, CtMethod method) {
        for (CtMethod m : methods) {
            if (m.equals(method) && method.getDeclaringClass().equals(m.getDeclaringClass())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds WithUR to the method name parameter of the executeTask call.
     * 
     * @param executeTask StringBuilder containing the complete executeTask call.
     * @param methodName Execute task method name.
     * @return StringBuilder containing the complete executeTask call after adding the WithUR to the method name
     *         parameter.
     */
    public static StringBuilder replaceMethodName(StringBuilder executeTask, String methodName) {
        String patternStr = ",\"" + methodName + "\",";
        int start = executeTask.toString().indexOf(patternStr);
        int end = start + patternStr.length();
        return executeTask.replace(start, end, ",\"" + methodName + "WithUR\",");
    }

    /**
     * Adds SLA params to the executeTask call.
     * 
     * @param executeTask StringBuilder containing the current executeTask call.
     * @param numParams Number of parameters.
     * @param appNameParam Application name.
     * @param slaIdParam SLA Id parameter.
     * @param urNameParam UR Name parameter.
     * @param primaryHostParam Primary host parameter.
     * @param transferIdParam Transfer Id parameter.
     * @return StringBuilder containing the complete executeTask call.
     */
    public static StringBuilder modifyString(StringBuilder executeTask, int numParams, String appNameParam,
        String slaIdParam, String urNameParam, String primaryHostParam, String transferIdParam) {

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

    /**
     * TODO javadoc.
     */
    /**
     * Runs a method with a target object.
     * 
     * @param o Target object.
     * @param methodClass Method class.
     * @param methodName Method name.
     * @param values Method parameter values.
     * @param types Method parameter types.
     * @return Return value after the method invocation (can be {@code null}).
     * @throws Throwable Re-throw the user exception thrown by the method
     */
    public static Object runMethodOnObject(Object o, Class<?> methodClass, String methodName, Object[] values,
        Class<?>[] types) throws Throwable {
        // Use reflection to get the requested method
        java.lang.reflect.Method method = null;
        try {
            method = methodClass.getMethod(methodName, types);
        } catch (SecurityException se) {
            ErrorManager.error("Error writing the instrumented class file", se);
            return null;
        } catch (NoSuchMethodException nsme) {
            String errMsg = "Requested method " + methodName + " of " + methodClass + " not found\n"
                + "Types length is " + types.length + "\n";
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
            ErrorManager.error("Wrong argument passed to method " + methodName + " with class " + o.toString(), iae);
        } catch (IllegalAccessException iae) {
            ErrorManager.error("Cannot access method " + methodName, iae);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                throw cause;// re-throw the user exception thrown by the method
            } else {
                throw e;
            }
        }

        return retValue;
    }

    /**
     * Returns whether the call is deleting a file or not.
     * 
     * @param mc Method call.
     * @return {@code true} if the method is deleting a file, {@code false} otherwise.
     */
    public static boolean isFileDelete(MethodCall mc) {
        if ("delete".equals(mc.getMethodName())) {
            return mc.getClassName().compareTo("java.io.File") == 0;
        }
        return false;
    }

}
