/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.loader.workflow;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Epilog;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.Prolog;
import es.bsc.compss.types.annotations.SchedulerHints;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.annotations.parameter.Type;
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
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.implementations.definition.BinaryDefinition;
import es.bsc.compss.types.implementations.definition.COMPSsDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDefinition.ContainerExecutionType;
import es.bsc.compss.types.implementations.definition.DecafDefinition;
import es.bsc.compss.types.implementations.definition.MPIDefinition;
import es.bsc.compss.types.implementations.definition.OmpSsDefinition;
import es.bsc.compss.types.implementations.definition.OpenCLDefinition;
import es.bsc.compss.util.EnvironmentLoader;
import es.bsc.compss.util.ErrorManager;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CEIParser {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final String PROCESSORS = "processors";

    private static final String PROC_NAME = "processorname";
    private static final String COMPUTING_UNITS = "computingunits";
    private static final String PROC_SPEED = "processorspeed";
    private static final String PROC_ARCH = "processorarchitecture";
    private static final String PROC_TYPE = "processortype";
    private static final String PROC_MEM_SIZE = "processorinternalmemorysize";
    private static final String PROC_PROP_NAME = "processorpropertyname";
    private static final String PROC_PROP_VALUE = "processorpropertyvalue";

    private static final String MEM_SIZE = "memorysize";
    private static final String MEM_TYPE = "memorytype";
    private static final String STORAGE_SIZE = "storagesize";
    private static final String STORAGE_TYPE = "storagetype";
    private static final String STORAGE_BW = "storagebw";
    private static final String OS_TYPE = "operatingsystemtype";
    private static final String OS_DISTRIBUTION = "operatingsystemdistribution";
    private static final String OS_VERSION = "operatingsystemversion";
    private static final String APP_SOFTWARE = "appsoftware";
    private static final String HOST_QUEUES = "hostqueues";
    private static final String WALL_CLOCK_LIMIT = "wallclocklimit";


    /**
     * Loads the annotated class and initializes the data structures that contain the constraints. For each method found
     * in the annotated interface creates its signature and adds the constraints to the structures.
     *
     * @param ceiName package and name of the Annotated Interface class
     * @return
     */
    public static void registerCoreElements(String ceiName, COMPSsRuntime runtime) {
        Class<?> annotItfClass;
        try {
            annotItfClass = Class.forName(ceiName);
        } catch (Exception e) {
            LOGGER.warn("Could not find class " + ceiName, e);
            return;
        }
        int coreCount = annotItfClass.getDeclaredMethods().length;
        if (DEBUG) {
            LOGGER.debug("Detected methods " + coreCount);
        }

        // Check registered methods
        for (java.lang.reflect.Method m : annotItfClass.getDeclaredMethods()) {
            LOGGER.debug("Method = " + m);
            registerITFMethod(m, runtime);
        }
    }

    /**
     * Parses a single ITF Method (can have multiple annotations).
     *
     * @param m Java lang method to parse.
     * @param runtime Runtime where to register the CE.
     * @return The core element definition.
     */
    private static void registerITFMethod(java.lang.reflect.Method m, COMPSsRuntime runtime) {
        // Computes the callee method signature and checks parameter annotations
        LOGGER.info("Evaluating method " + m.getName());

        // Check all annotations are valid
        checkMethodAnnotation(m);

        // Load if there is any non-native annotation or not
        boolean hasNonNative = checkNonNativeAnnotation(m);

        StringBuilder ceSignatureBuilder = new StringBuilder();
        String methodName = m.getName();
        ceSignatureBuilder.append(methodName).append("(");

        // Construct signature and check parameters
        boolean[] hasAnnotations = constructSignatureAndCheckParameters(m, hasNonNative, ceSignatureBuilder);

        String ceSignature = ceSignatureBuilder.toString();
        boolean hasStreams = hasAnnotations[0];
        boolean hasPrefixes = hasAnnotations[1];

        // Check all annotations present at the method for versioning
        if (DEBUG) {
            LOGGER.debug("   * Method method " + methodName + " has " + m.getAnnotations().length + " annotations");
        }
        defineImplementation(runtime, m, ceSignature, hasStreams, hasPrefixes);
    }

    /**
     * Checks if all the annotations present in method {@code m} are valid or not.
     *
     * @param m Method.
     */
    private static void checkMethodAnnotation(java.lang.reflect.Method m) {
        /*
         * Check that all method annotations are valid
         */
        for (Annotation annot : m.getAnnotations()) {
            if (!annot.annotationType().getName().equals(Constraints.class.getName())
                // Simple annotations
                && !annot.annotationType().getName().equals(Method.class.getName())
                && !annot.annotationType().getName().equals(HTTP.class.getName())
                && !annot.annotationType().getName().equals(Binary.class.getName())
                && !annot.annotationType().getName().equals(Container.class.getName())
                && !annot.annotationType().getName().equals(MPI.class.getName())
                && !annot.annotationType().getName().equals(Decaf.class.getName())
                && !annot.annotationType().getName().equals(COMPSs.class.getName())
                && !annot.annotationType().getName().equals(MultiNode.class.getName())
                && !annot.annotationType().getName().equals(OmpSs.class.getName())
                && !annot.annotationType().getName().equals(OpenCL.class.getName())
                // Repeatable annotations
                && !annot.annotationType().getName().equals(Methods.class.getName())
                && !annot.annotationType().getName().equals(Binaries.class.getName())
                && !annot.annotationType().getName().equals(Containers.class.getName())
                && !annot.annotationType().getName().equals(MPIs.class.getName())
                && !annot.annotationType().getName().equals(Decafs.class.getName())
                && !annot.annotationType().getName().equals(MultiCOMPSs.class.getName())
                && !annot.annotationType().getName().equals(MultiMultiNode.class.getName())
                && !annot.annotationType().getName().equals(MultiOmpSs.class.getName())
                && !annot.annotationType().getName().equals(OpenCLs.class.getName())
                // Scheduler hints
                && !annot.annotationType().getName().equals(SchedulerHints.class.getName())) {

                ErrorManager.warn("Unrecognised annotation " + annot.annotationType().getName() + " . SKIPPING");
            }
        }
    }

    /**
     * Returns if the method {@code m} has non native annotations or not.
     *
     * @param m method.
     * @return {@code true} if the method contains non-native annotation, {@code false} otherwise.
     */
    private static boolean checkNonNativeAnnotation(java.lang.reflect.Method m) {
        /*
         * Checks if there is a non-native annotation or not
         */
        for (Annotation annot : m.getAnnotations()) {
            if (annot.annotationType().getName().equals(Binary.class.getName())
                || annot.annotationType().getName().equals(Container.class.getName())
                || annot.annotationType().getName().equals(MPI.class.getName())
                || annot.annotationType().getName().equals(Decaf.class.getName())
                || annot.annotationType().getName().equals(COMPSs.class.getName())
                || annot.annotationType().getName().equals(MultiNode.class.getName())
                || annot.annotationType().getName().equals(OmpSs.class.getName())
                || annot.annotationType().getName().equals(OpenCL.class.getName())
                // Repeatable annotations
                || annot.annotationType().getName().equals(Binaries.class.getName())
                || annot.annotationType().getName().equals(Containers.class.getName())
                || annot.annotationType().getName().equals(MPIs.class.getName())
                || annot.annotationType().getName().equals(Decafs.class.getName())
                || annot.annotationType().getName().equals(MultiCOMPSs.class.getName())
                || annot.annotationType().getName().equals(MultiMultiNode.class.getName())
                || annot.annotationType().getName().equals(MultiOmpSs.class.getName())
                || annot.annotationType().getName().equals(OpenCLs.class.getName())) {

                return true;
            }
        }

        return false;
    }

    /**
     * Constructs the signature of method {@code m} and leaves the result in calleeMethodSignature. It also returns if
     * the method has stream parameters or not.
     *
     * @param m Method.
     * @param hasNonNative Whether the method has non-native annotations or not.
     * @param calleeMethodSignature Callee method signature.
     * @return Two booleans indicating if the method contains StdIO Streams and prefixes.
     */
    private static boolean[] constructSignatureAndCheckParameters(java.lang.reflect.Method m, boolean hasNonNative,
        StringBuilder calleeMethodSignature) {

        boolean hasStreams = false;
        boolean hasPrefixes = false;

        String methodName = m.getName();
        boolean hasSTDIN = false;
        boolean hasSTDOUT = false;
        boolean hasSTDERR = false;
        int numPars = m.getParameterAnnotations().length;
        if (numPars > 0) {
            for (int i = 0; i < numPars; i++) {
                Parameter par = (Parameter) m.getParameterAnnotations()[i][0];

                Class<?> parType = m.getParameterTypes()[i];
                Type annotType = par.type();
                String type = inferType(parType, annotType);

                // Add to callee
                if (i >= 1) {
                    calleeMethodSignature.append(",");
                }
                calleeMethodSignature.append(type);

                // Check parameter stream annotation
                switch (par.stream()) {
                    case STDIN:
                        if (hasSTDIN) {
                            ErrorManager.error(
                                "Method " + methodName + " has more than one parameter annotated has Stream.STDIN");
                        }
                        hasSTDIN = true;
                        break;
                    case STDOUT:
                        if (hasSTDOUT) {
                            ErrorManager.error(
                                "Method " + methodName + " has more than one parameter annotated has Stream.STDOUT");
                        }
                        hasSTDOUT = true;
                        break;
                    case STDERR:
                        if (hasSTDERR) {
                            ErrorManager.error(
                                "Method " + methodName + " has more than one parameter annotated has Stream.STDERR");
                        }
                        hasSTDERR = true;
                        break;
                    case UNSPECIFIED:
                        break;
                }
                hasStreams = hasStreams || !par.stream().equals(StdIOStream.UNSPECIFIED);
                hasPrefixes = hasPrefixes || !par.prefix().equals(Constants.PREFIX_EMPTY);
                // Check parameter annotation (warnings and errors)
                checkParameterAnnotation(m, par, i, hasNonNative);
            }
        }
        calleeMethodSignature.append(")");

        boolean[] hasAnnotation = { hasStreams,
            hasPrefixes };
        return hasAnnotation;
    }

    /**
     * Infers the type of a parameter. If the parameter is annotated as a FILE or a STRING, the type is taken from the
     * annotation. If the annotation is UNSPECIFIED, the type is taken from the formal type.
     *
     * @param formalType Formal type of the parameter
     * @param annotType Annotation type of the parameter
     * @return A String representing the type of the parameter
     */
    private static String inferType(Class<?> formalType, Type annotType) {
        if (annotType.equals(Type.UNSPECIFIED)) {
            if (formalType.isPrimitive()) {
                if (formalType.equals(boolean.class)) {
                    return "BOOLEAN_T";
                } else if (formalType.equals(char.class)) {
                    return "CHAR_T";
                } else if (formalType.equals(byte.class)) {
                    return "BYTE_T";
                } else if (formalType.equals(short.class)) {
                    return "SHORT_T";
                } else if (formalType.equals(int.class)) {
                    return "INT_T";
                } else if (formalType.equals(long.class)) {
                    return "LONG_T";
                } else if (formalType.equals(float.class)) {
                    return "FLOAT_T";
                } else {
                    // Type is double --> formalType.equals(double.class)
                    return "DOUBLE_T";
                }
            } else {
                // Object
                return "OBJECT_T";
            }
        } else {
            return annotType + "_T";
        }
    }

    /**
     * Treats and display errors and warning related to the annotation of 1 parameter of a method/service.
     *
     * @param m The method or service to be checked for warnings.
     * @param par The parameter to analyze.
     * @param i The position of the parameter (0 for the first parameter, 1 for the second, etc.).
     * @param hasNonNative Indicates if the method has non-native annotations or not.
     */
    private static void checkParameterAnnotation(java.lang.reflect.Method m, Parameter par, int i,
        boolean hasNonNative) {

        final String warningLocation = "In parameter number " + (i + 1) + " of method '" + m.getName()
            + "' in interface '" + m.getDeclaringClass().toString().replace("interface ", "") + "'.";

        Type annotType = par.type();
        Direction annotDirection = par.direction();
        StdIOStream stream = par.stream();

        boolean isOut = annotDirection.equals(Direction.OUT);
        boolean isInOut = annotDirection.equals(Direction.INOUT);

        /*
         * Type checks
         */
        if (annotType.equals(Type.STRING)) {
            // Strings are immutable
            if (isOut || isInOut) {
                ErrorManager.warn("Can't specify a String with direction OUT/INOUT since they are immutable."
                    + ErrorManager.NEWLINE + warningLocation + ErrorManager.NEWLINE + "Using direction=IN instead.");
            }
        } else if (m.getParameterTypes()[i].isPrimitive()) {
            // Primitive types are immutable (int, boolean, long, float, char, byte, short, double)
            if (isOut || isInOut) {
                String primType = m.getParameterTypes()[i].getName();
                ErrorManager.warn("Can't specify a primitive type ('" + primType + "') with direction OUT/INOUT, "
                    + "since they are always passed by value. " + ErrorManager.NEWLINE + warningLocation
                    + ErrorManager.NEWLINE + "Using direction=IN instead.");
            }
        } else if (annotType.equals(Type.OBJECT)) {
            // Objects are not supported as OUT parameters
            if (isOut) {
                ErrorManager.warn("Can't specify an Object with direction OUT." + ErrorManager.NEWLINE + warningLocation
                    + ErrorManager.NEWLINE + "Using direction=INOUT instead.");
            }
        }

        /*
         * Non native tasks only support FILES and STREAMS as INOUT/OUT parameters
         */
        if (hasNonNative) {
            if (!annotType.equals(Type.FILE) && !annotType.equals(Type.STREAM)) {
                if (isOut || isInOut) {
                    ErrorManager.error("Non-Native tasks only supports " + annotType.name() + " types in mode IN"
                        + ErrorManager.NEWLINE + warningLocation);
                }
            }
        }

        /*
         * Std IO Stream checks
         */
        if (!stream.equals(StdIOStream.UNSPECIFIED)) {
            // Stream parameters can only be files
            if (!annotType.equals(Type.FILE)) {
                ErrorManager.error(
                    "Can't specify an Stream with type different than File." + ErrorManager.NEWLINE + warningLocation);
            }

            switch (stream) {
                case STDIN:
                    if (isOut || isInOut) {
                        ErrorManager
                            .error("Stream STDIN must have direction IN" + ErrorManager.NEWLINE + warningLocation);
                    }
                    break;
                case STDOUT:
                    if (!isOut && !isInOut) {
                        ErrorManager.error(
                            "Stream STDOUT must have direction OUT or INOUT" + ErrorManager.NEWLINE + warningLocation);
                    }
                    break;
                case STDERR:
                    if (!isOut && !isInOut) {
                        ErrorManager.error(
                            "Stream STDERR must have direction OUT or INOUT" + ErrorManager.NEWLINE + warningLocation);
                    }
                    break;
                case UNSPECIFIED:
                    // We never reach this point since the previous if protects this case
                    break;

            }
        }
    }

    /**
     * Check all the defined implementations of the same method.
     *
     * @param runtime Runtime where to register the implementation
     * @param m Method.
     * @param ceSignature Callee method signature.
     * @param hasStreams Whether the method has StdIO Streams or not.
     * @param hasPrefixes Whether the method has StdIO Prefixes or not.
     */
    private static void defineImplementation(COMPSsRuntime runtime, java.lang.reflect.Method m, String ceSignature,
        boolean hasStreams, boolean hasPrefixes) {

        /*
         * Global constraints of the method
         */
        Constraints globalConstraints = null;
        boolean processLocalGeneral = false;
        if (m.isAnnotationPresent(Constraints.class)) {
            globalConstraints = m.getAnnotation(Constraints.class);
            processLocalGeneral = globalConstraints.isLocal();
        }

        String[] prolog = null;
        if (m.isAnnotationPresent(Prolog.class)) {
            Prolog pAnnot = m.getAnnotation(Prolog.class);
            prolog = new String[] { pAnnot.binary(),
                pAnnot.params(),
                Boolean.toString(pAnnot.failByExitValue()) };
        }

        String[] epilog = null;
        if (m.isAnnotationPresent(Epilog.class)) {
            Epilog eAnnot = m.getAnnotation(Epilog.class);
            epilog = new String[] { eAnnot.binary(),
                eAnnot.params(),
                Boolean.toString(eAnnot.failByExitValue()) };
        }

        // so far container within other decorators is only supported with Python @mpi and @mpmd_mpi. this is the case
        // where
        // the command doesn't start with the container but with "mpi" or something similar
        String[] container = null;

        /*
         * Check all annotations present at the method for versioning
         */
        String methodName = m.getName();

        /*
         * METHOD
         */
        for (Method methodAnnot : m.getAnnotationsByType(Method.class)) {
            LOGGER.debug("   * Processing @Method annotation");

            // Warning for ignoring streams
            if (hasStreams) {
                ErrorManager.warn(
                    "Java method " + methodName + " does not support stream annotations. SKIPPING stream annotation");
            }

            // Warning for ignoring prefixes
            if (hasPrefixes) {
                ErrorManager.warn(
                    "Java method " + methodName + " does not support prefix annotations. SKIPPING prefix annotation");
            }

            String declaringClass = methodAnnot.declaringClass();
            String methodSignature = ceSignature + declaringClass;
            addImplementation(runtime, ceSignature, MethodType.METHOD.toString(), methodSignature, processLocalGeneral,
                globalConstraints, methodAnnot.constraints(), prolog, epilog, container, Lang.JAVA.name(),
                declaringClass, methodName);
        }

        /*
         * HTTP
         */
        for (HTTP hAnno : m.getAnnotationsByType(HTTP.class)) {
            LOGGER.debug("   * Processing @HTTP annotation");

            // Warning for ignoring streams
            if (hasStreams) {
                ErrorManager.warn(
                    "Java HTTP " + methodName + " does not support stream annotations. SKIPPING stream annotation");
            }

            String declaringClass = hAnno.declaringClass();
            String httpSignature = ceSignature + declaringClass;

            runtime.registerCoreElement(ceSignature, httpSignature, null, TaskType.HTTP.toString(), false, false,
                prolog, epilog, container, hAnno.serviceName(), hAnno.resource(), hAnno.request(), hAnno.payload(),
                hAnno.payloadType(), hAnno.produces(), hAnno.updates(), hAnno.defReturn());

        }

        /*
         * CONTAINER
         */
        for (Container containerAnnot : m.getAnnotationsByType(Container.class)) {
            final String engine = EnvironmentLoader.loadFromEnvironment(containerAnnot.engine());
            final String image = EnvironmentLoader.loadFromEnvironment(containerAnnot.image());
            if (image == null || image.isEmpty() || image.equals(Constants.UNASSIGNED)) {
                ErrorManager.error("Empty image annotation for method " + m.getName());
            }
            final String options = EnvironmentLoader.loadFromEnvironment(containerAnnot.options());
            String internalExecutionTypeStr = EnvironmentLoader.loadFromEnvironment(containerAnnot.executionType());
            internalExecutionTypeStr = internalExecutionTypeStr.toUpperCase();
            ContainerExecutionType internalExecutionType = null;
            try {
                internalExecutionType = ContainerExecutionType.valueOf(internalExecutionTypeStr);
            } catch (IllegalArgumentException iae) {
                ErrorManager.error("Invalid container internal execution type for method " + m.getName());
            }
            final String internalBinary = EnvironmentLoader.loadFromEnvironment(containerAnnot.binary());
            final String internalParams = EnvironmentLoader.loadFromEnvironment(containerAnnot.args());
            final String internalFunc = EnvironmentLoader.loadFromEnvironment(containerAnnot.function());

            final String hostDir = EnvironmentLoader.loadFromEnvironment(containerAnnot.workingDir());
            final String containerFailByExitValue =
                EnvironmentLoader.loadFromEnvironment(containerAnnot.failByExitValue());

            switch (internalExecutionType) {
                case CET_BINARY:
                    if (internalBinary == null || internalBinary.isEmpty()
                        || internalBinary.equals(Constants.UNASSIGNED)) {
                        ErrorManager.error("Empty binary annotation for method " + m.getName());
                    }
                    break;
                case CET_PYTHON:
                    if (internalFunc == null || internalFunc.isEmpty() || internalFunc.equals(Constants.UNASSIGNED)) {
                        ErrorManager.error("Empty function annotation for method " + m.getName());
                    }
                    break;
            }

            // Load signature
            String containerSignature = ceSignature + ContainerDefinition.SIGNATURE;
            addImplementation(runtime, ceSignature, MethodType.CONTAINER.toString(), containerSignature,
                processLocalGeneral, globalConstraints, containerAnnot.constraints(), prolog, epilog, container, engine,
                image, options, internalExecutionTypeStr, internalBinary, internalParams, internalFunc, hostDir,
                containerFailByExitValue);
        }

        /*
         * BINARY
         */
        for (Binary binaryAnnot : m.getAnnotationsByType(Binary.class)) {
            String binary = EnvironmentLoader.loadFromEnvironment(binaryAnnot.binary());
            if (binary == null || binary.isEmpty() || binary.equals(Constants.UNASSIGNED)) {
                ErrorManager.error("Empty binary annotation for method " + m.getName());
            }
            String workingDir = EnvironmentLoader.loadFromEnvironment(binaryAnnot.workingDir());
            String params = EnvironmentLoader.loadFromEnvironment(binaryAnnot.args());
            String failByEVstr = EnvironmentLoader.loadFromEnvironment(binaryAnnot.failByExitValue());

            String binarySignature = ceSignature + BinaryDefinition.SIGNATURE;
            addImplementation(runtime, ceSignature, MethodType.BINARY.toString(), binarySignature, processLocalGeneral,
                globalConstraints, binaryAnnot.constraints(), prolog, epilog, container, binary, workingDir, params,
                failByEVstr);
        }

        /*
         * MPI
         */
        for (MPI mpiAnnot : m.getAnnotationsByType(MPI.class)) {
            LOGGER.debug("   * Processing @MPI annotation");

            final String binary = EnvironmentLoader.loadFromEnvironment(mpiAnnot.binary());
            if (binary == null || binary.isEmpty()) {
                ErrorManager.error("Empty binary annotation for method " + m.getName());
            }

            final String workingDir = EnvironmentLoader.loadFromEnvironment(mpiAnnot.workingDir());
            final String mpiRunner = EnvironmentLoader.loadFromEnvironment(mpiAnnot.mpiRunner());
            if (mpiRunner == null || mpiRunner.isEmpty()) {
                ErrorManager.error("Empty mpiRunner annotation for method " + m.getName());
            }
            final String mpiPPN = EnvironmentLoader.loadFromEnvironment(mpiAnnot.processesPerNode());
            final String mpiFlags = EnvironmentLoader.loadFromEnvironment(mpiAnnot.mpiFlags());
            final String scaleByCUStr = Boolean.toString(mpiAnnot.scaleByCU());
            final String params = EnvironmentLoader.loadFromEnvironment(mpiAnnot.args());
            final String failByEVstr = Boolean.toString(mpiAnnot.failByExitValue());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Binary: " + binary);
                LOGGER.debug("mpiRunner: " + mpiRunner);
            }

            String mpiSignature = ceSignature + MPIDefinition.SIGNATURE;
            addImplementation(runtime, ceSignature, MethodType.MPI.toString(), mpiSignature, processLocalGeneral,
                globalConstraints, mpiAnnot.constraints(), prolog, epilog, container, binary, workingDir, mpiRunner,
                mpiPPN, mpiFlags, scaleByCUStr, params, failByEVstr);
        }

        /*
         * Decaf
         */
        for (Decaf decafAnnot : m.getAnnotationsByType(Decaf.class)) {
            LOGGER.debug("   * Processing @DECAF annotation");

            final String dfScript = EnvironmentLoader.loadFromEnvironment(decafAnnot.dfScript());
            final String dfExecutor = EnvironmentLoader.loadFromEnvironment(decafAnnot.dfExecutor());
            final String dfLib = EnvironmentLoader.loadFromEnvironment(decafAnnot.dfLib());
            if (dfScript == null || dfScript.isEmpty()) {
                ErrorManager.error("Empty binary annotation for method " + m.getName());
            }
            final String workingDir = EnvironmentLoader.loadFromEnvironment(decafAnnot.workingDir());
            final String mpiRunner = EnvironmentLoader.loadFromEnvironment(decafAnnot.mpiRunner());
            if (mpiRunner == null || mpiRunner.isEmpty()) {
                ErrorManager.error("Empty mpiRunner annotation for method " + m.getName());
            }
            final String failByEVstr = Boolean.toString(decafAnnot.failByExitValue());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("DF Script: " + dfScript);
                LOGGER.debug("DF Executor: " + dfExecutor);
                LOGGER.debug("DF Lib: " + dfLib);
                LOGGER.debug("mpiRunner: " + mpiRunner);
            }

            String decafSignature = ceSignature + DecafDefinition.SIGNATURE;

            addImplementation(runtime, ceSignature, MethodType.DECAF.toString(), decafSignature, processLocalGeneral,
                globalConstraints, decafAnnot.constraints(), prolog, epilog, container, dfScript, dfExecutor, dfLib,
                workingDir, mpiRunner, failByEVstr);
        }

        /*
         * COMPSs
         */
        for (COMPSs compssAnnot : m.getAnnotationsByType(COMPSs.class)) {
            LOGGER.debug("   * Processing @COMPSs annotation");

            String runcompss = EnvironmentLoader.loadFromEnvironment(compssAnnot.runcompss());
            String flags = EnvironmentLoader.loadFromEnvironment(compssAnnot.flags());
            String workerInMaster = EnvironmentLoader.loadFromEnvironment(compssAnnot.workerInMaster());
            String appName = EnvironmentLoader.loadFromEnvironment(compssAnnot.appName());
            String appArgs = EnvironmentLoader.loadFromEnvironment(compssAnnot.appArgs());
            String workingDir = EnvironmentLoader.loadFromEnvironment(compssAnnot.workingDir());
            String failByEVstr = Boolean.toString(compssAnnot.failByExitValue());

            if (appName == null || appName.isEmpty()) {
                ErrorManager.error("Empty appName in COMPSs annotation for method " + m.getName());
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("runcompss: " + runcompss);
                LOGGER.debug("flags: " + flags);
                LOGGER.debug("workerInMaster: " + workerInMaster);
                LOGGER.debug("appName: " + appName);
                LOGGER.debug("appArgs: " + appArgs);
            }

            String compssSignature = ceSignature + COMPSsDefinition.SIGNATURE;
            addImplementation(runtime, ceSignature, MethodType.COMPSs.toString(), compssSignature, processLocalGeneral,
                globalConstraints, compssAnnot.constraints(), prolog, epilog, container, runcompss, flags, appName,
                appArgs, workerInMaster, workingDir, failByEVstr);

        }
        /*
         * MultiNode
         */
        for (MultiNode multiNodeAnnot : m.getAnnotationsByType(MultiNode.class)) {
            LOGGER.debug("   * Processing @MultiNode annotation");

            // Warning for ignoring streams
            if (hasStreams) {
                ErrorManager.warn("Java multi-node method " + methodName
                    + " does not support stream annotations. SKIPPING stream annotation");
            }

            // Warning for ignoring prefixes
            if (hasPrefixes) {
                ErrorManager.warn("Java multi-node method " + methodName
                    + " does not support prefix annotations. SKIPPING prefix annotation");
            }

            String declaringClass = multiNodeAnnot.declaringClass();
            String methodSignature = ceSignature + declaringClass;

            addImplementation(runtime, ceSignature, MethodType.MULTI_NODE.toString(), methodSignature,
                processLocalGeneral, globalConstraints, multiNodeAnnot.constraints(), prolog, epilog, container,
                Lang.JAVA.name(), declaringClass, methodName, multiNodeAnnot.processesPerNode());
        }

        /*
         * OMPSS
         */
        for (OmpSs ompssAnnot : m.getAnnotationsByType(OmpSs.class)) {
            LOGGER.debug("   * Processing @OmpSs annotation");
            String binary = EnvironmentLoader.loadFromEnvironment(ompssAnnot.binary());
            String workingDir = EnvironmentLoader.loadFromEnvironment(ompssAnnot.workingDir());
            String failByEVstr = Boolean.toString(ompssAnnot.failByExitValue());
            if (binary == null || binary.isEmpty()) {
                ErrorManager.error("Empty binary annotation for method " + m.getName());
            }

            String ompssSignature = ceSignature + OmpSsDefinition.SIGNATURE;

            addImplementation(runtime, ceSignature, MethodType.OMPSS.toString(), ompssSignature, processLocalGeneral,
                globalConstraints, ompssAnnot.constraints(), prolog, epilog, container, binary, workingDir,
                failByEVstr);
        }

        /*
         * OPENCL
         */
        for (OpenCL openclAnnot : m.getAnnotationsByType(OpenCL.class)) {
            LOGGER.debug("   * Processing @OpenCL annotation");
            String kernel = EnvironmentLoader.loadFromEnvironment(openclAnnot.kernel());
            String workingDir = EnvironmentLoader.loadFromEnvironment(openclAnnot.workingDir());

            if (kernel == null || kernel.isEmpty()) {
                ErrorManager.error("Empty kernel annotation for method " + m.getName());
            }

            String openclSignature = ceSignature + OpenCLDefinition.SIGNATURE;

            addImplementation(runtime, ceSignature, MethodType.OPENCL.toString(), openclSignature, processLocalGeneral,
                globalConstraints, openclAnnot.constraints(), prolog, epilog, container, kernel, workingDir);
        }
    }

    private static void addImplementation(COMPSsRuntime runtime, String ceSignature, String implType,
        String implSignature, boolean defaultIsLocal, Constraints globalConstraints, Constraints implConstraints,
        String[] prolog, String[] epilog, String[] container, String... typeArgs) {
        // Merge constraints into a string representation
        boolean isLocal = defaultIsLocal;

        HashMap<String, Object> constraintsMap = new HashMap<>();
        if (globalConstraints != null) {
            populateConstraintsMap(globalConstraints, constraintsMap);
        }
        if (implConstraints != null) {
            isLocal = isLocal || implConstraints.isLocal();
            populateConstraintsMap(implConstraints, constraintsMap);
        }

        StringBuilder constraintsBuilder = new StringBuilder();
        for (Map.Entry<String, Object> entry : constraintsMap.entrySet()) {
            constraintsBuilder.append(entry.getKey()).append(":").append(entry.getValue().toString()).append(";");
        }

        String constraints = constraintsBuilder.toString();
        // Register core element with string-based constraints
        runtime.registerCoreElement(ceSignature, implSignature, constraints, implType, Boolean.toString(isLocal),
            "false", prolog, epilog, container, typeArgs);
    }


    private static class Processor {

        private final Map<String, String> attributes = new HashMap<>();


        private String getAttribute(String name) {
            return attributes.get(name);
        }

        private void setAttribute(String name, String value) {
            attributes.put(name, value);
        }

        private boolean isUndefined() {
            return attributes.isEmpty();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("{");
            Iterator<Map.Entry<String, String>> entries = attributes.entrySet().iterator();
            if (entries.hasNext()) {
                Map.Entry<String, String> entry = entries.next();
                sb.append(entry.getKey()).append(":").append(entry.getValue());
                while (entries.hasNext()) {
                    entry = entries.next();
                    sb.append(",").append(entry.getKey()).append(":").append(entry.getValue());
                }
            }
            sb.append("}");
            return sb.toString();
        }
    }

    private static class ProcessorList {

        private final List<Processor> processors = new LinkedList<>();


        private void addProcessor(Processor proc, String cus) {
            if (!proc.isUndefined()) {
                if (cus == null) {
                    cus = "1";
                    proc.setAttribute(COMPUTING_UNITS, "1");
                }
            }

            if (cus != null) {
                try {
                    if (Integer.parseInt(cus) > 0) {
                        processors.add(proc);
                    }
                } catch (NumberFormatException nfe) {
                    // Env variable value
                    processors.add(proc);
                }
            }
        }

        private void mergeList(ProcessorList processors2) {
            for (Processor processor : processors2.processors) {
                processors.add(processor);
            }
        }

        private int getSize() {
            return processors.size();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("[");
            Iterator<Processor> procsIter = processors.iterator();
            if (procsIter.hasNext()) {
                Processor proc = procsIter.next();
                sb.append(proc.toString());
                while (procsIter.hasNext()) {
                    proc = procsIter.next();
                    sb.append(",").append(proc.toString());
                }
            }
            sb.append("]");
            return sb.toString();
        }
    }


    private static void appendProcessor(es.bsc.compss.types.annotations.Processor p, ProcessorList processors) {
        Processor proc = new Processor();
        String cus = null;
        if (Constants.UNASSIGNED.compareTo(p.computingUnits()) != 0) {
            cus = p.computingUnits();
            proc.setAttribute(COMPUTING_UNITS, p.computingUnits());
        }
        if (Constants.UNASSIGNED.compareTo(p.name()) != 0) {
            proc.setAttribute(PROC_NAME, p.name());
        }
        if (Constants.UNASSIGNED.compareTo(p.speed()) != 0) {
            proc.setAttribute(PROC_SPEED, p.speed());
        }
        if (Constants.UNASSIGNED.compareTo(p.architecture()) != 0) {
            proc.setAttribute(PROC_ARCH, p.architecture());
        }
        if (Constants.UNASSIGNED.compareTo(p.type()) != 0) {
            proc.setAttribute(PROC_TYPE, p.type());
        }
        if (Constants.UNASSIGNED.compareTo(p.internalMemorySize()) != 0) {
            proc.setAttribute(PROC_MEM_SIZE, p.internalMemorySize());
        }
        if (Constants.UNASSIGNED.compareTo(p.propertyName()) != 0) {
            proc.setAttribute(PROC_PROP_NAME, p.propertyName());
        }
        if (Constants.UNASSIGNED.compareTo(p.propertyValue()) != 0) {
            proc.setAttribute(PROC_PROP_VALUE, p.propertyValue());
        }
        processors.addProcessor(proc, cus);
    }

    private static void appendProcessor(Constraints constraints, ProcessorList processors) {
        Processor defaultProcessor = new Processor();
        String defaultCUs = null;
        if (Constants.UNASSIGNED.compareTo(constraints.computingUnits()) != 0) {
            defaultCUs = constraints.computingUnits();
            defaultProcessor.setAttribute(COMPUTING_UNITS, constraints.computingUnits());
        }

        if (Constants.UNASSIGNED.compareTo(constraints.processorName()) != 0) {
            defaultProcessor.setAttribute(PROC_NAME, constraints.processorName());
        }

        if (Constants.UNASSIGNED.compareTo(constraints.processorSpeed()) != 0) {
            defaultProcessor.setAttribute(PROC_SPEED, constraints.processorSpeed());
        }

        if (Constants.UNASSIGNED.compareTo(constraints.processorArchitecture()) != 0) {
            defaultProcessor.setAttribute(PROC_ARCH, constraints.processorArchitecture());
        }

        if (Constants.UNASSIGNED.compareTo(constraints.processorInternalMemorySize()) != 0) {
            defaultProcessor.setAttribute(PROC_MEM_SIZE, constraints.processorInternalMemorySize());
        }

        if (Constants.UNASSIGNED_PROCESSOR_TYPE.compareTo(constraints.processorType()) != 0) {
            defaultProcessor.setAttribute(PROC_TYPE, constraints.processorType());
        }

        if (Constants.UNASSIGNED.compareTo(constraints.processorPropertyName()) != 0) {
            defaultProcessor.setAttribute(PROC_PROP_NAME, constraints.processorPropertyName());
        }

        if (Constants.UNASSIGNED.compareTo(constraints.processorPropertyValue()) != 0) {
            defaultProcessor.setAttribute(PROC_PROP_VALUE, constraints.processorPropertyValue());
        }

        processors.addProcessor(defaultProcessor, defaultCUs);
    }

    private static Processor lookForProcessorType(ProcessorList processors, String type) {
        for (Processor p : processors.processors) {
            if (type == null) {
                if (p.getAttribute(PROC_TYPE) == null) {
                    return p;
                }
            } else {
                if (type.compareTo(p.getAttribute(PROC_TYPE)) == 0) {
                    return p;
                }

            }
        }
        return null;
    }

    private static void mergeProcessorLists(ProcessorList processors, ProcessorList oldProcessors) {
        for (Processor newProc : processors.processors) {
            Processor oldProc = lookForProcessorType(oldProcessors, newProc.getAttribute(PROC_TYPE));
            if (oldProc != null) {
                oldProc.attributes.putAll(newProc.attributes);
            } else {
                oldProcessors.processors.add(newProc);
            }
        }
    }

    private static void populateConstraintsMap(Constraints constraints, Map<String, Object> map) {
        ProcessorList processors = new ProcessorList();
        for (es.bsc.compss.types.annotations.Processor p : constraints.processors()) {
            appendProcessor(p, processors);
        }
        appendProcessor(constraints, processors);

        ProcessorList oldProcessors = (ProcessorList) map.get(PROCESSORS);
        if (oldProcessors != null && oldProcessors.getSize() > 0) {
            mergeProcessorLists(processors, oldProcessors);
        } else {
            map.put(PROCESSORS, processors);
        }

        // Memory
        if (Constants.UNASSIGNED.compareTo(constraints.memorySize()) != 0) {
            map.put(MEM_SIZE, constraints.memorySize());
        }

        if (Constants.UNASSIGNED.compareTo(constraints.memoryType()) != 0) {
            map.put(MEM_TYPE, constraints.memoryType());
        }

        // Storage
        if (Constants.UNASSIGNED.compareTo(constraints.storageType()) != 0) {
            map.put(STORAGE_TYPE, constraints.storageType());
        }

        if (Constants.UNASSIGNED.compareTo(constraints.storageSize()) != 0) {
            map.put(STORAGE_SIZE, constraints.storageSize());
        }

        if (Constants.UNASSIGNED.compareTo(constraints.storageBW()) != 0) {
            map.put(STORAGE_BW, constraints.storageBW());
        }

        // OS
        if (Constants.UNASSIGNED.compareTo(constraints.operatingSystemType()) != 0) {
            map.put(OS_TYPE, constraints.operatingSystemType());
        }

        if (Constants.UNASSIGNED.compareTo(constraints.operatingSystemDistribution()) != 0) {
            map.put(OS_DISTRIBUTION, constraints.operatingSystemDistribution());
        }

        if (Constants.UNASSIGNED.compareTo(constraints.operatingSystemVersion()) != 0) {
            map.put(OS_VERSION, constraints.operatingSystemVersion());
        }

        // Software
        if (Constants.UNASSIGNED.compareTo(constraints.appSoftware()) != 0) {
            String oldApps = (String) map.get(APP_SOFTWARE);
            if (oldApps != null) {
                Collection<String> software = new HashSet<>();
                String[] constApps = oldApps.split(",");
                Collections.addAll(software, constApps);
                constApps = constraints.appSoftware().split(",");
                Collections.addAll(software, constApps);
                String joinedApps = String.join(",", software);
                map.put(APP_SOFTWARE, joinedApps);
            } else {
                map.put(APP_SOFTWARE, constraints.appSoftware());
            }
        }

        // HostQueues
        if (Constants.UNASSIGNED.compareTo(constraints.hostQueues()) != 0) {
            String oldQueues = (String) map.get(HOST_QUEUES);
            if (oldQueues != null) {
                Collection<String> queues = new HashSet<>();
                String[] constQueues = oldQueues.split(",");
                Collections.addAll(queues, constQueues);
                constQueues = constraints.hostQueues().split(",");
                Collections.addAll(queues, constQueues);
                String joinedApps = String.join(",", queues);
                map.put(HOST_QUEUES, joinedApps);
            } else {
                map.put(HOST_QUEUES, constraints.hostQueues());
            }
        }

        if (Constants.UNASSIGNED.compareTo(constraints.wallClockLimit()) != 0) {
            map.put(WALL_CLOCK_LIMIT, constraints.wallClockLimit());
        }

    }

}
