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
package es.bsc.compss.util.parsers;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CoreElementDefinition;
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
import es.bsc.compss.types.implementations.ExecType;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.implementations.definition.BinaryDefinition;
import es.bsc.compss.types.implementations.definition.COMPSsDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDefinition.ContainerExecutionType;
import es.bsc.compss.types.implementations.definition.ContainerDescription;
import es.bsc.compss.types.implementations.definition.DecafDefinition;
import es.bsc.compss.types.implementations.definition.MPIDefinition;
import es.bsc.compss.types.implementations.definition.OmpSsDefinition;
import es.bsc.compss.types.implementations.definition.OpenCLDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.EnvironmentLoader;
import es.bsc.compss.util.ErrorManager;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ITFParser {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    /**
     * Loads the annotated class and initializes the data structures that contain the constraints. For each method found
     * in the annotated interface creates its signature and adds the constraints to the structures.
     *
     * @param annotItfClass package and name of the Annotated Interface class
     * @return
     */
    public static List<CoreElementDefinition> parseITFMethods(Class<?> annotItfClass) {
        List<CoreElementDefinition> updatedMethods = new LinkedList<>();

        int coreCount = annotItfClass.getDeclaredMethods().length;
        if (DEBUG) {
            LOGGER.debug("Detected methods " + coreCount);
        }

        // Check registered methods
        for (java.lang.reflect.Method m : annotItfClass.getDeclaredMethods()) {
            LOGGER.debug("Method = " + m);
            CoreElementDefinition ced = parseITFMethod(m);
            if (!ced.getImplementations().isEmpty()) {
                updatedMethods.add(ced);
            }
        }

        // Sort them alphabetically to avoid different core colors in the same execution
        Collections.sort(updatedMethods);

        return updatedMethods;
    }

    /**
     * Parses a single ITF Method (can have multiple annotations).
     *
     * @param m Java lang method to parse.
     * @return The core element definition.
     */
    private static CoreElementDefinition parseITFMethod(java.lang.reflect.Method m) {
        // Computes the callee method signature and checks parameter annotations
        LOGGER.info("Evaluating method " + m.getName());

        StringBuilder calleeMethodSignature = new StringBuilder();
        String methodName = m.getName();
        calleeMethodSignature.append(methodName).append("(");

        // Check all annotations are valid
        checkMethodAnnotation(m);

        // Load if there is any non-native annotation or not
        boolean hasNonNative = checkNonNativeAnnotation(m);

        // Construct signature and check parameters
        boolean[] hasAnnotations = constructSignatureAndCheckParameters(m, hasNonNative, calleeMethodSignature);
        boolean hasStreams = hasAnnotations[0];
        boolean hasPrefixes = hasAnnotations[1];

        // Check all annotations present at the method for versioning
        if (DEBUG) {
            LOGGER.debug("   * Method method " + methodName + " has " + m.getAnnotations().length + " annotations");
        }
        CoreElementDefinition ced = new CoreElementDefinition();
        checkDefinedImplementations(m, calleeMethodSignature, hasStreams, hasPrefixes, ced);

        // Register all implementations
        ced.setCeSignature(calleeMethodSignature.toString());

        // Returns the assigned methodId
        return ced;
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
     * @param m Method.
     * @param calleeMethodSignature Callee method signature.
     * @param hasStreams Whether the method has StdIO Streams or not.
     * @param hasPrefixes Whether the method has StdIO Prefixes or not.
     * @param ced The CoreElement definition of the method.
     */
    private static void checkDefinedImplementations(java.lang.reflect.Method m, StringBuilder calleeMethodSignature,
        boolean hasStreams, boolean hasPrefixes, CoreElementDefinition ced) {

        /*
         * Global constraints of the method
         */
        MethodResourceDescription defaultConstraints = MethodResourceDescription.EMPTY_FOR_CONSTRAINTS.copy();
        boolean processLocalGeneral = false;
        if (m.isAnnotationPresent(Constraints.class)) {
            Constraints generalConstraints = m.getAnnotation(Constraints.class);
            processLocalGeneral = generalConstraints.isLocal();
            defaultConstraints = new MethodResourceDescription(generalConstraints);
        }

        ExecType prolog = null;
        if (m.isAnnotationPresent(Prolog.class)) {
            Prolog pAnnot = m.getAnnotation(Prolog.class);
            prolog = new ExecType(pAnnot.binary(), pAnnot.params(), pAnnot.failByExitValue());
        }

        ExecType epilog = null;
        if (m.isAnnotationPresent(Epilog.class)) {
            Epilog eAnnot = m.getAnnotation(Epilog.class);
            epilog = new ExecType(eAnnot.binary(), eAnnot.params(), eAnnot.failByExitValue());
        }

        // so far container within other decorators is only supported with Python @mpi and @mpmd_mpi. this is the case
        // where
        // the command doesn't start with the container but with "mpi" or something similar
        ContainerDescription container = null;

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
            String methodSignature = calleeMethodSignature.toString() + declaringClass;

            // Load specific method constraints if present
            MethodResourceDescription implConstraints = defaultConstraints;
            boolean implProcessLocal = processLocalGeneral;
            if (methodAnnot.constraints() != null) {
                Constraints implConstraintsAnnot = methodAnnot.constraints();
                implProcessLocal = processLocalGeneral || implConstraintsAnnot.isLocal();
                implConstraints = new MethodResourceDescription(implConstraintsAnnot);
                implConstraints.mergeMultiConstraints(defaultConstraints);
            }

            // Register method implementation
            ImplementationDescription<?, ?> implDef = null;
            try {
                implDef = ImplementationDescription.defineImplementation(MethodType.METHOD.toString(), methodSignature,
                    implProcessLocal, implConstraints, prolog, epilog, container, declaringClass, methodName);
            } catch (Exception e) {
                ErrorManager.error(e.getMessage());
            }
            ced.addImplementation(implDef);
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

            calleeMethodSignature.insert(0, hAnno.declaringClass() + ".");

            // Register HTTP implementation
            ImplementationDescription<?, ?> implDef = null;
            try {
                implDef = ImplementationDescription.defineImplementation(TaskType.HTTP.toString(),
                    calleeMethodSignature.toString(), false, null, prolog, epilog, container, hAnno.serviceName(),
                    hAnno.resource(), hAnno.request(), hAnno.payload(), hAnno.payloadType(), hAnno.produces(),
                    hAnno.updates(), hAnno.defReturn());
            } catch (Exception e) {
                ErrorManager.error(e.getMessage());
            }
            ced.addImplementation(implDef);
        }

        /*
         * CONTAINER
         */
        for (Container containerAnnot : m.getAnnotationsByType(Container.class)) {
            String engine = EnvironmentLoader.loadFromEnvironment(containerAnnot.engine());
            String image = EnvironmentLoader.loadFromEnvironment(containerAnnot.image());
            String options = EnvironmentLoader.loadFromEnvironment(containerAnnot.options());
            String internalExecutionTypeStr = EnvironmentLoader.loadFromEnvironment(containerAnnot.executionType());
            String internalBinary = EnvironmentLoader.loadFromEnvironment(containerAnnot.binary());
            String internalParams = EnvironmentLoader.loadFromEnvironment(containerAnnot.args());
            String internalFunc = EnvironmentLoader.loadFromEnvironment(containerAnnot.function());

            String hostDir = EnvironmentLoader.loadFromEnvironment(containerAnnot.workingDir());
            String containerFailByExitValue = EnvironmentLoader.loadFromEnvironment(containerAnnot.failByExitValue());

            // Check parameters
            if (image == null || image.isEmpty() || image.equals(Constants.UNASSIGNED)) {
                ErrorManager.error("Empty image annotation for method " + m.getName());
            }

            internalExecutionTypeStr = internalExecutionTypeStr.toUpperCase();
            ContainerExecutionType internalExecutionType = null;
            try {
                internalExecutionType = ContainerExecutionType.valueOf(internalExecutionTypeStr);
            } catch (IllegalArgumentException iae) {
                ErrorManager.error("Invalid container internal execution type for method " + m.getName());
            }

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
            String containerSignature = calleeMethodSignature.toString() + ContainerDefinition.SIGNATURE;

            // Load specific method constraints if present
            MethodResourceDescription implConstraints = defaultConstraints;
            boolean implProcessLocal = processLocalGeneral;
            if (containerAnnot.constraints() != null) {
                Constraints implConstraintsAnnot = containerAnnot.constraints();
                implProcessLocal = processLocalGeneral || implConstraintsAnnot.isLocal();
                implConstraints = new MethodResourceDescription(implConstraintsAnnot);
                implConstraints.mergeMultiConstraints(defaultConstraints);
            }

            // Register container implementation
            ImplementationDescription<?, ?> implDef = null;
            try {
                implDef = ImplementationDescription.defineImplementation(MethodType.CONTAINER.toString(),
                    containerSignature, implProcessLocal, implConstraints, prolog, epilog, container, engine, image,
                    options, internalExecutionTypeStr, internalBinary, internalParams, internalFunc, hostDir,
                    containerFailByExitValue);
            } catch (Exception e) {
                ErrorManager.error(e.getMessage());
            }

            ced.addImplementation(implDef);
        }

        /*
         * BINARY
         */
        for (Binary binaryAnnot : m.getAnnotationsByType(Binary.class)) {
            String binary = EnvironmentLoader.loadFromEnvironment(binaryAnnot.binary());
            String workingDir = EnvironmentLoader.loadFromEnvironment(binaryAnnot.workingDir());
            String params = EnvironmentLoader.loadFromEnvironment(binaryAnnot.args());
            String failByEVstr = EnvironmentLoader.loadFromEnvironment(binaryAnnot.failByExitValue());

            if (binary == null || binary.isEmpty() || binary.equals(Constants.UNASSIGNED)) {
                ErrorManager.error("Empty binary annotation for method " + m.getName());
            }

            String binarySignature = calleeMethodSignature.toString() + BinaryDefinition.SIGNATURE;

            // Load specific method constraints if present
            MethodResourceDescription implConstraints = defaultConstraints;
            boolean implProcessLocal = processLocalGeneral;
            if (binaryAnnot.constraints() != null) {
                Constraints implConstraintsAnnot = binaryAnnot.constraints();
                implProcessLocal = processLocalGeneral || implConstraintsAnnot.isLocal();
                implConstraints = new MethodResourceDescription(implConstraintsAnnot);
                implConstraints.mergeMultiConstraints(defaultConstraints);
            }

            // Register binary implementation
            ImplementationDescription<?, ?> implDef = null;
            try {
                implDef = ImplementationDescription.defineImplementation(MethodType.BINARY.toString(), binarySignature,
                    implProcessLocal, implConstraints, prolog, epilog, container, binary, workingDir, params,
                    failByEVstr);
            } catch (Exception e) {
                ErrorManager.error(e.getMessage(), e);
            }

            ced.addImplementation(implDef);
        }

        /*
         * MPI
         */
        for (MPI mpiAnnot : m.getAnnotationsByType(MPI.class)) {
            LOGGER.debug("   * Processing @MPI annotation");

            String binary = EnvironmentLoader.loadFromEnvironment(mpiAnnot.binary());
            String workingDir = EnvironmentLoader.loadFromEnvironment(mpiAnnot.workingDir());
            String mpiRunner = EnvironmentLoader.loadFromEnvironment(mpiAnnot.mpiRunner());
            String mpiPPN = EnvironmentLoader.loadFromEnvironment(mpiAnnot.processesPerNode());
            String mpiFlags = EnvironmentLoader.loadFromEnvironment(mpiAnnot.mpiFlags());
            String scaleByCUStr = Boolean.toString(mpiAnnot.scaleByCU());
            String params = EnvironmentLoader.loadFromEnvironment(mpiAnnot.args());
            String failByEVstr = Boolean.toString(mpiAnnot.failByExitValue());

            if (mpiRunner == null || mpiRunner.isEmpty()) {
                ErrorManager.error("Empty mpiRunner annotation for method " + m.getName());
            }
            if (binary == null || binary.isEmpty()) {
                ErrorManager.error("Empty binary annotation for method " + m.getName());
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Binary: " + binary);
                LOGGER.debug("mpiRunner: " + mpiRunner);
            }

            String mpiSignature = calleeMethodSignature.toString() + MPIDefinition.SIGNATURE;

            // Load specific method constraints if present
            MethodResourceDescription implConstraints = defaultConstraints;
            boolean implProcessLocal = processLocalGeneral;
            if (mpiAnnot.constraints() != null) {
                Constraints implConstraintsAnnot = mpiAnnot.constraints();
                implProcessLocal = processLocalGeneral || implConstraintsAnnot.isLocal();
                implConstraints = new MethodResourceDescription(implConstraintsAnnot);
                implConstraints.mergeMultiConstraints(defaultConstraints);
            }

            // Register service implementation
            ImplementationDescription<?, ?> implDef = null;
            try {
                implDef = ImplementationDescription.defineImplementation(MethodType.MPI.toString(), mpiSignature,
                    implProcessLocal, implConstraints, prolog, epilog, container, binary, workingDir, mpiRunner, mpiPPN,
                    mpiFlags, scaleByCUStr, params, failByEVstr);
            } catch (Exception e) {
                ErrorManager.error(e.getMessage());
            }
            ced.addImplementation(implDef);
        }

        /*
         * Decaf
         */
        for (Decaf decafAnnot : m.getAnnotationsByType(Decaf.class)) {
            LOGGER.debug("   * Processing @DECAF annotation");

            String dfScript = EnvironmentLoader.loadFromEnvironment(decafAnnot.dfScript());
            String dfExecutor = EnvironmentLoader.loadFromEnvironment(decafAnnot.dfExecutor());
            String dfLib = EnvironmentLoader.loadFromEnvironment(decafAnnot.dfLib());
            String workingDir = EnvironmentLoader.loadFromEnvironment(decafAnnot.workingDir());
            String mpiRunner = EnvironmentLoader.loadFromEnvironment(decafAnnot.mpiRunner());
            String failByEVstr = Boolean.toString(decafAnnot.failByExitValue());

            if (mpiRunner == null || mpiRunner.isEmpty()) {
                ErrorManager.error("Empty mpiRunner annotation for method " + m.getName());
            }
            if (dfScript == null || dfScript.isEmpty()) {
                ErrorManager.error("Empty binary annotation for method " + m.getName());
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("DF Script: " + dfScript);
                LOGGER.debug("DF Executor: " + dfExecutor);
                LOGGER.debug("DF Lib: " + dfLib);
                LOGGER.debug("mpiRunner: " + mpiRunner);
            }

            String decafSignature = calleeMethodSignature.toString() + DecafDefinition.SIGNATURE;

            // Load specific method constraints if present
            MethodResourceDescription implConstraints = defaultConstraints;
            boolean implProcessLocal = processLocalGeneral;
            if (decafAnnot.constraints() != null) {
                Constraints implConstraintsAnnot = decafAnnot.constraints();
                implProcessLocal = processLocalGeneral || implConstraintsAnnot.isLocal();
                implConstraints = new MethodResourceDescription(implConstraintsAnnot);
                implConstraints.mergeMultiConstraints(defaultConstraints);
            }

            // Register service implementation
            ImplementationDescription<?, ?> implDef = null;
            try {
                implDef = ImplementationDescription.defineImplementation(MethodType.DECAF.toString(), decafSignature,
                    implProcessLocal, implConstraints, prolog, epilog, container, dfScript, dfExecutor, dfLib,
                    workingDir, mpiRunner, failByEVstr);
            } catch (Exception e) {
                ErrorManager.error(e.getMessage());
            }
            ced.addImplementation(implDef);
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

            String compssSignature = calleeMethodSignature.toString() + COMPSsDefinition.SIGNATURE;

            // Load specific method constraints if present
            MethodResourceDescription implConstraints = defaultConstraints;
            boolean implProcessLocal = processLocalGeneral;
            if (compssAnnot.constraints() != null) {
                Constraints implConstraintsAnnot = compssAnnot.constraints();
                implProcessLocal = processLocalGeneral || implConstraintsAnnot.isLocal();
                implConstraints = new MethodResourceDescription(implConstraintsAnnot);
                implConstraints.mergeMultiConstraints(defaultConstraints);
            }

            // Register service implementation
            ImplementationDescription<?, ?> implDef = null;
            try {
                implDef = ImplementationDescription.defineImplementation(MethodType.COMPSs.toString(), compssSignature,
                    implProcessLocal, implConstraints, prolog, epilog, container, runcompss, flags, appName, appArgs,
                    workerInMaster, workingDir, failByEVstr);
            } catch (Exception e) {
                ErrorManager.error(e.getMessage());
            }

            ced.addImplementation(implDef);
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
            String methodSignature = calleeMethodSignature.toString() + declaringClass;

            // Load specific method constraints if present
            MethodResourceDescription implConstraints = defaultConstraints;
            boolean implProcessLocal = processLocalGeneral;
            if (multiNodeAnnot.constraints() != null) {

                Constraints implConstraintsAnnot = multiNodeAnnot.constraints();
                implProcessLocal = processLocalGeneral || implConstraintsAnnot.isLocal();
                implConstraints = new MethodResourceDescription(implConstraintsAnnot);
                implConstraints.mergeMultiConstraints(defaultConstraints);
            }

            // Register method implementation
            ImplementationDescription<?, ?> implDef = null;
            try {
                implDef = ImplementationDescription.defineImplementation(MethodType.MULTI_NODE.toString(),
                    methodSignature, implProcessLocal, implConstraints, prolog, epilog, container, declaringClass,
                    methodName, multiNodeAnnot.processesPerNode());
            } catch (Exception e) {
                ErrorManager.error(e.getMessage());
            }
            ced.addImplementation(implDef);

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

            String ompssSignature = calleeMethodSignature.toString() + OmpSsDefinition.SIGNATURE;

            // Load specific method constraints if present
            MethodResourceDescription implConstraints = defaultConstraints;
            boolean implProcessLocal = processLocalGeneral;
            if (ompssAnnot.constraints() != null) {
                Constraints implConstraintsAnnot = ompssAnnot.constraints();
                implProcessLocal = processLocalGeneral || implConstraintsAnnot.isLocal();
                implConstraints = new MethodResourceDescription(implConstraintsAnnot);
                implConstraints.mergeMultiConstraints(defaultConstraints);
            }

            // Register service implementation
            ImplementationDescription<?, ?> implDef = null;
            try {
                implDef = ImplementationDescription.defineImplementation(MethodType.OMPSS.toString(), ompssSignature,
                    implProcessLocal, implConstraints, prolog, epilog, container, binary, workingDir, failByEVstr);
            } catch (Exception e) {
                ErrorManager.error(e.getMessage());
            }
            ced.addImplementation(implDef);
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

            String openclSignature = calleeMethodSignature.toString() + OpenCLDefinition.SIGNATURE;

            // Load specific method constraints if present
            MethodResourceDescription implConstraints = defaultConstraints;
            boolean implProcessLocal = processLocalGeneral;
            if (openclAnnot.constraints() != null) {
                Constraints implConstraintsAnnot = openclAnnot.constraints();
                implProcessLocal = processLocalGeneral || implConstraintsAnnot.isLocal();
                implConstraints = new MethodResourceDescription(implConstraintsAnnot);
                implConstraints.mergeMultiConstraints(defaultConstraints);
            }

            // Register service implementation
            ImplementationDescription<?, ?> implDef = null;
            try {
                implDef = ImplementationDescription.defineImplementation(MethodType.OPENCL.toString(), openclSignature,
                    implProcessLocal, implConstraints, prolog, epilog, container, kernel, workingDir);
            } catch (Exception e) {
                ErrorManager.error(e.getMessage());
            }

            ced.addImplementation(implDef);
        }
    }
}
