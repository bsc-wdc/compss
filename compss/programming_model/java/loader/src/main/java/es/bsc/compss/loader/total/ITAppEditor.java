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
package es.bsc.compss.loader.total;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.api.COMPSsGroup;
import es.bsc.compss.loader.LoaderConstants;
import es.bsc.compss.loader.LoaderUtils;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.SchedulerHints;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Binary;
import es.bsc.compss.types.annotations.task.COMPSs;
import es.bsc.compss.types.annotations.task.Decaf;
import es.bsc.compss.types.annotations.task.HTTP;
import es.bsc.compss.types.annotations.task.MPI;
import es.bsc.compss.types.annotations.task.MultiNode;
import es.bsc.compss.types.annotations.task.OmpSs;
import es.bsc.compss.types.annotations.task.OpenCL;
import es.bsc.compss.util.EnvironmentLoader;
import java.io.File;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import javassist.CannotCompileException;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.NotFoundException;
import javassist.expr.ExprEditor;
import javassist.expr.FieldAccess;
import javassist.expr.MethodCall;
import javassist.expr.NewExpr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ITAppEditor extends ExprEditor {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String ERROR_NO_EMPTY_CONSTRUCTOR = "ERROR: No empty constructor on object class ";

    // Inserted method calls
    private static final String EXECUTE_TASK = ".executeTask(";
    private static final String PROCEED = "$_ = $proceed(";
    private static final String COMPSS_LOADER_GROUP = "es.bsc.compss.loader.total.COMPSsGroupLoader(";

    private static final String DATA_TYPES = DataType.class.getCanonicalName();
    private static final String DATA_DIRECTION = Direction.class.getCanonicalName();
    private static final String DATA_STREAM = StdIOStream.class.getCanonicalName();

    private static final String LANG = Lang.class.getCanonicalName() + ".JAVA";

    private static final String CHECK_SCO_TYPE = "LoaderUtils.checkSCOType(";
    private static final String RUN_METHOD_ON_OBJECT = "LoaderUtils.runMethodOnObject(";

    // Pointers to internal variables
    private Method[] remoteMethods;
    private CtMethod[] instrCandidates; // methods that will be instrumented if they are not remote
    private String itApiVar;
    private String itSRVar;
    private String itORVar;
    private String itAppIdVar;
    private CtClass appClass;
    private String renamedClassName;
    private String originalClassName;


    /**
     * Modifies the current application to instrument the task methods with remote invocations.
     *
     * @param remoteMethods List of ITF remote methods.
     * @param instrCandidates List of detected methods in the main code.
     * @param itApiVar COMPSs API pointer.
     * @param itSRVar Stream Registry.
     * @param itORVar Object Registry pointer.
     * @param itAppIdVar COMPSs Application Id variable.
     * @param appClass Application main class.
     */
    public ITAppEditor(Method[] remoteMethods, CtMethod[] instrCandidates, String itApiVar, String itSRVar,
        String itORVar, String itAppIdVar, CtClass appClass, String originalClassName) {

        super();
        this.remoteMethods = remoteMethods;
        this.instrCandidates = instrCandidates;
        this.itApiVar = itApiVar;
        this.itSRVar = itSRVar;
        this.itORVar = itORVar;
        this.itAppIdVar = itAppIdVar;
        this.appClass = appClass;
        if (originalClassName != null) {
            this.renamedClassName = appClass.getName();
        } else {
            this.renamedClassName = null;
        }
        this.originalClassName = originalClassName;
    }

    /**
     * Returns the application class.
     *
     * @return The application class.
     */
    public CtClass getAppClass() {
        return this.appClass;
    }

    /**
     * Instruments the creation of objects streams and stream wrappers.
     *
     * @param ne New expression
     */
    @Override
    public void edit(NewExpr ne) throws CannotCompileException {
        String fullName = ne.getClassName();
        boolean isInternal = fullName.startsWith(LoaderConstants.LOADER_INTERNAL_PREFIX);
        boolean isIO = fullName.startsWith(LoaderConstants.LOADER_IO_PREFIX);

        // Only edit non-internal calls
        if (!isInternal) {
            StringBuilder modifiedExpr = new StringBuilder();
            StringBuilder callPars = new StringBuilder();
            StringBuilder toSerialize = new StringBuilder();
            try {
                CtClass[] paramTypes = ne.getConstructor().getParameterTypes();
                if (paramTypes.length > 0) {
                    int i = 1;
                    for (CtClass parType : paramTypes) {
                        if (i > 1) {
                            callPars.append(',');
                        }
                        String parId = "$" + (i++);
                        if (parType.isPrimitive()) {
                            callPars.append(parId);
                        } else { // Object (also array)
                            if (DEBUG) {
                                LOGGER.debug("Parameter " + (i - 1) + " of constructor " + ne.getConstructor()
                                    + " is an object, adding access");
                            }

                            String internalObject = CallGenerator.oRegGetInternalObject(itORVar, itAppIdVar, parId);
                            modifiedExpr.insert(0, CallGenerator.oRegNewObjectAccess(itORVar, itAppIdVar, parId) + ";");
                            callPars.append(internalObject).append(" == null ? ").append(parId).append(" : ")
                                .append("(" + parType.getName() + ")").append(internalObject);
                            toSerialize.append(CallGenerator.oRegSerializeLocally(itORVar, itAppIdVar, parId))
                                .append(";");
                        }
                    }
                }
            } catch (NotFoundException e) {
                throw new CannotCompileException(e);
            }

            if (isIO) {
                String className = fullName.substring(8);
                modifiedExpr.append(inspectCreation(className, callPars));
            } else {
                modifiedExpr.append(PROCEED).append(callPars).append(");");
                modifiedExpr.append(toSerialize);
            }

            if (DEBUG) {
                LOGGER.debug(
                    "Replacing regular constructor call of class " + fullName + " by " + modifiedExpr.toString());
            }

            // Update new expression
            ne.replace(modifiedExpr.toString());

        } else if (fullName.equals(COMPSsGroup.class.getCanonicalName())) {
            ne.replace(substitutesCOMPSsGroup());
        }
    }

    /**
     * Replaces calls to remote methods by calls to executeTask or black-boxes methods.
     */
    @Override
    public void edit(MethodCall mc) throws CannotCompileException {
        LOGGER.debug("---- BEGIN EDIT METHOD CALL " + mc.getMethodName() + " ----");

        Method declaredMethod = null;
        CtMethod calledMethod = null;
        try {
            calledMethod = mc.getMethod();
            declaredMethod = LoaderUtils.checkRemote(calledMethod, remoteMethods, originalClassName, renamedClassName);
        } catch (NotFoundException e) {
            throw new CannotCompileException(e);
        }

        if (declaredMethod != null) {
            // Current method must be executed remotely, change the call
            if (DEBUG) {
                LOGGER.debug("Replacing task method call " + mc.getMethodName());
            }

            String taskClassName = mc.getClassName();
            if (renamedClassName != null && taskClassName.equals(renamedClassName)) {
                taskClassName = originalClassName;
            }
            // Replace the call to the method by the call to executeTask
            String executeTask = replaceTaskMethodCall(mc.getMethodName(), taskClassName, declaredMethod, calledMethod);
            if (DEBUG) {
                LOGGER.debug("Replacing task method call by " + executeTask);
            }

            mc.replace(executeTask);
        } else if (LoaderUtils.isStreamClose(mc)) {
            if (DEBUG) {
                LOGGER.debug("Replacing close on a stream of class " + mc.getClassName());
            }

            // Close call on a stream
            // No need to instrument the stream object, assuming it will always be local
            String streamClose = replaceCloseStream();
            if (DEBUG) {
                LOGGER.debug("Replacing stream close by " + streamClose);
            }

            mc.replace(streamClose);
        } else if (LoaderUtils.isFileDelete(mc)) {
            if (DEBUG) {
                LOGGER.debug("Replacing delete file");
            }

            String deleteFile = replaceDeleteFile();
            if (DEBUG) {
                LOGGER.debug("Replacing delete file by " + deleteFile);
            }

            mc.replace(deleteFile);
        } else if (mc.getClassName().equals(LoaderConstants.CLASS_COMPSS_API)) {
            // The method is an API call
            if (DEBUG) {
                LOGGER.debug("Replacing API call " + mc.getMethodName());
            }

            String modifiedAPICall = replaceAPICall(mc.getMethodName(), calledMethod);
            if (DEBUG) {
                LOGGER.debug("Replacing API call by " + modifiedAPICall);
            }

            mc.replace(modifiedAPICall);
        } else if ((!mc.getClassName().equals(LoaderConstants.CLASS_COMPSS_GROUP))
            && (!LoaderUtils.contains(instrCandidates, calledMethod))) {
            // The method is a black box
            if (DEBUG) {
                LOGGER.debug("Replacing regular method call " + mc.getMethodName());
            }

            String modifiedCall = replaceBlackBox(mc.getMethodName(), mc.getClassName(), calledMethod);
            if (DEBUG) {
                LOGGER.debug("Replacing regular method call by " + modifiedCall);
            }

            mc.replace(modifiedCall);
        } else {
            // The method is an instrumented method, nothing to do
            if (DEBUG) {
                LOGGER.debug("Skipping instrumented method " + mc.getMethodName());
            }
        }

        LOGGER.debug("---- END EDIT METHOD CALL ----");
    }

    /**
     * Check the access to fields of objects.
     */
    @Override
    public void edit(FieldAccess fa) throws CannotCompileException {
        CtField field = null;
        try {
            field = fa.getField();
            if (Modifier.isStatic(field.getModifiers())) {
                return;
            }
        } catch (NotFoundException e) {
            throw new CannotCompileException(e);
        }
        String fieldName = field.getName();

        if (DEBUG) {
            LOGGER.debug(
                "Keeping track of access to field " + fieldName + " of class " + field.getDeclaringClass().getName());
        }

        boolean isWriter = fa.isWriter();

        // First check the object containing the field
        StringBuilder toInclude = new StringBuilder();
        toInclude.append(CallGenerator.oRegNewObjectAccess(itORVar, itAppIdVar, "$0", isWriter)).append(";");

        // Execute the access on the internal object
        String internalObject = CallGenerator.oRegGetInternalObject(itORVar, itAppIdVar, "$0");
        String objectClass = fa.getClassName();
        toInclude.append("if (").append(internalObject).append(" != null) {");
        if (isWriter) {
            // store a new value in the field
            toInclude.append("((").append(objectClass).append(')').append(internalObject).append(").").append(fieldName)
                .append(" = $1;");
            toInclude.append("} else { " + PROCEED + "$$); }");
            // Serialize the (internal) object locally after the access
            toInclude.append(CallGenerator.oRegSerializeLocally(itORVar, itAppIdVar, "$0")).append(";");
        } else {
            // read the field value
            toInclude.append("$_ = ((").append(objectClass).append(')').append(internalObject).append(").")
                .append(fieldName).append(';'); // read
            toInclude.append("} else { " + PROCEED + "$$); }");
        }

        fa.replace(toInclude.toString());

        if (DEBUG) {
            LOGGER.debug("Replaced regular field access by " + toInclude.toString());
        }
    }

    /**
     * Class creation inspection.
     */
    private String inspectCreation(String className, StringBuilder callPars) {
        String modifiedExpr = "";

        if (DEBUG) {
            LOGGER.debug("Inspecting the creation of an object of class " + className);
        }

        // $$ = pars separated by commas, $args = pars in an array of objects
        boolean found = false;
        for (String streamClass : LoaderConstants.getSupportedStreamTypes()) {
            if (className.equals(streamClass)) {
                modifiedExpr = "$_ = " + CallGenerator.newStreamClass(itSRVar, itAppIdVar, streamClass, callPars);
                found = true;
                break;
            }
        }
        if (!found) { // Not a stream
            if (className.equals(File.class.getCanonicalName())) {
                modifiedExpr = "$_ = " + CallGenerator.newCOMPSsFile(itSRVar, itAppIdVar, callPars);
            } else {
                String internalObject = CallGenerator.oRegGetInternalObject(itORVar, itAppIdVar, "$1");
                String par1 = internalObject + " == null ? (Object)$1 : " + internalObject;
                modifiedExpr = PROCEED + callPars + "); ";
                modifiedExpr += "if ($_ instanceof " + FilterInputStream.class.getCanonicalName() + " || $_ instanceof "
                    + FilterOutputStream.class.getCanonicalName() + ") {";
                modifiedExpr += CallGenerator.newFilterStream(this.itSRVar, this.itAppIdVar, par1);
            }
        }
        if (DEBUG) {
            LOGGER.debug("Modifiying creation of an object of class " + className + " with \"" + modifiedExpr + "\"");
        }
        return modifiedExpr;
    }

    private String substitutesCOMPSsGroup() {
        String modifiedExpr = "";
        if (DEBUG) {
            LOGGER.debug("Substituting COMPSs group creation. ");
        }
        modifiedExpr = "$_ = new  " + COMPSS_LOADER_GROUP + this.itApiVar + ", " + this.itAppIdVar + ", $$);";
        return modifiedExpr;
    }

    /**
     * Replaces calls to local methods by executeTask.
     */
    private String replaceTaskMethodCall(String methodName, String className, Method declaredMethod,
        CtMethod calledMethod) throws CannotCompileException {

        if (DEBUG) {
            LOGGER.debug("Found call to remote method " + methodName);
        }

        Class<?> retType = declaredMethod.getReturnType();
        boolean isVoid = retType.equals(void.class);
        boolean isStatic = Modifier.isStatic(calledMethod.getModifiers());
        Class<?>[] paramTypes = declaredMethod.getParameterTypes();
        int numParams = paramTypes.length;
        if (!isStatic) {
            numParams++;
        }
        if (!isVoid) {
            numParams++;
        }

        // Build the executeTask call string
        StringBuilder executeTask = new StringBuilder();
        executeTask.append(this.itApiVar).append(EXECUTE_TASK);
        executeTask.append(this.itAppIdVar).append(',');
        // Common values
        boolean isPrioritary = Boolean.parseBoolean(Constants.IS_NOT_PRIORITARY_TASK);
        OnFailure onFailure = OnFailure.RETRY;
        int timeOut = 0;
        int numNodes = Constants.SINGLE_NODE;
        // Scheduler hints values
        boolean isReplicated = Boolean.parseBoolean(Constants.IS_NOT_REPLICATED_TASK);
        boolean isDistributed = Boolean.parseBoolean(Constants.IS_NOT_DISTRIBUTED_TASK);
        if (declaredMethod.isAnnotationPresent(SchedulerHints.class)) {
            SchedulerHints schedAnnot = declaredMethod.getAnnotation(SchedulerHints.class);
            isReplicated = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(schedAnnot.isReplicated()));
            isDistributed = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(schedAnnot.isDistributed()));
        }

        // Specific implementation values
        boolean isMethod = !declaredMethod.isAnnotationPresent(HTTP.class);

        if (isMethod) {
            executeTask.append(LANG).append(','); // language set to null

            // Method: native, Binary, MPI, COMPSs, Multi-Node, OMPSs, OpenCL
            if (declaredMethod.isAnnotationPresent(es.bsc.compss.types.annotations.task.Method.class)) {
                es.bsc.compss.types.annotations.task.Method methodAnnot =
                    declaredMethod.getAnnotation(es.bsc.compss.types.annotations.task.Method.class);
                isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(methodAnnot.priority()));
                onFailure = methodAnnot.onFailure();
                timeOut = Integer.valueOf(methodAnnot.timeOut());
            } else if (declaredMethod.isAnnotationPresent(Binary.class)) {
                Binary binaryAnnot = declaredMethod.getAnnotation(Binary.class);
                isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(binaryAnnot.priority()));
            } else if (declaredMethod.isAnnotationPresent(MPI.class)) {
                MPI mpiAnnot = declaredMethod.getAnnotation(MPI.class);
                isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(mpiAnnot.priority()));
                // Parse processes from environment if needed
                String numNodesSTR = EnvironmentLoader.loadFromEnvironment(mpiAnnot.processes());
                numNodes = (numNodesSTR != null && !numNodesSTR.isEmpty() && !numNodesSTR.equals(Constants.UNASSIGNED))
                    ? Integer.valueOf(numNodesSTR)
                    : Constants.SINGLE_NODE;
                String ppnSTR = EnvironmentLoader.loadFromEnvironment(mpiAnnot.processesPerNode());
                int ppn = (ppnSTR != null && !ppnSTR.isEmpty() && !ppnSTR.equals(Constants.UNASSIGNED))
                    ? Integer.valueOf(ppnSTR)
                    : 1;
                if (ppn > 1) {
                    if (numNodes < ppn) {
                        LOGGER.error("ERROR: The specified processes in the mpi task is smaller and processesPerNode");
                        throw new CannotCompileException("Specified processes is smaller and processesPerNode");
                    }
                    if ((numNodes % ppn) > 0) {
                        LOGGER.error(
                            "ERROR: The specified processes in the mpi task must be multiple of processesPerNode");
                        throw new CannotCompileException("Specified processes must be multiple of processesPerNode");
                    }
                    numNodes = numNodes / ppn;
                }
            } else if (declaredMethod.isAnnotationPresent(Decaf.class)) {
                Decaf decafAnnot = declaredMethod.getAnnotation(Decaf.class);
                isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(decafAnnot.priority()));
                // Parse computingNodes from environment if needed
                String numNodesSTR = EnvironmentLoader.loadFromEnvironment(decafAnnot.computingNodes());
                numNodes = (numNodesSTR != null && !numNodesSTR.isEmpty() && !numNodesSTR.equals(Constants.UNASSIGNED))
                    ? Integer.valueOf(numNodesSTR)
                    : Constants.SINGLE_NODE;
            } else if (declaredMethod.isAnnotationPresent(COMPSs.class)) {
                COMPSs compssAnnot = declaredMethod.getAnnotation(COMPSs.class);
                isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(compssAnnot.priority()));
                // Parse computingNodes from environment if needed
                String numNodesSTR = EnvironmentLoader.loadFromEnvironment(compssAnnot.computingNodes());
                numNodes = (numNodesSTR != null && !numNodesSTR.isEmpty() && !numNodesSTR.equals(Constants.UNASSIGNED))
                    ? Integer.valueOf(numNodesSTR)
                    : Constants.SINGLE_NODE;
            } else if (declaredMethod.isAnnotationPresent(MultiNode.class)) {
                MultiNode multiNodeAnnot = declaredMethod.getAnnotation(MultiNode.class);
                isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(multiNodeAnnot.priority()));
                // Parse computingNodes from environment if needed
                String numNodesSTR = EnvironmentLoader.loadFromEnvironment(multiNodeAnnot.computingNodes());
                numNodes = (numNodesSTR != null && !numNodesSTR.isEmpty() && !numNodesSTR.equals(Constants.UNASSIGNED))
                    ? Integer.valueOf(numNodesSTR)
                    : Constants.SINGLE_NODE;
            } else if (declaredMethod.isAnnotationPresent(OmpSs.class)) {
                OmpSs ompssAnnot = declaredMethod.getAnnotation(OmpSs.class);
                isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(ompssAnnot.priority()));
            } else if (declaredMethod.isAnnotationPresent(OpenCL.class)) {
                OpenCL openCLAnnot = declaredMethod.getAnnotation(OpenCL.class);
                isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(openCLAnnot.priority()));
            }

            executeTask.append("\"").append(className).append("\"").append(',');
            executeTask.append("\"").append(methodName).append("\"").append(',');

        } else if (declaredMethod.isAnnotationPresent(HTTP.class)) {
            HTTP httpAnnotation = declaredMethod.getAnnotation(HTTP.class);

            String declareMethodFullyQualifiedName = httpAnnotation.declaringClass() + "." + declaredMethod.getName();

            executeTask.append("\"").append(declareMethodFullyQualifiedName).append("\"").append(',');
        }

        // Add scheduler common values
        executeTask.append(isPrioritary).append(',');
        executeTask.append(numNodes).append(",");
        // Default reduce params (Not supported yet in Java)
        executeTask.append(false).append(','); // isReduce
        executeTask.append(0).append(','); // reduceChunkSize
        executeTask.append(isReplicated).append(',');
        executeTask.append(isDistributed).append(',');

        // Add if call has target object or not
        executeTask.append(!isStatic).append(',');

        // Add parameters
        executeTask.append(numParams).append(',');

        // Add the onFailure behavior
        executeTask.append(OnFailure.class.getCanonicalName() + "." + onFailure).append(',');

        // Add the timeOut time
        executeTask.append(timeOut);

        if (numParams == 0) {
            executeTask.append(",null);");
        } else {
            Annotation[][] paramAnnot = declaredMethod.getParameterAnnotations();

            // TODO (below line): add handling of return type
            // declaredMethod.getReturnType();

            CallInformation callInformation = processParameters(declaredMethod, paramAnnot, paramTypes, isVoid,
                isStatic, isMethod, numParams, retType);
            executeTask.append(callInformation.getToAppend());
            executeTask.insert(0, callInformation.getToPrepend());
        }

        return executeTask.toString();
    }

    /**
     * Process the parameters, the target object and the return value of a given method.
     */
    private CallInformation processParameters(Method declaredMethod, Annotation[][] paramAnnot, Class<?>[] paramTypes,
        boolean isVoid, boolean isStatic, boolean isMethod, int numParams, Class<?> retType)
        throws CannotCompileException {

        StringBuilder toAppend = new StringBuilder("");
        StringBuilder toPrepend = new StringBuilder("");

        toAppend.append(",new Object[]{");
        // Add the actual parameters of the method
        for (int i = 0; i < paramAnnot.length; i++) {
            Class<?> formalType = paramTypes[i];
            Parameter par = ((Parameter) paramAnnot[i][0]);

            /*
             * Append the value of the current parameter according to the type. Basic types must be wrapped by an object
             * first
             */
            ParameterInformation infoParam = processParameterValue(i, par, formalType);
            toAppend.append(infoParam.getToAppend());
            toPrepend.insert(0, infoParam.getToPrepend());
            toAppend.append(infoParam.getType()).append(",");
            toAppend.append(infoParam.getDirection()).append(",");
            toAppend.append(infoParam.getStream()).append(",");
            toAppend.append(infoParam.getPrefix() + ",");
            // TODO Aldo: removeto line below
            // toAppend.append("\"\"" + ","); // Parameter Name

            toAppend.append(infoParam.getName() + ","); // Parameter Name TODO Aldo: fix error
            if (infoParam.getType().equals(DATA_TYPES + ".FILE_T")) {
                toAppend.append("\"FILE\"" + ","); // Parameter Content Type
            } else {
                toAppend.append("\"\"" + ","); // Parameter Content Type
            }
            toAppend.append(infoParam.getWeight() + ",");
            toAppend.append("new Boolean(" + infoParam.getKeepRename() + ")");
            if (i < paramAnnot.length - 1) {
                toAppend.append(",");
            }
        }

        // Add the target object of the call as an IN/INOUT parameter, for class methods
        String targetObject = processTargetObject(declaredMethod, isStatic, numParams, isVoid, isMethod);
        toAppend.append(targetObject);

        // Add the return value as an OUT parameter, if any
        ReturnInformation returnInfo = processReturnParameter(isVoid, numParams, retType);
        toAppend.append(returnInfo.getToAppend());
        toPrepend.insert(0, returnInfo.getToPrepend());
        toAppend.append("});");
        toAppend.append(returnInfo.getAfterExecution());

        CallInformation callInformation = new CallInformation(toAppend.toString(), toPrepend.toString());
        return callInformation;
    }

    /**
     * Process the parameter values of a method call.
     */
    private ParameterInformation processParameterValue(int paramIndex, Parameter par, Class<?> formalType) {
        Type annotType = par.type();
        Direction paramDirection = par.direction();
        StdIOStream paramStream = par.stream();
        String paramPrefix = par.prefix();
        String paramName = par.name(); // TODO Aldo: use this
        String paramWeight = par.weight();
        boolean paramKeepRenames = par.keepRename();

        StringBuilder infoToAppend = new StringBuilder("");
        StringBuilder infoToPrepend = new StringBuilder("");
        String type = "";

        switch (annotType) {
            case FILE:
                // The File type needs to be specified explicitly, since its formal type is String
                type = DATA_TYPES + ".FILE_T";
                infoToAppend.append('$').append(paramIndex + 1).append(',');
                infoToPrepend.insert(0, CallGenerator.addTaskFile(this.itSRVar, this.itAppIdVar, paramIndex));
                break;
            case STRING:
                // Mechanism to make a String be treated like a list of chars instead of like another object.
                // Dependencies won't be watched for the string.
                type = DATA_TYPES + ".STRING_T";
                infoToAppend.append('$').append(paramIndex + 1).append(',');
                break;
            case STREAM:
                type = DATA_TYPES + ".STREAM_T";
                infoToAppend.append("$").append(paramIndex + 1).append(",");
                break;
            default:
                // Process the regular parameter value
                type = processRegularParameterValue(paramIndex, formalType, infoToAppend);
                break;
        }

        // Build the parameter information and return
        ParameterInformation infoParam = new ParameterInformation(infoToAppend.toString(), infoToPrepend.toString(),
            type, paramDirection, paramStream, paramPrefix, paramName, paramWeight, paramKeepRenames);
        return infoParam;
    }

    private String processRegularParameterValue(int paramIndex, Class<?> formalType, StringBuilder infoToAppend) {
        String type;
        if (formalType.isPrimitive()) {
            if (formalType.equals(boolean.class)) {
                type = DATA_TYPES + ".BOOLEAN_T";
                infoToAppend.append("new Boolean(").append("$").append(paramIndex + 1).append("),");
            } else if (formalType.equals(char.class)) {
                type = DATA_TYPES + ".CHAR_T";
                infoToAppend.append("new Character(").append("$").append(paramIndex + 1).append("),");
            } else if (formalType.equals(byte.class)) {
                type = DATA_TYPES + ".BYTE_T";
                infoToAppend.append("new Byte(").append("$").append(paramIndex + 1).append("),");
            } else if (formalType.equals(short.class)) {
                type = DATA_TYPES + ".SHORT_T";
                infoToAppend.append("new Short(").append("$").append(paramIndex + 1).append("),");
            } else if (formalType.equals(int.class)) {
                type = DATA_TYPES + ".INT_T";
                infoToAppend.append("new Integer(").append("$").append(paramIndex + 1).append("),");
            } else if (formalType.equals(long.class)) {
                type = DATA_TYPES + ".LONG_T";
                infoToAppend.append("new Long(").append("$").append(paramIndex + 1).append("),");
            } else if (formalType.equals(float.class)) {
                type = DATA_TYPES + ".FLOAT_T";
                infoToAppend.append("new Float(").append("$").append(paramIndex + 1).append("),");
            } else if (formalType.equals(double.class)) {
                type = DATA_TYPES + ".DOUBLE_T";
                infoToAppend.append("new Double(").append("$").append(paramIndex + 1).append("),");
            } else {
                LOGGER.warn(
                    "ERROR: Unrecognised formal type " + formalType.getCanonicalName() + " on parameter " + paramIndex);
                type = "";
            }
        } else { // Object or Self-Contained Object or Persistent SCO
            type = CHECK_SCO_TYPE + "$" + (paramIndex + 1) + ")";
            infoToAppend.append("$").append(paramIndex + 1).append(",");
        }

        return type;
    }

    /**
     * Process the target object of a given method call.
     */
    private String processTargetObject(Method declaredMethod, boolean isStatic, int numParams, boolean isVoid,
        boolean isMethod) {

        StringBuilder targetObj = new StringBuilder("");
        if (!isStatic) {
            // Assuming object, it is unlikely that a user selects a method invoked on an array
            int numRealParams = (isVoid ? numParams : numParams - 1);
            if (numRealParams > 1) {
                targetObj.append(',');
            }
            // Add target object
            targetObj.append("$0,");

            // Add type
            targetObj.append(CHECK_SCO_TYPE + "$0)");

            // Add direction
            // Check if the method will modify the target object (default yes)
            if (isMethod) {
                Direction targetDirection = null;
                if (declaredMethod.isAnnotationPresent(es.bsc.compss.types.annotations.task.Method.class)) {
                    es.bsc.compss.types.annotations.task.Method methodAnnot =
                        declaredMethod.getAnnotation(es.bsc.compss.types.annotations.task.Method.class);
                    targetDirection = methodAnnot.targetDirection();
                } else if (declaredMethod.isAnnotationPresent(MultiNode.class)) {
                    MultiNode multiNodeAnnot = declaredMethod.getAnnotation(MultiNode.class);
                    targetDirection = multiNodeAnnot.targetDirection();
                }
                targetObj.append(',').append(DATA_DIRECTION + "." + targetDirection.name());
            } else {
                // Service
                targetObj.append(',').append(DATA_DIRECTION + ".INOUT");
            }

            // Add binary stream
            targetObj.append(',').append(DATA_STREAM + "." + StdIOStream.UNSPECIFIED);
            // Add empty prefix
            targetObj.append(',').append("\"").append(Constants.PREFIX_EMPTY).append("\"");
            // Add empty parameter name
            targetObj.append(',').append("\"").append("\"");
            // Add empty parameter content type
            targetObj.append(',').append("\"").append("\"");
            // Add default parameter weight
            targetObj.append(',').append("\"").append("1.0").append("\"");
            // Add default parameter keep rename
            targetObj.append(',').append("new Boolean(false)");

        }

        return targetObj.toString();
    }

    /**
     * Process the return parameter of a given method call.
     */
    private ReturnInformation processReturnParameter(boolean isVoid, int numParams, Class<?> retType)
        throws CannotCompileException {

        StringBuilder infoToAppend = new StringBuilder("");
        StringBuilder infoToPrepend = new StringBuilder("");
        StringBuilder afterExecute = new StringBuilder("");

        if (!isVoid) {
            if (numParams > 1) {
                infoToAppend.append(',');
            }

            if (retType.isPrimitive()) {
                /*
                 * ********************************* PRIMITIVE *********************************
                 */
                String tempRetVar = "ret" + System.nanoTime();
                infoToAppend.append(tempRetVar).append(',').append(DATA_TYPES + ".OBJECT_T").append(',')
                    .append(DATA_DIRECTION + ".OUT").append(',').append(DATA_STREAM + "." + StdIOStream.UNSPECIFIED)
                    .append(',').append("\"").append(Constants.PREFIX_EMPTY).append("\"").append(",").append("\"")
                    .append("\"");
                // Add empty parameter content
                // infoToAppend.append(',').append("\"").append("\"");
                infoToAppend.append(',').append("\"").append(retType.toString()).append("\"");

                // Add default parameter weight
                infoToAppend.append(',').append("\"").append("1.0").append("\"");
                // Add default parameter keep rename
                infoToAppend.append(',').append("new Boolean(false)");

                String retValueCreation = "Object " + tempRetVar + " = ";
                String cast;
                String converterMethod;
                if (retType.isAssignableFrom(boolean.class)) {
                    retValueCreation += "new Boolean(false);";
                    cast = "(Boolean)";
                    converterMethod = "booleanValue()";
                } else if (retType.isAssignableFrom(char.class)) {
                    retValueCreation += "new Character(Character.MIN_VALUE);";
                    cast = "(Character)";
                    converterMethod = "charValue()";
                } else if (retType.isAssignableFrom(byte.class)) {
                    retValueCreation += "new Byte(Byte.MIN_VALUE);";
                    cast = "(Byte)";
                    converterMethod = "byteValue()";
                } else if (retType.isAssignableFrom(short.class)) {
                    retValueCreation += "new Short(Short.MIN_VALUE);";
                    cast = "(Short)";
                    converterMethod = "shortValue()";
                } else if (retType.isAssignableFrom(int.class)) {
                    retValueCreation += "new Integer(Integer.MIN_VALUE);";
                    cast = "(Integer)";
                    converterMethod = "intValue()";
                } else if (retType.isAssignableFrom(long.class)) {
                    retValueCreation += "new Long(Long.MIN_VALUE);";
                    cast = "(Long)";
                    converterMethod = "longValue()";
                } else if (retType.isAssignableFrom(float.class)) {
                    retValueCreation += "new Float(Float.MIN_VALUE);";
                    cast = "(Float)";
                    converterMethod = "floatValue()";
                } else { // (retType.isAssignableFrom(double.class))
                    retValueCreation += "new Double(Double.MIN_VALUE);";
                    cast = "(Double)";
                    converterMethod = "doubleValue()";
                }

                // Before paramsModified, declare and instance a temp wrapper object containing the primitive value
                infoToPrepend.insert(0, retValueCreation);

                /*
                 * After execute task, register an access to the wrapper object, get its (remotely) generated value and
                 * assign it to the application's primitive type var
                 */
                afterExecute.append(CallGenerator.oRegNewObjectAccess(itORVar, itAppIdVar, tempRetVar)).append(";");
                afterExecute.append("$_ = (").append(cast)
                    .append(CallGenerator.oRegGetInternalObject(itORVar, itAppIdVar, tempRetVar)).append(").")
                    .append(converterMethod).append(";");
            } else if (retType.isArray()) {
                // ARRAY
                String typeName = retType.getName();
                Class<?> compType = retType.getComponentType();
                int numDim = typeName.lastIndexOf('[');
                String dims = "[0]";
                while (numDim-- > 0) {
                    dims += "[]";
                }
                while (compType.getComponentType() != null) {
                    compType = compType.getComponentType();
                }
                String compTypeName = compType.getName();
                infoToPrepend.insert(0, "$_ = new " + compTypeName + dims + ';');
                infoToAppend.append("$_,").append(DATA_TYPES + ".OBJECT_T").append(',').append(DATA_DIRECTION + ".OUT")
                    .append(',').append(DATA_STREAM + ".UNSPECIFIED").append(',').append("\"")
                    .append(Constants.PREFIX_EMPTY).append("\"").append(',').append("\"").append("\"");
                // Add empty parameter content
                infoToAppend.append(',').append("\"").append("\"");
                // Add default parameter weight
                infoToAppend.append(',').append("\"").append("1.0").append("\"");
                // Add default parameter keep rename
                infoToAppend.append(',').append("new Boolean(false)");
            } else {
                // OBJECT
                // Wrapper for a primitive type: return a default value
                if (retType.isAssignableFrom(Boolean.class)) {
                    infoToPrepend.insert(0, "$_ = new Boolean(false);");
                } else if (retType.isAssignableFrom(Character.class)) {
                    infoToPrepend.insert(0, "$_ = new Character(Character.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Byte.class)) {
                    infoToPrepend.insert(0, "$_ = new Byte(Byte.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Short.class)) {
                    infoToPrepend.insert(0, "$_ = new Short(Short.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Integer.class)) {
                    infoToPrepend.insert(0, "$_ = new Integer(Integer.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Long.class)) {
                    infoToPrepend.insert(0, "$_ = new Long(Long.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Float.class)) {
                    infoToPrepend.insert(0, "$_ = new Float(Float.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Double.class)) {
                    infoToPrepend.insert(0, "$_ = new Double(Double.MIN_VALUE);");
                } else {
                    // Object (maybe String): use the no-args constructor
                    // Check that object class has empty constructor
                    String typeName = retType.getName();
                    try {
                        Class.forName(typeName).getConstructor();
                    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
                        throw new CannotCompileException(ERROR_NO_EMPTY_CONSTRUCTOR + typeName);
                    }

                    infoToPrepend.insert(0, "$_ = new " + typeName + "();");
                }

                infoToAppend.append("$_,").append(CHECK_SCO_TYPE + "$_)");
                // Add direction
                infoToAppend.append(',').append(DATA_DIRECTION + ".OUT");
                // Add stream binary
                infoToAppend.append(',').append(DATA_STREAM + ".UNSPECIFIED");
                // Add empty prefix
                infoToAppend.append(',').append("\"").append(Constants.PREFIX_EMPTY).append("\"");
                // Add empty parameter name
                infoToAppend.append(',').append("\"").append("\"");
                // Add empty parameter content
                infoToAppend.append(',').append("\"").append("\"");
                // Add default parameter weight
                infoToAppend.append(',').append("\"").append("1.0").append("\"");
                // Add default parameter keep rename
                infoToAppend.append(',').append("new Boolean(false)");
            }
        }

        ReturnInformation returnInfo =
            new ReturnInformation(infoToAppend.toString(), infoToPrepend.toString(), afterExecute.toString());
        return returnInfo;
    }

    /**
     * Replaces the close stream call.
     *
     * @return
     */
    private String replaceCloseStream() {
        String streamClose = PROCEED + "$$); " + CallGenerator.closeStream(this.itSRVar, this.itAppIdVar);
        return streamClose;
    }

    /**
     * Replaces the delete file call.
     *
     * @return
     */
    private String replaceDeleteFile() {
        String deleteFile = "$_ = " + CallGenerator.deleteFile(itApiVar, itAppIdVar);
        return deleteFile;
    }

    /**
     * Replaces the API calls.
     */
    private String replaceAPICall(String methodName, CtMethod method) throws CannotCompileException {
        boolean isVoid = false;
        boolean hasArgs = false;

        try {
            Class<?> retType = method.getReturnType().getClass();
            isVoid = retType.equals(void.class);
            hasArgs = (method.getParameterTypes().length != 0);
        } catch (NotFoundException e) {
            throw new CannotCompileException(e);
        }

        // Add the COMPSsRuntime API Call with the given appId ALWAYS as FIRST parameter
        // Something like: itApiVar.methodName(itAppIdVar, $$);
        StringBuilder apiCall = new StringBuilder("");
        if (isVoid) {
            apiCall.append("$_ = ").append(this.itApiVar);
        } else {
            apiCall.append(this.itApiVar);
        }

        apiCall.append(".").append(methodName).append("(").append(this.itAppIdVar);

        if (hasArgs) {
            apiCall.append(", $$");
        } else {
            // Nothing to add
        }

        apiCall.append(");");

        return apiCall.toString();
    }

    /**
     * Replaces the blackBox calls.
     *
     * @return
     */
    private String replaceBlackBox(String methodName, String className, CtMethod method) throws CannotCompileException {
        if (DEBUG) {
            LOGGER.debug("Inspecting method call to black-box method " + methodName + ", looking for objects");
        }

        StringBuilder modifiedCall = new StringBuilder();
        StringBuilder toSerialize = new StringBuilder();

        // Check if the black-box we're going to is one of the array watch methods
        boolean isArrayWatch = method.getDeclaringClass().getName().equals(LoaderConstants.CLASS_ARRAY_ACCESS_WATCHER);

        // First check the target object
        modifiedCall.append(CallGenerator.oRegNewObjectAccess(itORVar, itAppIdVar, "$0")).append(";");
        toSerialize.append(CallGenerator.oRegSerializeLocally(itORVar, itAppIdVar, "$0")).append(";");

        /*
         * Now add the call. If the target object of the call is a task object, invoke the method on the internal object
         * stored by the runtime. Also check the parameters. We need to control the parameters of non-remote and
         * non-instrumented methods (black box), since they represent the border to the code where we can't intercept
         * anything. If any of these parameters is an object we kept track of, synchronize
         */
        String redirectedCallPars = null;
        try {
            CtClass[] paramTypes = method.getParameterTypes();
            if (paramTypes.length > 0) {
                int i = 1;
                StringBuilder aux1 = new StringBuilder("new Object[] {");
                for (CtClass parType : paramTypes) {
                    if (i > 1) {
                        aux1.append(',');
                        /* aux2.append(','); */
                    }
                    String parId = "$" + i;
                    if (parType.isPrimitive()) {
                        if (parType.equals(CtClass.booleanType)) {
                            aux1.append("new Boolean(").append(parId).append(')');
                        } else if (parType.equals(CtClass.charType)) {
                            aux1.append("new Character(").append(parId).append(')');
                        } else if (parType.equals(CtClass.byteType)) {
                            aux1.append("new Byte(").append(parId).append(')');
                        } else if (parType.equals(CtClass.shortType)) {
                            aux1.append("new Short(").append(parId).append(')');
                        } else if (parType.equals(CtClass.intType)) {
                            aux1.append("new Integer(").append(parId).append(')');
                        } else if (parType.equals(CtClass.longType)) {
                            aux1.append("new Long(").append(parId).append(')');
                        } else if (parType.equals(CtClass.floatType)) {
                            aux1.append("new Float(").append(parId).append(')');
                        } else { // if (parType.equals(CtClass.doubleType))
                            aux1.append("new Double(").append(parId).append(')');
                        }
                    } else if (parType.getName().equals(COMPSsFile.class.getName())) {
                        if (DEBUG) {
                            LOGGER.debug("Parameter " + i + " of black-box method " + methodName
                                + " is an COMPSs File, adding File synch");
                        }
                        aux1.append(CallGenerator.synchFile(parId));

                    } else if (parType.getName().equals(String.class.getName())) { // This is a string
                        if (DEBUG) {
                            LOGGER.debug("Parameter " + i + " of black-box method " + methodName
                                + " is an String, adding File/object access");
                        }
                        if (isArrayWatch && i == 3) {
                            // Prevent from synchronizing task return objects to be stored in an array position
                            aux1.append(parId);
                        } else {
                            String calledClass = className;
                            if (calledClass.equals(PrintStream.class.getName())
                                || calledClass.equals(StringBuilder.class.getName())) {
                                // If the call is inside a PrintStream or StringBuilder, only synchronize objects files
                                // already has the name
                                String internalObject = CallGenerator.oRegGetInternalObject(itORVar, itAppIdVar, parId);
                                modifiedCall.insert(0,
                                    CallGenerator.oRegNewObjectAccess(itORVar, itAppIdVar, parId) + ";");
                                aux1.append(internalObject).append(" == null ? ").append(parId).append(" : ")
                                    .append("(" + parType.getName() + ")").append(internalObject);
                                toSerialize.append(CallGenerator.oRegSerializeLocally(itORVar, itAppIdVar, parId))
                                    .append(";");
                            } else {
                                String internalObject = CallGenerator.oRegGetInternalObject(itORVar, itAppIdVar, parId);
                                String taskFile = CallGenerator.isTaskFile(this.itSRVar, this.itAppIdVar, parId);
                                String apiOpenFile = CallGenerator.openFile(this.itApiVar, this.itAppIdVar, parId,
                                    DATA_DIRECTION + ".INOUT");
                                modifiedCall.insert(0,
                                    CallGenerator.oRegNewObjectAccess(itORVar, itAppIdVar, parId) + ";");
                                // Adding check of task files
                                aux1.append(taskFile).append(" ? ").append(apiOpenFile).append(" : ")
                                    .append(internalObject).append(" == null ? ").append(parId).append(" : ")
                                    .append("(" + parType.getName() + ")").append(internalObject);
                                toSerialize.append(CallGenerator.oRegSerializeLocally(itORVar, itAppIdVar, parId))
                                    .append(";");
                            }
                        }
                    } else { // Object (also array)
                        if (DEBUG) {
                            LOGGER.debug("Parameter " + i + " of black-box method " + methodName
                                + " is an object, adding access");
                        }

                        if (isArrayWatch && i == 3) {
                            // Prevent from synchronizing task return objects to be stored in an array position
                            aux1.append(parId);
                        } else {
                            String internalObject = CallGenerator.oRegGetInternalObject(itORVar, itAppIdVar, parId);
                            modifiedCall.insert(0, CallGenerator.oRegNewObjectAccess(itORVar, itAppIdVar, parId) + ";");
                            aux1.append(internalObject).append(" == null ? ").append(parId).append(" : ")
                                .append("(" + parType.getName() + ")").append(internalObject);
                            toSerialize.append(CallGenerator.oRegSerializeLocally(itORVar, itAppIdVar, parId))
                                .append(";");
                        }
                    }
                    i++;
                }
                aux1.append("}");
                redirectedCallPars = aux1.toString();
            }
        } catch (NotFoundException e) {
            throw new CannotCompileException(e);
        }
        String internalObject = CallGenerator.oRegGetInternalObject(itORVar, itAppIdVar, "$0");
        modifiedCall.append("if (").append(internalObject).append(" != null) {")
            .append("$_ = ($r)" + RUN_METHOD_ON_OBJECT).append(internalObject).append(",$class,\"").append(methodName)
            .append("\",").append(redirectedCallPars).append(",$sig);")
            .append("}else { $_ = ($r)" + RUN_METHOD_ON_OBJECT + "$0,$class,\"").append(methodName).append("\",")
            .append(redirectedCallPars).append(",$sig); }");

        // Serialize the (internal) objects locally after the call
        modifiedCall.append(toSerialize);

        // Return all the modified call
        return modifiedCall.toString();
    }


    private class ParameterInformation {

        private final String toAppend;
        private final String toPrepend;
        private final String type;
        private final Direction direction;
        private final StdIOStream stream;
        private final String prefix;
        private final String name;
        private final String weight;
        private final boolean keepRename;


        public ParameterInformation(String toAppend, String toPrepend, String type, Direction direction,
            StdIOStream stream, String prefix, String name, String weight, boolean keepRename) {
            this.toAppend = toAppend;
            this.toPrepend = toPrepend;
            this.type = type;
            this.direction = direction;
            this.stream = stream;
            this.prefix = prefix;
            this.name = name;
            this.weight = weight;
            this.keepRename = keepRename;
        }

        public String getToAppend() {
            return this.toAppend;
        }

        public String getToPrepend() {
            return this.toPrepend;
        }

        public String getType() {
            return this.type;
        }

        public String getDirection() {
            return DATA_DIRECTION + "." + this.direction.name();
        }

        public String getStream() {
            return DATA_STREAM + "." + this.stream.name();
        }

        public String getPrefix() {
            return "\"" + this.prefix + "\"";
        }

        public String getName() {
            return "\"" + this.name + "\"";
        }

        public String getWeight() {
            return "\"" + this.weight + "\"";
        }

        public boolean getKeepRename() {
            return this.keepRename;
        }
    }

    private class ReturnInformation {

        private final String toAppend;
        private final String toPrepend;
        private final String afterExecution;


        public ReturnInformation(String toAppend, String toPrepend, String afterExecution) {
            this.toAppend = toAppend;
            this.toPrepend = toPrepend;
            this.afterExecution = afterExecution;
        }

        public String getToAppend() {
            return this.toAppend;
        }

        public String getToPrepend() {
            return this.toPrepend;
        }

        public String getAfterExecution() {
            return this.afterExecution;
        }

    }

    private class CallInformation {

        private final String toAppend;
        private final String toPrepend;


        public CallInformation(String toAppend, String toPrepend) {
            this.toAppend = toAppend;
            this.toPrepend = toPrepend;
        }

        public String getToAppend() {
            return this.toAppend;
        }

        public String getToPrepend() {
            return this.toPrepend;
        }

    }

}
