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
    private static final String COMPSS_LOADER_GROUP = LoaderConstants.CLASS_COMPSS_GROUP_LOADER + "(";

    private static final String COMPSS_API = APIHandler.class.getCanonicalName();
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
    private String itWfVar;
    private String itAppIdVar;
    private CtClass appClass;


    /**
     * Modifies the current application to instrument the task methods with remote invocations.
     *
     * @param remoteMethods List of ITF remote methods.
     * @param instrCandidates List of detected methods in the main code.
     * @param itApiVar COMPSs API pointer.
     * @param itSRVar Stream Registry.
     * @param itORVar Object Registry pointer.
     * @param itWfVar Workflow variable.
     * @param itAppIdVar COMPSs Application Id variable.
     * @param appClass Application main class.
     */
    public ITAppEditor(Method[] remoteMethods, CtMethod[] instrCandidates, String itApiVar, String itSRVar,
        String itORVar, String itWfVar, String itAppIdVar, CtClass appClass) {

        super();
        this.remoteMethods = remoteMethods;
        this.instrCandidates = instrCandidates;
        this.itApiVar = itApiVar;
        this.itSRVar = itSRVar;
        this.itORVar = itORVar;
        this.itWfVar = itWfVar;
        this.itAppIdVar = itAppIdVar;
        this.appClass = appClass;
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

        String taskClassName = mc.getClassName();
        Method declaredMethod = null;
        CtMethod calledMethod = null;
        try {
            calledMethod = mc.getMethod();
            declaredMethod = LoaderUtils.checkRemote(calledMethod, remoteMethods);
        } catch (NotFoundException e) {
            throw new CannotCompileException(e);
        }

        if (declaredMethod != null) {
            // Current method must be executed remotely, change the call
            if (DEBUG) {
                LOGGER.debug("Replacing task method call " + mc.getMethodName());
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
                modifiedExpr = "$_ = " + CallGenerator.newStreamClass(itSRVar, itAppIdVar, streamClass, callPars) + ";";
                found = true;
                break;
            }
        }
        if (!found) { // Not a stream
            if (className.equals(File.class.getCanonicalName())) {
                modifiedExpr = "$_ = " + CallGenerator.newCOMPSsFile(itSRVar, itAppIdVar, callPars) + ";";
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
        modifiedExpr = "$_ = new  " + COMPSS_LOADER_GROUP + this.itWfVar + ", $$);";
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
        executeTask.append(timeOut).append(",");

        if (numParams == 0) {
            executeTask.append("null);");
        } else {
            Annotation[][] paramAnnot = declaredMethod.getParameterAnnotations();
            TaskCall callInformation =
                new TaskCall(declaredMethod, paramAnnot, paramTypes, isVoid, isStatic, isMethod, retType);
            executeTask.insert(0, callInformation.getPreCall());
            executeTask.append(callInformation.getCallParameters()).append(");");
            executeTask.append(callInformation.getPostCall());
        }

        return executeTask.toString();
    }

    /**
     * Replaces the close stream call.
     *
     * @return
     */
    private String replaceCloseStream() {
        String streamClose = PROCEED + "$$); " + CallGenerator.closeStream(this.itSRVar, this.itAppIdVar) + ";";
        return streamClose;
    }

    /**
     * Replaces the delete file call.
     *
     * @return
     */
    private String replaceDeleteFile() {
        String deleteFile = "$_ = " + CallGenerator.deleteFile(itApiVar, itAppIdVar) + ";";
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
        StringBuilder apiCall = new StringBuilder();
        if (isVoid) {
            apiCall.append("$_ = ");
        }

        apiCall.append(COMPSS_API).append(".").append(methodName).append("(").append(this.itApiVar).append(",")
            .append(this.itWfVar).append(",").append(this.itORVar);

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


    private class TaskCall {

        ArgumentInformation[] params;
        TargetInformation targetObject;
        ReturnInformation returnInfo;


        public TaskCall(Method declaredMethod, Annotation[][] paramAnnot, Class<?>[] paramTypes, boolean isVoid,
            boolean isStatic, boolean isMethod, Class<?> retType) throws CannotCompileException {

            this.params = new ArgumentInformation[paramAnnot.length];
            for (int i = 0; i < paramAnnot.length; i++) {
                Class<?> formalType = paramTypes[i];
                Parameter par = ((Parameter) paramAnnot[i][0]);
                this.params[i] = new ArgumentInformation(i, par, formalType);
            }
            if (isStatic) {
                this.targetObject = null;
            } else {
                this.targetObject = new TargetInformation(declaredMethod, isMethod);
            }
            if (isVoid) {
                this.returnInfo = null;
            } else {
                this.returnInfo = new ReturnInformation(retType);
            }
        }

        public String getPreCall() {
            StringBuilder preCall = new StringBuilder();
            for (ArgumentInformation pi : params) {
                preCall.append(pi.getParamPreparation());
            }
            if (returnInfo != null) {
                preCall.append(returnInfo.getDummyCreation());
            }
            return preCall.toString();
        }

        public String getCallParameters() {
            boolean addedParameter = false;
            StringBuilder onCall = new StringBuilder("new Object[]{");
            for (ArgumentInformation p : this.params) {
                if (addedParameter) {
                    onCall.append(",");
                } else {
                    addedParameter = true;
                }
                onCall.append(p.getParamDesc());
            }
            if (this.targetObject != null) {
                if (addedParameter) {
                    onCall.append(",");
                } else {
                    addedParameter = true;
                }
                onCall.append(this.targetObject.getParamDesc());
            }
            if (returnInfo != null) {
                // Assuming object, it is unlikely that a user selects a method invoked on an array
                if (addedParameter) {
                    onCall.append(",");
                }
                onCall.append(returnInfo.getParamDesc());
            }
            onCall.append("}");
            return onCall.toString();
        }

        public String getPostCall() {
            StringBuilder postCall = new StringBuilder();
            for (ArgumentInformation pi : params) {
                postCall.append(pi.getParamCleanup());
            }
            if (returnInfo != null) {
                postCall.append(returnInfo.getResultCollection());
            }
            return postCall.toString();
        }

        private String buildParameter(String parValue, String parType, String direction, String stream, String prefix,
            String name, String contentType, String weight, boolean keepRename) {
            return parValue + ',' + // value
                parType + ',' + // type
                DATA_DIRECTION + "." + direction + ',' + // direction
                DATA_STREAM + "." + stream + ',' + // stream
                "\"" + prefix + "\"" + ',' + // prefix
                "\"" + name + "\"," + // param name
                "\"" + contentType + "\"" + ',' + // content
                "\"" + weight + "\"" + ',' + // weight
                "new Boolean(" + keepRename + ")"; // keep rename
        }

        private String buildParameter(String parValue, String parType, String direction, String contentType) {
            return buildParameter(parValue, parType, direction, "UNSPECIFIED", Constants.PREFIX_EMPTY, "", contentType,
                "1.0", false);
        }

        private String buildOutParameter(String parValue, String parType, String contentType) {
            return buildParameter(parValue, parType, "OUT", contentType);
        }


        /**
         * Represents an argument of a given task call.
         */
        private class ArgumentInformation {

            private final String paramPreparation;
            private final String paramDesc;
            private final String paramCleanup;


            public ArgumentInformation(int paramIndex, Parameter par, Class<?> formalType) {
                String paramPreparation = "";
                String paramCleanup = "";
                String parVal = "$" + (paramIndex + 1);
                String parType;
                String parContent = "";
                switch (par.type()) {
                    case FILE:
                        // The File type needs to be specified explicitly, since its formal type is String
                        parType = DATA_TYPES + ".FILE_T";
                        parContent = "FILE";
                        paramPreparation = CallGenerator.addTaskFile(itSRVar, itAppIdVar, parVal) + ";";
                        if (par.direction() == Direction.IN_DELETE) {
                            paramCleanup = CallGenerator.removeTaskFile(itSRVar, itAppIdVar, parVal) + ";";
                        }
                        break;
                    case STRING:
                        // Mechanism to make a String be treated like a list of chars instead of like another object.
                        // Dependencies won't be watched for the string.
                        parType = DATA_TYPES + ".STRING_T";
                        break;
                    case STREAM:
                        parType = DATA_TYPES + ".STREAM_T";
                        break;
                    default:
                        // Process the regular parameter value
                        if (formalType.isPrimitive()) {
                            if (formalType.equals(boolean.class)) {
                                parType = DATA_TYPES + ".BOOLEAN_T";
                                parVal = "new Boolean($" + (paramIndex + 1) + (")");
                            } else if (formalType.equals(char.class)) {
                                parType = DATA_TYPES + ".CHAR_T";
                                parVal = "new Character($" + (paramIndex + 1) + (")");
                            } else if (formalType.equals(byte.class)) {
                                parType = DATA_TYPES + ".BYTE_T";
                                parVal = "new Byte($" + (paramIndex + 1) + (")");
                            } else if (formalType.equals(short.class)) {
                                parType = DATA_TYPES + ".SHORT_T";
                                parVal = "new Short($" + (paramIndex + 1) + (")");
                            } else if (formalType.equals(int.class)) {
                                parType = DATA_TYPES + ".INT_T";
                                parVal = "new Integer($" + (paramIndex + 1) + (")");
                            } else if (formalType.equals(long.class)) {
                                parType = DATA_TYPES + ".LONG_T";
                                parVal = "new Long($" + (paramIndex + 1) + (")");
                            } else if (formalType.equals(float.class)) {
                                parType = DATA_TYPES + ".FLOAT_T";
                                parVal = "new Float($" + (paramIndex + 1) + (")");
                            } else if (formalType.equals(double.class)) {
                                parType = DATA_TYPES + ".DOUBLE_T";
                                parVal = "new Double($" + (paramIndex + 1) + (")");
                            } else {
                                LOGGER.warn("ERROR: Unrecognised formal type " + formalType.getCanonicalName()
                                    + " on parameter " + paramIndex);
                                parType = "";
                            }
                        } else { // Object or Self-Contained Object or Persistent SCO
                            parType = CHECK_SCO_TYPE + "$" + (paramIndex + 1) + ")";
                            if (par.direction() == Direction.IN_DELETE) {
                                paramCleanup =
                                    CallGenerator.oRegRemove(itORVar, itAppIdVar, "$" + (paramIndex + 1)) + ";";
                            }
                        }
                        break;
                }
                this.paramPreparation = paramPreparation;
                this.paramDesc = buildParameter(parVal, parType, par.direction().name(), par.stream().name(),
                    par.prefix(), par.name(), parContent, par.weight(), par.keepRename());
                this.paramCleanup = paramCleanup;
            }

            public String getParamPreparation() {
                return this.paramPreparation;
            }

            public String getParamDesc() {
                return this.paramDesc;
            }

            public String getParamCleanup() {
                return paramCleanup;
            }
        }

        /**
         * Represents the target object of a given task call.
         */
        private class TargetInformation {

            private final String paramDescription;


            public TargetInformation(Method declaredMethod, boolean isMethod) {
                String tgtVal;
                String tgtType;
                tgtVal = "$0";
                tgtType = CHECK_SCO_TYPE + "$0)";

                // Add direction
                String parDirection;
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
                    parDirection = targetDirection.name();
                } else {
                    // Service
                    parDirection = "INOUT";
                }

                this.paramDescription = buildParameter(tgtVal, tgtType, parDirection, "");
            }

            public String getParamDesc() {
                return this.paramDescription;
            }
        }

        /**
         * Represents the return object of a given task call.
         */
        private class ReturnInformation {

            private final String dummyCreation;
            private final String paramDesc;
            private final String resultCollection;


            public ReturnInformation(Class<?> retType) throws CannotCompileException {
                StringBuilder dummyCreation = new StringBuilder();
                String param = "";
                StringBuilder resCollection = new StringBuilder();

                String parValue;
                String parType;
                String contentType = "";
                if (retType.isPrimitive()) {
                    /*
                     * ********************************* PRIMITIVE *********************************
                     */
                    String tempRetVar = "ret" + System.nanoTime();
                    dummyCreation.append("Object ").append(tempRetVar).append(" = ");
                    String cast;
                    String converterMethod;
                    if (retType.isAssignableFrom(boolean.class)) {
                        dummyCreation.append("new Boolean(false);");
                        cast = "(Boolean)";
                        converterMethod = "booleanValue()";
                    } else if (retType.isAssignableFrom(char.class)) {
                        dummyCreation.append("new Character(Character.MIN_VALUE);");
                        cast = "(Character)";
                        converterMethod = "charValue()";
                    } else if (retType.isAssignableFrom(byte.class)) {
                        dummyCreation.append("new Byte(Byte.MIN_VALUE);");
                        cast = "(Byte)";
                        converterMethod = "byteValue()";
                    } else if (retType.isAssignableFrom(short.class)) {
                        dummyCreation.append("new Short(Short.MIN_VALUE);");
                        cast = "(Short)";
                        converterMethod = "shortValue()";
                    } else if (retType.isAssignableFrom(int.class)) {
                        dummyCreation.append("new Integer(Integer.MIN_VALUE);");
                        cast = "(Integer)";
                        converterMethod = "intValue()";
                    } else if (retType.isAssignableFrom(long.class)) {
                        dummyCreation.append("new Long(Long.MIN_VALUE);");
                        cast = "(Long)";
                        converterMethod = "longValue()";
                    } else if (retType.isAssignableFrom(float.class)) {
                        dummyCreation.append("new Float(Float.MIN_VALUE);");
                        cast = "(Float)";
                        converterMethod = "floatValue()";
                    } else { // (retType.isAssignableFrom(double.class))
                        dummyCreation.append("new Double(Double.MIN_VALUE);");
                        cast = "(Double)";
                        converterMethod = "doubleValue()";
                    }

                    parValue = tempRetVar;
                    parType = DATA_TYPES + ".OBJECT_T";
                    contentType = retType.toString();

                    /*
                     * After execute task, register an access to the wrapper object, get its (remotely) generated value
                     * and assign it to the application's primitive type var
                     */
                    resCollection.append(CallGenerator.oRegNewObjectAccess(itORVar, itAppIdVar, tempRetVar))
                        .append(";");
                    resCollection.append("$_ = (").append(cast)
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

                    parValue = "$_";
                    parType = DATA_TYPES + ".OBJECT_T";
                    String compTypeName = compType.getName();
                    dummyCreation.append("$_ = new ").append(compTypeName).append(dims).append(';');

                } else {
                    // OBJECT
                    // Wrapper for a primitive type: return a default value
                    if (retType.isAssignableFrom(Boolean.class)) {
                        dummyCreation.append("$_ = new Boolean(false);");
                    } else if (retType.isAssignableFrom(Character.class)) {
                        dummyCreation.append("$_ = new Character(Character.MIN_VALUE);");
                    } else if (retType.isAssignableFrom(Byte.class)) {
                        dummyCreation.append("$_ = new Byte(Byte.MIN_VALUE);");
                    } else if (retType.isAssignableFrom(Short.class)) {
                        dummyCreation.append("$_ = new Short(Short.MIN_VALUE);");
                    } else if (retType.isAssignableFrom(Integer.class)) {
                        dummyCreation.append("$_ = new Integer(Integer.MIN_VALUE);");
                    } else if (retType.isAssignableFrom(Long.class)) {
                        dummyCreation.append("$_ = new Long(Long.MIN_VALUE);");
                    } else if (retType.isAssignableFrom(Float.class)) {
                        dummyCreation.append("$_ = new Float(Float.MIN_VALUE);");
                    } else if (retType.isAssignableFrom(Double.class)) {
                        dummyCreation.append("$_ = new Double(Double.MIN_VALUE);");
                    } else {
                        // Object (maybe String): use the no-args constructor
                        // Check that object class has empty constructor
                        String typeName = retType.getName();
                        try {
                            Class.forName(typeName).getConstructor();
                        } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
                            throw new CannotCompileException(ERROR_NO_EMPTY_CONSTRUCTOR + typeName);
                        }

                        dummyCreation.append("$_ = new ").append(typeName).append("();");
                    }

                    parValue = "$_";
                    parType = CHECK_SCO_TYPE + "$_)";
                }
                param = buildOutParameter(parValue, parType, contentType);

                this.dummyCreation = dummyCreation.toString();
                this.paramDesc = param;
                this.resultCollection = resCollection.toString();
            }

            public String getParamDesc() {
                return this.paramDesc;
            }

            public String getDummyCreation() {
                return this.dummyCreation;
            }

            public String getResultCollection() {
                return this.resultCollection;
            }

        }
    }
}
