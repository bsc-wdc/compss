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

import es.bsc.compss.api.COMPSsGroup;
import es.bsc.compss.loader.LoaderConstants;
import es.bsc.compss.loader.LoaderUtils;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.Direction;
import java.io.File;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.PrintStream;
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

    // Inserted method calls
    private static final String PROCEED = "$_ = $proceed(";
    private static final String COMPSS_LOADER_GROUP = LoaderConstants.CLASS_COMPSS_GROUP_LOADER + "(";

    private static final String COMPSS_API = APIHandler.class.getCanonicalName();
    private static final String DATA_DIRECTION = Direction.class.getCanonicalName();

    private static final String RUN_METHOD_ON_OBJECT = "LoaderUtils.runMethodOnObject(";

    // Pointers to internal variables
    private Method[] remoteMethods;
    private CtMethod[] instrCandidates; // methods that will be instrumented if they are not remote
    private String itWfVar;


    /**
     * Modifies the current application to instrument the task methods with remote invocations.
     *
     * @param remoteMethods List of ITF remote methods.
     * @param instrCandidates List of detected methods in the main code.
     * @param itWfVar Workflow variable.
     */
    public ITAppEditor(Method[] remoteMethods, CtMethod[] instrCandidates, String itWfVar) {

        super();
        this.remoteMethods = remoteMethods;
        this.instrCandidates = instrCandidates;
        this.itWfVar = itWfVar;
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
                            String typeCast = "(" + parType.getName() + ")";
                            callPars.append(typeCast + CallGenerator.wfAccessObject(itWfVar, parId));
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
        toInclude.append("(").append("(").append(fa.getClassName()).append(")") // cast
            .append(CallGenerator.wfAccessObject(itWfVar, "$0", isWriter)) // object
            .append(").").append(fieldName);
        if (isWriter) {
            // store a new value in the field
            toInclude.append(" = $1;");
        } else {
            // read the field value
            toInclude.insert(0, "$_ = ");
            toInclude.append(";");
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
                modifiedExpr = "$_ = " + CallGenerator.newStreamClass(itWfVar, streamClass, callPars) + ";";
                found = true;
                break;
            }
        }
        if (!found) { // Not a stream
            if (className.equals(File.class.getCanonicalName())) {
                modifiedExpr = "$_ = " + CallGenerator.newCOMPSsFile(itWfVar, callPars) + ";";
            } else {
                String internalObject = CallGenerator.getRegisteredObjectValue(itWfVar, "$1");
                String par1 = internalObject + " == null ? $1 : " + internalObject;
                modifiedExpr = PROCEED + callPars + "); ";
                modifiedExpr += "if ($_ instanceof " + FilterInputStream.class.getCanonicalName() + " || $_ instanceof "
                    + FilterOutputStream.class.getCanonicalName() + ") {";
                modifiedExpr += CallGenerator.newFilterStream(this.itWfVar, par1);
            }
        }
        if (DEBUG) {
            LOGGER.debug("Modifying creation of an object of class " + className + " with \"" + modifiedExpr + "\"");
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
    private String replaceTaskMethodCall(String methodName, String className, Method itfMethod, CtMethod calledMethod)
        throws CannotCompileException {

        if (DEBUG) {
            LOGGER.debug("Found call to remote method " + methodName);
        }
        TaskCall callInformation = new TaskCall(this.itWfVar, className, methodName, calledMethod, itfMethod);
        return callInformation.toCommands();
    }

    /**
     * Replaces the close stream call.
     *
     * @return
     */
    private String replaceCloseStream() {
        String streamClose = PROCEED + "$$); " + CallGenerator.closeStream(this.itWfVar) + ";";
        return streamClose;
    }

    /**
     * Replaces the delete file call.
     *
     * @return
     */
    private String replaceDeleteFile() {
        return "$_ = " + CallGenerator.deleteFile(itWfVar, "$0") + ";";
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

        apiCall.append(COMPSS_API).append(".").append(methodName).append("(").append(this.itWfVar);

        if (hasArgs) {
            apiCall.append(", $$");
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

        // Check if the black-box we're going to is one of the array watch methods
        boolean isArrayWatch = method.getDeclaringClass().getName().equals(LoaderConstants.CLASS_ARRAY_ACCESS_WATCHER);

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
                            if (!className.equals(PrintStream.class.getName())
                                && !className.equals(StringBuilder.class.getName())) {
                                String taskFile = CallGenerator.isTaskFile(parId);
                                String apiOpenFile =
                                    CallGenerator.openFile(this.itWfVar, parId, DATA_DIRECTION + ".INOUT");
                                aux1.append(taskFile).append(" ? ").append(apiOpenFile).append(" : ");
                            }
                            // If the call is inside a PrintStream or StringBuilder, only synchronize objects files
                            // already has the name
                            aux1.append("(").append(parType.getName()).append(")") // cast
                                .append(CallGenerator.wfAccessObject(itWfVar, parId)); // object
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
                            aux1.append("(").append(parType.getName()).append(")") // cast
                                .append(CallGenerator.wfAccessObject(itWfVar, parId)); // object
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

        modifiedCall.append("$_ = ($r)") // result and casting
            .append(RUN_METHOD_ON_OBJECT) // invoke method
            .append(CallGenerator.wfAccessObject(itWfVar, "$0")) // target object
            .append(",$class,\"").append(methodName).append("\",").append(redirectedCallPars).append(",$sig);");

        // Return all the modified call
        return modifiedCall.toString();
    }

}
