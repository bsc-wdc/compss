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
import es.bsc.compss.types.annotations.task.MPI;
import es.bsc.compss.types.annotations.task.MultiNode;
import es.bsc.compss.types.annotations.task.OmpSs;
import es.bsc.compss.types.annotations.task.OpenCL;
import es.bsc.compss.util.EnvironmentLoader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.Collectors;
import javassist.CannotCompileException;
import javassist.CtMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TaskCall {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);

    // ERRORS
    private static final String ERROR_NO_EMPTY_CONSTRUCTOR = "ERROR: No empty constructor on object class ";
    private static final String ERROR_PROCESSES_LT_PPN =
        "ERROR: The specified processes in the mpi task is smaller and processesPerNode";
    private static final String ERROR_PROCS_NO_MULTIPLE_PPN =
        "ERROR: The specified processes in the mpi task must be multiple of processesPerNode";

    // Inserted method calls
    private static final String EXECUTE_TASK = ".executeTask(";
    private static final String CHECK_SCO_TYPE = "LoaderUtils.checkSCOType(";

    // CLASSES BEING USED
    private static final String SIGNATURE_BUILDER = SignatureBuilder.class.getCanonicalName();
    private static final String DATA_TYPES = DataType.class.getCanonicalName();
    private static final String DATA_TYPE_PSCO = DATA_TYPES + "." + DataType.PSCO_T.name();
    private static final String DATA_TYPE_OBJECT = DATA_TYPES + "." + DataType.OBJECT_T.name();
    private static final String DATA_DIRECTION = Direction.class.getCanonicalName();
    private static final String DATA_STREAM = StdIOStream.class.getCanonicalName();

    final String itWfVar;

    final String className;
    final String methodName;

    final boolean isPrioritary;
    final OnFailure onFailure;
    final int timeOut;
    final int numNodes;
    // Scheduler hints values
    final boolean isReplicated;
    final boolean isDistributed;

    final ArgumentInformation[] params;
    final TargetInformation target;
    final ReturnInformation result;


    /**
     * Constructs a new Task Call object with all the information from an invocation to a task method.
     *
     * @param itWfVar name of the variable containing the workflow
     * @param className name of the called class
     * @param methodName name of the called method
     * @param calledMethod method being called
     * @param itfMethod corresponding method definition from the CEI.
     * @throws CannotCompileException Error in the information included in the CE description
     */
    public TaskCall(String itWfVar, String className, String methodName, CtMethod calledMethod, Method itfMethod)
        throws CannotCompileException {
        this.itWfVar = itWfVar;
        this.className = className;
        this.methodName = methodName;

        // Scheduler hints values
        boolean isReplicated = Boolean.parseBoolean(Constants.IS_NOT_REPLICATED_TASK);
        boolean isDistributed = Boolean.parseBoolean(Constants.IS_NOT_DISTRIBUTED_TASK);
        if (itfMethod.isAnnotationPresent(SchedulerHints.class)) {
            SchedulerHints schedAnnot = itfMethod.getAnnotation(SchedulerHints.class);
            isReplicated = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(schedAnnot.isReplicated()));
            isDistributed = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(schedAnnot.isDistributed()));
        }
        this.isDistributed = isDistributed;
        this.isReplicated = isReplicated;
        String parDirection = "INOUT";

        boolean isPrioritary = Boolean.parseBoolean(Constants.IS_NOT_PRIORITARY_TASK);
        OnFailure onFailure = OnFailure.RETRY;
        int timeOut = 0;
        int numNodes = Constants.SINGLE_NODE;
        // Method: native, Binary, MPI, COMPSs, Multi-Node, OMPSs, OpenCL
        if (itfMethod.isAnnotationPresent(es.bsc.compss.types.annotations.task.Method.class)) {
            es.bsc.compss.types.annotations.task.Method methodAnnot =
                itfMethod.getAnnotation(es.bsc.compss.types.annotations.task.Method.class);
            isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(methodAnnot.priority()));
            onFailure = methodAnnot.onFailure();
            timeOut = Integer.valueOf(methodAnnot.timeOut());
            parDirection = methodAnnot.targetDirection().name();
        } else if (itfMethod.isAnnotationPresent(Binary.class)) {
            Binary binaryAnnot = itfMethod.getAnnotation(Binary.class);
            isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(binaryAnnot.priority()));
        } else if (itfMethod.isAnnotationPresent(MPI.class)) {
            MPI mpiAnnot = itfMethod.getAnnotation(MPI.class);
            isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(mpiAnnot.priority()));
            // Parse processes from environment if needed
            String numNodesSTR = EnvironmentLoader.loadFromEnvironment(mpiAnnot.processes());
            numNodes = (numNodesSTR != null && !numNodesSTR.isEmpty() && !numNodesSTR.equals(Constants.UNASSIGNED))
                ? Integer.valueOf(numNodesSTR)
                : Constants.SINGLE_NODE;
            String ppnSTR = EnvironmentLoader.loadFromEnvironment(mpiAnnot.processesPerNode());
            int ppn =
                (ppnSTR != null && !ppnSTR.isEmpty() && !ppnSTR.equals(Constants.UNASSIGNED)) ? Integer.valueOf(ppnSTR)
                    : 1;
            if (ppn > 1) {
                if (numNodes < ppn) {
                    LOGGER.error(ERROR_PROCESSES_LT_PPN);
                    throw new CannotCompileException(ERROR_PROCESSES_LT_PPN);
                }
                if ((numNodes % ppn) > 0) {
                    LOGGER.error(ERROR_PROCS_NO_MULTIPLE_PPN);
                    throw new CannotCompileException(ERROR_PROCS_NO_MULTIPLE_PPN);
                }
                numNodes = numNodes / ppn;
            }
        } else if (itfMethod.isAnnotationPresent(Decaf.class)) {
            Decaf decafAnnot = itfMethod.getAnnotation(Decaf.class);
            isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(decafAnnot.priority()));
            // Parse computingNodes from environment if needed
            String numNodesSTR = EnvironmentLoader.loadFromEnvironment(decafAnnot.computingNodes());
            numNodes = (numNodesSTR != null && !numNodesSTR.isEmpty() && !numNodesSTR.equals(Constants.UNASSIGNED))
                ? Integer.valueOf(numNodesSTR)
                : Constants.SINGLE_NODE;
        } else if (itfMethod.isAnnotationPresent(COMPSs.class)) {
            COMPSs compssAnnot = itfMethod.getAnnotation(COMPSs.class);
            isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(compssAnnot.priority()));
            // Parse computingNodes from environment if needed
            String numNodesSTR = EnvironmentLoader.loadFromEnvironment(compssAnnot.computingNodes());
            numNodes = (numNodesSTR != null && !numNodesSTR.isEmpty() && !numNodesSTR.equals(Constants.UNASSIGNED))
                ? Integer.valueOf(numNodesSTR)
                : Constants.SINGLE_NODE;
        } else if (itfMethod.isAnnotationPresent(MultiNode.class)) {
            MultiNode multiNodeAnnot = itfMethod.getAnnotation(MultiNode.class);
            isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(multiNodeAnnot.priority()));
            // Parse computingNodes from environment if needed
            String numNodesSTR = EnvironmentLoader.loadFromEnvironment(multiNodeAnnot.computingNodes());
            numNodes = (numNodesSTR != null && !numNodesSTR.isEmpty() && !numNodesSTR.equals(Constants.UNASSIGNED))
                ? Integer.valueOf(numNodesSTR)
                : Constants.SINGLE_NODE;
            parDirection = multiNodeAnnot.targetDirection().name();
        } else if (itfMethod.isAnnotationPresent(OmpSs.class)) {
            OmpSs ompssAnnot = itfMethod.getAnnotation(OmpSs.class);
            isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(ompssAnnot.priority()));
        } else if (itfMethod.isAnnotationPresent(OpenCL.class)) {
            OpenCL openCLAnnot = itfMethod.getAnnotation(OpenCL.class);
            isPrioritary = Boolean.parseBoolean(EnvironmentLoader.loadFromEnvironment(openCLAnnot.priority()));
        }
        this.isPrioritary = isPrioritary;
        this.numNodes = numNodes;
        this.onFailure = onFailure;
        this.timeOut = timeOut;

        Class<?>[] paramTypes = itfMethod.getParameterTypes();
        Annotation[][] paramAnnot = itfMethod.getParameterAnnotations();
        this.params = new ArgumentInformation[paramAnnot.length];
        for (int i = 0; i < paramAnnot.length; i++) {
            Class<?> formalType = paramTypes[i];
            Parameter par = ((Parameter) paramAnnot[i][0]);
            this.params[i] = new ArgumentInformation(i, par, formalType);
        }

        boolean isStatic = Modifier.isStatic(calledMethod.getModifiers());
        if (isStatic) {
            this.target = null;
        } else {
            this.target = new TargetInformation(itfMethod, parDirection);
        }

        Class<?> retType = itfMethod.getReturnType();
        boolean isVoid = retType.equals(void.class);
        if (isVoid) {
            this.result = null;
        } else {
            this.result = new ReturnInformation(retType);
        }
    }

    /**
     * Returns the necessary instructions to invoke the task.
     * 
     * @return necessary instructions to invoke the task.
     */
    public String toCommands() {
        StringBuilder cmd = new StringBuilder();
        cmd.append(itWfVar).append(EXECUTE_TASK);
        cmd.append(SIGNATURE_BUILDER).append(".build(\"").append(this.className).append("\",\"").append(this.methodName)
            .append("\",").append("new ").append(DATA_TYPES).append("[").append("]{");
        cmd.append(Arrays.stream(this.params)
            .map(o -> DATA_TYPE_PSCO.equals(o.type) ? DATA_TYPES + "." + DataType.OBJECT_T.name() : o.type)
            .collect(Collectors.joining(",")));
        cmd.append("}),");

        // Add the onFailure behavior
        cmd.append(OnFailure.class.getCanonicalName() + "." + onFailure).append(',');
        // Add the timeOut time
        cmd.append(timeOut).append(",");

        // Add scheduler common values
        cmd.append(isPrioritary).append(',');
        cmd.append(numNodes).append(",");
        // Default reduce params (Not supported yet in Java)
        cmd.append(false).append(','); // isReduce
        cmd.append(0).append(','); // reduceChunkSize
        cmd.append(isReplicated).append(',');
        cmd.append(isDistributed).append(',');

        // Add if call has target object or not
        boolean hasTarget = target != null;
        cmd.append(hasTarget).append(',');
        int numResults = result == null ? 0 : 1;
        // Add number of returns
        cmd.append("new Integer(").append(numResults).append("),");
        int numParams = params.length + (hasTarget ? 1 : 0) + numResults;
        // Add parameters
        cmd.append(numParams).append(',');

        if (numParams > 0) {
            cmd.insert(0, getPreCall());
            cmd.append(getCallParameters()).append(");");
            cmd.append(getPostCall());
        } else {
            cmd.append("null);");
        }
        return cmd.toString();
    }

    private String getPreCall() {
        StringBuilder preCall = new StringBuilder();
        for (ArgumentInformation pi : params) {
            preCall.append(pi.getParamPreparation());
        }
        if (target != null) {
            preCall.append(target.getParamPreparation());
        }
        if (result != null) {
            preCall.append(result.getParamPreparation());
        }
        return preCall.toString();
    }

    private String getCallParameters() {
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
        if (this.target != null) {
            if (addedParameter) {
                onCall.append(",");
            } else {
                addedParameter = true;
            }
            onCall.append(this.target.getParamDesc());
        }
        if (result != null) {
            // Assuming object, it is unlikely that a user selects a method invoked on an array
            if (addedParameter) {
                onCall.append(",");
            }
            onCall.append(result.getParamDesc());
        }
        onCall.append("}");
        return onCall.toString();
    }

    private String getPostCall() {
        StringBuilder postCall = new StringBuilder();
        for (ArgumentInformation pi : params) {
            postCall.append(pi.getParamCleanup());
        }
        if (result != null) {
            postCall.append(result.getResultCollection());
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

        private final String type;
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
                    paramPreparation = CallGenerator.addTaskFile(parVal) + ";";
                    if (par.direction() == Direction.IN_DELETE) {
                        paramCleanup = CallGenerator.removeTaskFile(parVal) + ";";
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
                        paramPreparation = CallGenerator.wfObjectParameter(itWfVar, "$" + (paramIndex + 1)) + ";";
                        parType = CHECK_SCO_TYPE + "$" + (paramIndex + 1) + ")";
                        if (par.direction() == Direction.IN_DELETE) {
                            paramCleanup = CallGenerator.wfDeleteObject(itWfVar, "$" + (paramIndex + 1)) + ";";
                        }
                    }
                    break;
            }
            this.type = parType;
            this.paramPreparation = paramPreparation;
            this.paramDesc = buildParameter(parVal, parType, par.direction().name(), par.stream().name(), par.prefix(),
                par.name(), parContent, par.weight(), par.keepRename());
            this.paramCleanup = paramCleanup;
        }

        public String getType() {
            return type;
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

        private final String paramPreparation;
        private final String targetDescription;


        public TargetInformation(Method declaredMethod, String parDirection) {
            String tgtVal = "$0";
            String tgtType = CHECK_SCO_TYPE + "$0)";

            this.paramPreparation = CallGenerator.wfObjectParameter(itWfVar, tgtVal) + ";";
            this.targetDescription = buildParameter(tgtVal, tgtType, parDirection, "");
        }

        public String getParamPreparation() {
            return this.paramPreparation;
        }

        public String getParamDesc() {
            return this.targetDescription;
        }
    }

    /**
     * Represents the return object of a given task call.
     */
    private class ReturnInformation {

        private final String paramPreparation;
        private final String paramDesc;
        private final String resultCollection;


        public ReturnInformation(Class<?> retType) throws CannotCompileException {
            StringBuilder resPreparation = new StringBuilder();
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
                resPreparation.append("Object ").append(tempRetVar).append(" = ");
                String cast;
                String converterMethod;
                if (retType.isAssignableFrom(boolean.class)) {
                    resPreparation.append("new Boolean(false);");
                    cast = "(Boolean)";
                    converterMethod = "booleanValue()";
                } else if (retType.isAssignableFrom(char.class)) {
                    resPreparation.append("new Character(Character.MIN_VALUE);");
                    cast = "(Character)";
                    converterMethod = "charValue()";
                } else if (retType.isAssignableFrom(byte.class)) {
                    resPreparation.append("new Byte(Byte.MIN_VALUE);");
                    cast = "(Byte)";
                    converterMethod = "byteValue()";
                } else if (retType.isAssignableFrom(short.class)) {
                    resPreparation.append("new Short(Short.MIN_VALUE);");
                    cast = "(Short)";
                    converterMethod = "shortValue()";
                } else if (retType.isAssignableFrom(int.class)) {
                    resPreparation.append("new Integer(Integer.MIN_VALUE);");
                    cast = "(Integer)";
                    converterMethod = "intValue()";
                } else if (retType.isAssignableFrom(long.class)) {
                    resPreparation.append("new Long(Long.MIN_VALUE);");
                    cast = "(Long)";
                    converterMethod = "longValue()";
                } else if (retType.isAssignableFrom(float.class)) {
                    resPreparation.append("new Float(Float.MIN_VALUE);");
                    cast = "(Float)";
                    converterMethod = "floatValue()";
                } else { // (retType.isAssignableFrom(double.class))
                    resPreparation.append("new Double(Double.MIN_VALUE);");
                    cast = "(Double)";
                    converterMethod = "doubleValue()";
                }

                parValue = tempRetVar;
                parType = DATA_TYPES + ".OBJECT_T";
                contentType = retType.toString();

                /*
                 * After execute task, register an access to the wrapper object, get its (remotely) generated value and
                 * assign it to the application's primitive type var
                 */
                resCollection.append("$_ = (").append(cast).append(CallGenerator.wfAccessObject(itWfVar, tempRetVar))
                    .append(").").append(converterMethod).append(";");
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
                resPreparation.append("$_ = new ").append(compTypeName).append(dims).append(';');
            } else {
                parValue = "$_";
                parType = CHECK_SCO_TYPE + "$_)";
                // OBJECT
                // Wrapper for a primitive type: return a default value
                if (retType.isAssignableFrom(Boolean.class)) {
                    resPreparation.append("$_ = new Boolean(false);");
                } else if (retType.isAssignableFrom(Character.class)) {
                    resPreparation.append("$_ = new Character(Character.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Byte.class)) {
                    resPreparation.append("$_ = new Byte(Byte.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Short.class)) {
                    resPreparation.append("$_ = new Short(Short.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Integer.class)) {
                    resPreparation.append("$_ = new Integer(Integer.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Long.class)) {
                    resPreparation.append("$_ = new Long(Long.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Float.class)) {
                    resPreparation.append("$_ = new Float(Float.MIN_VALUE);");
                } else if (retType.isAssignableFrom(Double.class)) {
                    resPreparation.append("$_ = new Double(Double.MIN_VALUE);");
                } else {
                    // Object (maybe String): use the no-args constructor
                    // Check that object class has empty constructor
                    String typeName = retType.getName();
                    try {
                        Class.forName(typeName).getConstructor();
                    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
                        throw new CannotCompileException(ERROR_NO_EMPTY_CONSTRUCTOR + typeName);
                    }

                    resPreparation.append("$_ = new ").append(typeName).append("();");
                }
            }

            resPreparation.append(CallGenerator.wfObjectParameter(itWfVar, parValue)).append(";");
            param = buildOutParameter(parValue, parType, contentType);

            this.paramPreparation = resPreparation.toString();
            this.paramDesc = param;
            this.resultCollection = resCollection.toString();
        }

        public String getParamDesc() {
            return this.paramDesc;
        }

        public String getParamPreparation() {
            return this.paramPreparation;
        }

        public String getResultCollection() {
            return this.resultCollection;
        }

    }

    public static class SignatureBuilder {

        private SignatureBuilder() {

        }

        /**
         * Constructs a signature for a given method.
         * 
         * @param className name of the class containing the method
         * @param methodName name of the method
         * @param argType data type of the arguments
         * @return signature of the invoked method
         */
        public static String build(String className, String methodName, DataType[] argType) {
            return methodName + "(" // methodName(
                + Arrays.stream(argType) // Create a comma-separated list for types
                    .map(dt -> dt == DataType.PSCO_T ? DataType.OBJECT_T.name() : dt.name()) // PSCO_T ->OBJECT
                    .collect(Collectors.joining(","))
                + ")" + className;
        }
    }
}
