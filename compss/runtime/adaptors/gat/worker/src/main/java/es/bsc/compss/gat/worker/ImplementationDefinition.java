/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.gat.worker;

import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamURI;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.job.Job.JobHistory;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.util.ErrorManager;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public abstract class ImplementationDefinition implements Invocation {

    public static final String ERROR_INVOKE = "Error invoking requested method";
    private static final String ERROR_APP_PARAMETERS = "ERROR: Incorrect number of parameters";
    private static final String WARN_UNSUPPORTED_DATA_TYPE = "WARNING: Unsupported data type";
    private static final String WARN_UNSUPPORTED_STREAM = "WARNING: Unsupported data stream";

    private final boolean debug;
    private final int jobId;
    private final int taskId;
    private final JobHistory history;

    private final List<String> hostnames;
    private final int cus;

    private final boolean hasTarget;
    private final boolean hasReturn;

    private final int numParams;
    private final LinkedList<Param> params;

    public ImplementationDefinition(boolean enableDebug, String args[], int appArgsIdx) {
        jobId = 0;
        taskId = 0;
        history = JobHistory.NEW;

        this.debug = enableDebug;

        int numNodes = Integer.parseInt(args[appArgsIdx++]);
        hostnames = new ArrayList<>();
        for (int i = 0; i < numNodes; ++i) {
            String nodeName = args[appArgsIdx++];
            if (nodeName.endsWith("-ib0")) {
                nodeName = nodeName.substring(0, nodeName.lastIndexOf("-ib0"));
            }
            hostnames.add(nodeName);
        }
        String hostname = "localhost";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            ErrorManager.warn("Cannot obtain hostname. Loading default value " + hostname);
        }
        hostnames.add(hostname);
        cus = Integer.parseInt(args[appArgsIdx++]);

        // Get if has target or not
        hasTarget = Boolean.parseBoolean(args[appArgsIdx++]);

        // Get return type if specified
        String returnType = args[appArgsIdx++];
        if (returnType == null || returnType.equals("null") || returnType.isEmpty()) {
            hasReturn = false;
        } else {
            hasReturn = true;
        }
        int numReturns = Integer.parseInt(args[appArgsIdx++]);

        numParams = Integer.parseInt(args[appArgsIdx++]);
        LinkedList<Param> paramsTmp;
        try {
            paramsTmp = parseArguments(args, appArgsIdx);
        } catch (Exception e) {
            ErrorManager.error(e.getMessage());
            paramsTmp = new LinkedList<>();
        }
        params = paramsTmp;

    }

    @Override
    public int getJobId() {
        return this.jobId;
    }

    @Override
    public boolean isDebugEnabled() {
        return this.debug;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Has Target: ").append(hasTarget).append("\n");
        sb.append("Has Return: ").append(hasReturn).append("\n");
        sb.append("Parameters  (").append(numParams).append(")\n");

        Iterator<Param> paramsIter = params.iterator();
        int paramId = 0;
        for (; paramId < (hasTarget ? numParams - 1 : numParams); paramId++) {
            Param p = paramsIter.next();
            sb.append(" * Parameter ").append(paramId).append("\n");
            sb.append("     - Type ").append(p.type).append("\n");
            sb.append("     - Prefix ").append(p.prefix).append("\n");
            sb.append("     - Stream ").append(p.stream).append("\n");
            sb.append("     - Original name ").append(p.originalName).append("\n");
            sb.append("     - Value ").append(p.value).append("\n");
            sb.append("     - Write final value ").append(p.writeFinalValue).append("\n");
        }
        if (hasTarget) {
            Param p = paramsIter.next();
            sb.append(" * Target Object\n");
            sb.append("     - Type ").append(p.type).append("\n");
            sb.append("     - Prefix ").append(p.prefix).append("\n");
            sb.append("     - Stream ").append(p.stream).append("\n");
            sb.append("     - Original name ").append(p.originalName).append("\n");
            sb.append("     - Value ").append(p.value).append("\n");
            sb.append("     - Write final value ").append(p.writeFinalValue).append("\n");
        }

        if (hasReturn) {
            Param p = paramsIter.next();
            sb.append(" * Return\n");
            sb.append("     - Original name ").append(p.originalName).append("\n");
            sb.append("     - Value ").append(p.value).append("\n");
            sb.append("     - Write final value ").append(p.writeFinalValue).append("\n");
        }
        return sb.toString();
    }

    @Override
    public final int getTaskId() {
        return this.taskId;
    }

    @Override
    public final TaskType getTaskType() {
        return TaskType.METHOD;
    }

    @Override
    public abstract AbstractMethodImplementation getMethodImplementation();

    @Override
    public MethodResourceDescription getRequirements() {
        MethodResourceDescription mrd = new MethodResourceDescription();
        Processor p = new Processor();
        p.setComputingUnits(cus);
        mrd.addProcessor(p);
        return mrd;
    }

    public abstract MethodType getType();

    public abstract String toCommandString();

    public abstract String toLogString();

    public abstract Invoker getInvoker(InvocationContext context, File sandBoxDir) throws JobExecutionException;

    private LinkedList<Param> parseArguments(String[] args, int appArgsIdx) throws Exception {
        // Check received arguments
        if (args.length < 2 * numParams + appArgsIdx) {
            ErrorManager.error(ERROR_APP_PARAMETERS);
        }

        LinkedList<Param> paramsList = new LinkedList<>();
        DataType[] dataTypesEnum = DataType.values();
        Stream[] dataStream = Stream.values();

        for (int paramIdx = 0; paramIdx < numParams; paramIdx++) {
            DataType argType;

            Stream stream;
            String prefix;

            //Object and primitiveTypes
            Object value = null;
            //File
            String originalName = "NO_NAME";
            boolean writeFinal = false;

            int argTypeIdx = Integer.parseInt(args[appArgsIdx++]);
            if (argTypeIdx >= dataTypesEnum.length) {
                ErrorManager.error(WARN_UNSUPPORTED_DATA_TYPE + argTypeIdx);
            }
            argType = dataTypesEnum[argTypeIdx];

            int argStreamIdx = Integer.parseInt(args[appArgsIdx++]);
            if (argStreamIdx >= dataStream.length) {
                ErrorManager.error(WARN_UNSUPPORTED_STREAM + argStreamIdx);
            }
            stream = dataStream[argStreamIdx];

            prefix = args[appArgsIdx++];
            if (prefix == null || prefix.isEmpty()) {
                prefix = Constants.PREFIX_EMTPY;
            }
            switch (argType) {
                case FILE_T:
                    originalName = args[appArgsIdx++];
                    value = originalName;
                    break;
                case OBJECT_T:
                case BINDING_OBJECT_T:
                case PSCO_T:
                    value = (String) args[appArgsIdx++];
                    writeFinal = ((String) args[appArgsIdx++]).equals("W");
                    break;
                case EXTERNAL_PSCO_T:
                    value = args[appArgsIdx++];
                    break;
                case BOOLEAN_T:
                    value = Boolean.valueOf(args[appArgsIdx++]);
                    break;
                case CHAR_T:
                    value = args[appArgsIdx++].charAt(0);
                    break;
                case STRING_T:
                    int numSubStrings = Integer.parseInt(args[appArgsIdx++]);
                    String aux = "";
                    for (int j = 0; j < numSubStrings; j++) {
                        if (j != 0) {
                            aux += " ";
                        }
                        aux += args[appArgsIdx++];
                    }
                    value = aux;
                    break;
                case BYTE_T:
                    value = new Byte(args[appArgsIdx++]);
                    break;
                case SHORT_T:
                    value = new Short(args[appArgsIdx++]);
                    break;
                case INT_T:
                    value = new Integer(args[appArgsIdx++]);
                    break;
                case LONG_T:
                    value = new Long(args[appArgsIdx++]);
                    break;
                case FLOAT_T:
                    value = new Float(args[appArgsIdx++]);
                    break;
                case DOUBLE_T:
                    value = new Double(args[appArgsIdx++]);
                    break;
                default:
                    throw new Exception(WARN_UNSUPPORTED_DATA_TYPE + argType);
            }

            Param p = new Param(argType, prefix, stream, originalName, writeFinal);
            if (value != null) {
                p.setValue(value);
            }
            paramsList.add(p);
        }
        if (hasReturn) {
            Param returnParam = new Param(DataType.OBJECT_T, "", Stream.UNSPECIFIED, "NO_NAME", true);
            returnParam.setValue(args[appArgsIdx + 3]);
            paramsList.add(returnParam);
        }
        return paramsList;
    }

    @Override
    public List<InvocationParam> getParams() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public InvocationParam getTarget() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<InvocationParam> getResults() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<String> getSlaveNodesNames() {
        return this.hostnames;
    }

    @Override
    public JobHistory getHistory() {
        return history;
    }


    private static class Param implements InvocationParam {

        private DataType type;
        private Object value;

        private final String prefix;
        private final Stream stream;
        private final String originalName;
        private final boolean writeFinalValue;

        public Param(DataType type, String prefix, Stream stream, String originalName, boolean writeFinalValue) {
            this.type = type;
            this.prefix = prefix;
            this.stream = stream;
            this.originalName = originalName;
            this.writeFinalValue = writeFinalValue;
        }

        @Override
        public void setType(DataType type) {
            this.type = type;
        }

        @Override
        public DataType getType() {
            return this.type;
        }

        @Override
        public boolean isPreserveSourceData() {
            return false;
        }

        @Override
        public boolean isWriteFinalValue() {
            return writeFinalValue;
        }

        @Override
        public String getPrefix() {
            return this.prefix;
        }

        @Override
        public Stream getStream() {
            return this.stream;
        }

        @Override
        public String getOriginalName() {
            return this.originalName;
        }

        @Override
        public Object getValue() {
            return this.value;
        }

        @Override
        public void setValue(Object val) {
            this.value = val;
        }

        @Override
        public void setValueClass(Class<?> aClass) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Class<?> getValueClass() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String getDataMgmtId() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void setOriginalName(String originalName) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public List<InvocationParamURI> getSources() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }
}
