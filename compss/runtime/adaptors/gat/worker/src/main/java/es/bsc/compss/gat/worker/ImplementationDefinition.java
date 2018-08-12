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

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamURI;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.job.Job.JobHistory;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.util.ErrorManager;

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

    private final LinkedList<Param> arguments = new LinkedList<>();
    private final Param target;
    private final LinkedList<Param> results = new LinkedList<>();

    public ImplementationDefinition(boolean enableDebug, String args[], int appArgsIdx) {
        System.out.println("jobId = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
        jobId = Integer.parseInt(args[appArgsIdx++]);
        System.out.println("task = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
        taskId = Integer.parseInt(args[appArgsIdx++]);
        history = JobHistory.NEW;

        this.debug = enableDebug;

        System.out.println("numNodes = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
        int numNodes = Integer.parseInt(args[appArgsIdx++]);
        hostnames = new ArrayList<>();
        for (int i = 0; i < numNodes; ++i) {
            System.out.println("node " + i + " = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
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
        System.out.println("Computing Units = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
        cus = Integer.parseInt(args[appArgsIdx++]);

        System.out.println("Number Arguments = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
        int numParams = Integer.parseInt(args[appArgsIdx++]);
        // Get if has target or not
        System.out.println("Has Target = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
        hasTarget = Boolean.parseBoolean(args[appArgsIdx++]);

        System.out.println("Number Returns = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
        int numReturns = Integer.parseInt(args[appArgsIdx++]);

        LinkedList<Param> paramsTmp;
        try {
            paramsTmp = parseArguments(args, appArgsIdx, numParams, numReturns);
        } catch (Exception e) {
            ErrorManager.error(e.getMessage());
            paramsTmp = new LinkedList<>();
        }

        Iterator<Param> paramsItr = paramsTmp.descendingIterator();
        for (int i = 0; i < numReturns; i++) {
            results.addFirst(paramsItr.next());
        }
        if (hasTarget) {
            target = paramsItr.next();
        } else {
            target = null;
        }
        while (paramsItr.hasNext()) {
            arguments.addFirst(paramsItr.next());
        }
    }

    private LinkedList<Param> parseArguments(String[] args, int appArgsIdx, int numParams, int numReturns) throws Exception {
        System.out.println("Parameters: ");
        LinkedList<Param> paramsList = new LinkedList<>();
        DataType[] dataTypesEnum = DataType.values();
        Stream[] dataStream = Stream.values();

        int totalParams = numParams + numReturns + (hasTarget ? 1 : 0);
        for (int paramIdx = 0; paramIdx < totalParams; paramIdx++) {
            System.out.println("\t - Parameter " + paramIdx);
            DataType argType;

            Stream stream;
            String prefix;

            //Object and primitiveTypes
            Object value = null;
            //File
            String originalName = "NO_NAME";
            boolean writeFinal = false;
            System.out.println("\t\tType = args[" + appArgsIdx + "]=" + args[appArgsIdx] + " = " + dataTypesEnum[Integer.parseInt(args[appArgsIdx])]);
            int argTypeIdx = Integer.parseInt(args[appArgsIdx++]);
            if (argTypeIdx >= dataTypesEnum.length) {
                ErrorManager.error(WARN_UNSUPPORTED_DATA_TYPE + argTypeIdx);
            }
            argType = dataTypesEnum[argTypeIdx];

            System.out.println("\t\tStream = args[" + appArgsIdx + "]=" + args[appArgsIdx] + " = " + dataStream[Integer.parseInt(args[appArgsIdx])]);
            int argStreamIdx = Integer.parseInt(args[appArgsIdx++]);
            if (argStreamIdx >= dataStream.length) {
                ErrorManager.error(WARN_UNSUPPORTED_STREAM + argStreamIdx);
            }
            stream = dataStream[argStreamIdx];

            System.out.println("\t\tPrefix = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
            prefix = args[appArgsIdx++];
            if (prefix == null || prefix.isEmpty()) {
                prefix = Constants.PREFIX_EMTPY;
            }
            switch (argType) {
                case FILE_T:
                    System.out.println("\t\tOriginal Name = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    originalName = args[appArgsIdx++];
                    System.out.println("\t\tLocation = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    value = args[appArgsIdx++];
                    break;
                case OBJECT_T:
                case BINDING_OBJECT_T:
                case PSCO_T:
                    System.out.println("\t\tValue = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    String fileLocation = (String) args[appArgsIdx++];
                    value = fileLocation;
                    originalName = fileLocation;
                    System.out.println("\t\tWrite Final = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    writeFinal = ((String) args[appArgsIdx++]).equals("W");
                    break;
                case EXTERNAL_PSCO_T:
                    System.out.println("\t\tValue = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    value = args[appArgsIdx++];
                    break;
                case BOOLEAN_T:
                    System.out.println("\t\tValue = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    value = Boolean.valueOf(args[appArgsIdx++]);
                    break;
                case CHAR_T:
                    System.out.println("\t\tValue = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    value = args[appArgsIdx++].charAt(0);
                    break;
                case STRING_T:
                    System.out.println("\t\tNum substrings = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    int numSubStrings = Integer.parseInt(args[appArgsIdx++]);
                    String aux = "";
                    for (int j = 0; j < numSubStrings; j++) {
                        if (j != 0) {
                            aux += " ";
                        }
                        System.out.println("\t\t\t args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                        aux += args[appArgsIdx++];
                    }
                    value = aux;
                    break;
                case BYTE_T:
                    System.out.println("\t\tValue = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    value = new Byte(args[appArgsIdx++]);
                    break;
                case SHORT_T:
                    System.out.println("\t\tValue = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    value = new Short(args[appArgsIdx++]);
                    break;
                case INT_T:
                    System.out.println("\t\tValue = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    value = new Integer(args[appArgsIdx++]);
                    break;
                case LONG_T:
                    System.out.println("\t\tValue = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    value = new Long(args[appArgsIdx++]);
                    break;
                case FLOAT_T:
                    System.out.println("\t\tValue = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
                    value = new Float(args[appArgsIdx++]);
                    break;
                case DOUBLE_T:
                    System.out.println("\t\tValue = args[" + appArgsIdx + "]=" + args[appArgsIdx]);
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

        return paramsList;
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
        sb.append("Parameters  (").append(arguments.size()).append(")\n");

        int idx = 0;
        for (Param p : arguments) {
            sb.append(" * Arguments ").append(idx).append("\n");
            sb.append("     - Type ").append(p.type).append("\n");
            sb.append("     - Prefix ").append(p.prefix).append("\n");
            sb.append("     - Stream ").append(p.stream).append("\n");
            sb.append("     - Original name ").append(p.originalName).append("\n");
            sb.append("     - Value ").append(p.value).append("\n");
            sb.append("     - Write final value ").append(p.writeFinalValue).append("\n");
            idx++;
        }
        if (hasTarget) {
            sb.append(" * Target Object\n");
            sb.append("     - Type ").append(target.type).append("\n");
            sb.append("     - Prefix ").append(target.prefix).append("\n");
            sb.append("     - Stream ").append(target.stream).append("\n");
            sb.append("     - Original name ").append(target.originalName).append("\n");
            sb.append("     - Value ").append(target.value).append("\n");
            sb.append("     - Write final value ").append(target.writeFinalValue).append("\n");
        }
        idx = 0;
        sb.append("Returns  (").append(arguments.size()).append(")\n");
        for (Param p : results) {
            sb.append(" * Return ").append(idx).append("\n");
            sb.append("     - Original name ").append(p.originalName).append("\n");
            sb.append("     - Value ").append(p.value).append("\n");
            sb.append("     - Write final value ").append(p.writeFinalValue).append("\n");
            idx++;
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

    @Override
    public List<? extends InvocationParam> getParams() {
        return arguments;
    }

    @Override
    public InvocationParam getTarget() {
        return target;
    }

    @Override
    public List<? extends InvocationParam> getResults() {
        return results;
    }

    @Override
    public List<String> getSlaveNodesNames() {
        return this.hostnames;
    }

    public int getComputingUnits() {
        return cus;
    }

    @Override
    public JobHistory getHistory() {
        return history;
    }


    private static class Param implements InvocationParam {

        private DataType type;
        private Object value;
        private Class<?> valueClass;

        private final String prefix;
        private final Stream stream;
        private String originalName;
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
        public void setOriginalName(String originalName) {
            this.originalName = originalName;
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
        public void setValueClass(Class<?> valClass) {
            this.valueClass = valClass;
        }

        @Override
        public Class<?> getValueClass() {
            return this.valueClass;
        }

        @Override
        public String getDataMgmtId() {
            //Unrelevant information
            return null;
        }

        @Override
        public String getSourceDataId() {
            //Unrelevant information
            return null;
        }

        @Override
        public List<InvocationParamURI> getSources() {
            //File is not available to be fetch from anywhere
            return null;
        }
    }
}
