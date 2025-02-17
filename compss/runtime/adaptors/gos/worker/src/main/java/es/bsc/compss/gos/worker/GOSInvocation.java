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
package es.bsc.compss.gos.worker;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamURI;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.util.ErrorManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class GOSInvocation implements Invocation {

    protected static final String ERROR_INVOKE = "Error invoking requested method";
    private static final String WARN_UNSUPPORTED_DATA_TYPE = "WARNING: Unsupported data type";
    private static final String WARN_UNSUPPORTED_STREAM = "WARNING: Unsupported data stream";

    private final boolean debug;
    private final int jobId;
    private final int taskId;
    private final Lang lang;
    private final JobHistory history;
    private final OnFailure onFailure;
    private final long timeout;

    private final List<String> hostnames;
    private final String cpuMap;
    private final int cus;
    private final String gpuMap;
    private final int gus;
    private final String fpgaMap;
    private final int fpgaus;

    private final boolean hasTarget;

    private final LinkedList<Param> arguments = new LinkedList<>();
    private final Param target;
    private final LinkedList<Param> results = new LinkedList<>();
    private final AbstractMethodImplementation impl;


    /**
     * Creates a new ImplementationDefinition instance.
     * 
     * @param enableDebug Whether the debug mode is enabled or not.
     * @param impl Abstract method Implementation.
     * @param args Application arguments.
     * @param appArgsIdx Application arguments parsing index.
     */
    public GOSInvocation(boolean enableDebug, Lang lang, AbstractMethodImplementation impl, String[] args,
        int appArgsIdx) {

        this.impl = impl;
        this.jobId = Integer.parseInt(args[appArgsIdx++]);
        this.taskId = Integer.parseInt(args[appArgsIdx++]);
        this.lang = lang;
        this.history = JobHistory.valueOf(args[appArgsIdx++]);

        this.timeout = Long.parseLong(args[appArgsIdx++]);
        this.onFailure = OnFailure.valueOf(args[appArgsIdx++]);

        this.debug = enableDebug;

        int numNodes = Integer.parseInt(args[appArgsIdx++]);
        this.hostnames = new ArrayList<>();
        for (int i = 0; i < numNodes; ++i) {
            String nodeName = args[appArgsIdx++];
            if (nodeName.endsWith("-ib0")) {
                nodeName = nodeName.substring(0, nodeName.lastIndexOf("-ib0"));
            }
            this.hostnames.add(nodeName);
        }

        this.cus = Integer.parseInt(args[appArgsIdx++]);
        this.cpuMap = args[appArgsIdx++];

        this.gus = Integer.parseInt(args[appArgsIdx++]);
        this.gpuMap = args[appArgsIdx++];

        this.fpgaus = Integer.parseInt(args[appArgsIdx++]);
        this.fpgaMap = args[appArgsIdx++];

        int numParams = Integer.parseInt(args[appArgsIdx++]);
        // Get if has target or not
        this.hasTarget = Boolean.parseBoolean(args[appArgsIdx++]);
        int numReturns = Integer.parseInt(args[appArgsIdx++]);

        LinkedList<Param> paramsTmp;
        try {
            paramsTmp = parseArguments(args, appArgsIdx, numParams, numReturns);
        } catch (Exception e) {
            ErrorManager.error("Exception parsing arguments", e);
            paramsTmp = new LinkedList<>();
        }

        Iterator<Param> paramsItr = paramsTmp.descendingIterator();
        for (int i = 0; i < numReturns; i++) {
            this.results.addFirst(paramsItr.next());
        }
        if (this.hasTarget) {
            this.target = paramsItr.next();
        } else {
            this.target = null;
        }
        while (paramsItr.hasNext()) {
            GOSInvocation.Param p = paramsItr.next();
            this.arguments.addFirst(p);
        }
    }

    private LinkedList<Param> parseArguments(String[] args, int appArgsIdx, int numParams, int numReturns)
        throws Exception {
        LinkedList<Param> paramsList = new LinkedList<>();
        DataType[] dataTypesEnum = DataType.values();
        StdIOStream[] dataStream = StdIOStream.values();

        int totalParams = numParams + numReturns + (hasTarget ? 1 : 0);
        for (int paramIdx = 0; paramIdx < totalParams; paramIdx++) {
            // Object and primitiveTypes
            Object value = null;
            // File
            String originalName = "NO_NAME";
            boolean writeFinal = false;
            int argTypeIdx = Integer.parseInt(args[appArgsIdx++]);
            if (argTypeIdx >= dataTypesEnum.length) {
                ErrorManager.error(WARN_UNSUPPORTED_DATA_TYPE + argTypeIdx);
            }
            final DataType argType = dataTypesEnum[argTypeIdx];

            int argStreamIdx = Integer.parseInt(args[appArgsIdx++]);
            if (argStreamIdx >= dataStream.length) {
                ErrorManager.error(WARN_UNSUPPORTED_STREAM + argStreamIdx);
            }
            final StdIOStream stream = dataStream[argStreamIdx];

            String prefix = args[appArgsIdx++];
            if (prefix == null || prefix.isEmpty()) {
                prefix = Constants.PREFIX_EMPTY;
            }

            String name = args[appArgsIdx++];
            if (name.compareTo("null") == 0) {
                name = "";
            }

            String pyType = args[appArgsIdx++];
            if (pyType.compareTo("null") == 0) {
                pyType = "";
            }

            double weight = Double.parseDouble(args[appArgsIdx++]);
            boolean keepRename = Boolean.parseBoolean(args[appArgsIdx++]);

            // // So far, not available in args array
            // String pyType = "null";

            switch (argType) {
                case FILE_T:
                case EXTERNAL_STREAM_T:
                    originalName = args[appArgsIdx++];
                    value = args[appArgsIdx++];
                    break;
                case OBJECT_T:
                case BINDING_OBJECT_T:
                case STREAM_T:
                case PSCO_T:
                    String fileLocation = (String) args[appArgsIdx++];
                    value = fileLocation;
                    originalName = fileLocation;
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
                case STRING_64_T:
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

            Param p = new Param(argType, prefix, name, pyType, stream, weight, keepRename, originalName, writeFinal);
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
    public List<Integer> getPredecessors() {
        return null;
    }

    @Override
    public Integer getNumSuccessors() {
        return 0;
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
        return impl.getTaskType();
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return this.impl;
    }

    @Override
    public MethodResourceDescription getRequirements() {
        MethodResourceDescription mrd = new MethodResourceDescription();
        Processor p = new Processor();
        p.setComputingUnits(cus);
        mrd.addProcessor(p);
        return mrd;
    }

    public MethodType getType() {
        return this.impl.getMethodType();
    }

    public String toLogString() {
        return this.impl.getMethodDefinition();
    }

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

    public String getGPUMAp() {
        return this.cpuMap;
    }

    public int getGPUComputingUnits() {
        return this.gus;
    }

    public String getFPGAMap() {
        return this.fpgaMap;
    }

    public int getFPGAUnits() {
        return this.fpgaus;
    }

    public String getCPUMap() {
        return this.cpuMap;
    }

    public int getComputingUnits() {
        return cus;
    }

    @Override
    public JobHistory getHistory() {
        return history;
    }

    @Override
    public Lang getLang() {
        return lang;
    }

    @Override
    public long getTimeOut() {
        return timeout;
    }

    @Override
    public OnFailure getOnFailure() {
        return onFailure;
    }

    @Override
    public boolean producesEmptyResultsOnFailure() {
        return true;
    }

    @Override
    public String getParallelismSource() {
        return null;
    }

    @Override
    public void executionStarts() {
        // Profiles are not collected because they cannot be sent back to the master
    }

    @Override
    public void executionEnds() {
        // Profiles are not collected because they cannot be sent back to the master
    }


    private static class Param implements InvocationParam {

        private DataType type;
        private Object value;
        private Class<?> valueClass;
        private boolean collective;
        private final String prefix;
        private final String name;
        private final String contentType;
        private final double weight;
        private final boolean keepRename;
        private final StdIOStream stream;
        private String originalName;
        private String renamedName;
        private final boolean writeFinalValue;
        private boolean forwarded = false;


        public Param(DataType type, String prefix, String name, String contentType, StdIOStream stream, double weight,
            boolean keepRename, String originalName, boolean writeFinalValue) {
            this.type = type;
            this.prefix = prefix;
            this.name = name;
            this.contentType = contentType;
            this.weight = weight;
            this.keepRename = keepRename;
            this.stream = stream;
            this.originalName = originalName;
            this.writeFinalValue = writeFinalValue;
            this.collective = type == DataType.COLLECTION_T || type == DataType.DICT_COLLECTION_T;

        }

        @Override
        public void setType(DataType type) {
            this.type = type;
            this.collective = (type == DataType.COLLECTION_T || type == DataType.DICT_COLLECTION_T);
        }

        @Override
        public DataType getType() {
            return this.type;
        }

        @Override
        public boolean isCollective() {
            return collective;

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
        public String getName() {
            return this.name;
        }

        @Override
        public String getContentType() {
            return this.contentType;
        }

        @Override
        public StdIOStream getStdIOStream() {
            return this.stream;
        }

        @Override
        public double getWeight() {
            return this.weight;
        }

        @Override
        public boolean isKeepRename() {
            return this.keepRename;
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
        public String getRenamedName() {
            return this.renamedName;
        }

        @Override
        public void setRenamedName(String renamedName) {
            this.renamedName = renamedName;
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
            // Unrelevant information
            return null;
        }

        @Override
        public String getSourceDataId() {
            // Unrelevant information
            return null;
        }

        @Override
        public List<InvocationParamURI> getSources() {
            // File is not available to be fetch from anywhere
            return null;
        }

        @Override
        public void resultIsForwarded() {
            forwarded = true;
        }

        @Override
        public boolean isForwardedResult() {
            return forwarded;
        }

    }
}
