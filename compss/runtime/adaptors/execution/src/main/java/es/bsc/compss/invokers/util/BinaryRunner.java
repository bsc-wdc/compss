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
package es.bsc.compss.invokers.util;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.exceptions.ExternalPropertyException;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.types.StdIOStream;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.util.ExternalStreamHandler;
import es.bsc.compss.util.StreamGobbler;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.util.serializers.Serializer;
import es.bsc.distrostreamlib.api.DistroStream;
import es.bsc.distrostreamlib.api.files.FileDistroStream;
import es.bsc.distrostreamlib.api.objects.ObjectDistroStream;
import es.bsc.distrostreamlib.client.DistroStreamClient;
import es.bsc.distrostreamlib.requests.CloseStreamRequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.ProcessBuilder.Redirect;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;


public class BinaryRunner {

    private static final String ERROR_PARAM_NOT_STRING = "ERROR: Binary parameter cannot be serialized to string";
    private static final String ERROR_STREAM = "ERROR: Object and PSCO streams are not supported in non-native tasks";

    private static final String ERROR_OUTPUTREADER = "ERROR: Cannot retrieve command output";
    private static final String ERROR_ERRORREADER = "ERROR: Cannot retrieve command error";
    private static final String ERROR_PROC_EXEC = "ERROR: Exception executing Binary command";
    private static final String ERROR_EXIT_VALUE = "ERROR: Exception exit value is not 0";

    private static final String ERROR_EXT_STREAM_BASE_DIR = "ERROR: Cannot retrieve base_dir from External Stream";
    private static final String ERROR_EXT_STREAM_CLOSURE = "ERROR: Cannot close External Stream due to internal error.";
    private static final String ERROR_EXT_STREAM_GET_ID = "ERROR: Cannot close External Stream due to innvalid Id";

    private static final String DEFAULT_PB_KILL_SIGNAL = "15";
    private static final int PB_KILL_SIGNAL =
        Integer.valueOf(System.getProperty(COMPSsConstants.WORKER_BINARY_KILL_SIGNAL, DEFAULT_PB_KILL_SIGNAL));

    private static final String APP_PARAMETER_OPEN_TOKEN = "\\{\\{";
    private static final String APP_PARAMETER_OPEN_TOKEN_ORIG = "{{";
    private static final String APP_PARAMETER_CLOSE_TOKEN = "}}";
    private static final String DUMMY_SEPARATOR = "<_<<>>_>";
    private static final String DUMMY_SPACE_REPLACE = "<___>";
    private static final String DUMMY_PYTHON_EMPTY_STRING = "3mPtY57r1Ng";

    private Process process;


    /**
     * Creates a BinaryRunner instance to execute the command.
     */
    public BinaryRunner() {
        this.process = null;
    }

    // STATIC API

    /**
     * Converts the values to the CMD standard and calculates with are the streamValues.
     *
     * @param parameters Binary parameters
     * @param target Binary target parameter
     * @param streamValues Binary stream values
     * @param pythonInterpreter Currently loaded python interpreter.
     * @return Binary execution Command as list of strings
     * @throws InvokeExecutionException Error creating command.
     */
    public static ArrayList<String> createCMDParametersFromValues(List<? extends InvocationParam> parameters,
        InvocationParam target, StdIOStream streamValues, String pythonInterpreter) throws InvokeExecutionException {

        ArrayList<String> binaryParams = new ArrayList<>();
        for (InvocationParam param : parameters) {
            binaryParams.addAll(processParam(param, streamValues, pythonInterpreter));
        }
        if (target != null) {
            binaryParams.addAll(processParam(target, streamValues, pythonInterpreter));
        }
        return binaryParams;
    }

    /**
     * Replaces parameters names with their values and return as a String.
     *
     * @param parameters Binary parameters
     * @param params parameters that should invoke
     * @param pythonInterpreter Currently loaded python interpreter.
     * @return formatted string where param names are replaces with the values.
     */
    public static String[] buildAppParams(List<? extends InvocationParam> parameters, String params,
        String pythonInterpreter) throws InvokeExecutionException {

        if (params == null || params.isEmpty()) {
            return new String[0];
        }

        StdIOStream streamValues = new StdIOStream();
        // mark spaces from the original 'params' string and don't mix them with spaces
        // occurring in parameter strings
        String paramsString = String.join(DUMMY_SPACE_REPLACE, params.split(" "));

        for (InvocationParam param : parameters) {
            String tmpName =
                APP_PARAMETER_OPEN_TOKEN_ORIG + param.getName().replaceFirst("#kwarg_", "") + APP_PARAMETER_CLOSE_TOKEN;
            if (!params.contains(tmpName)) {
                continue;
            }
            String replacement =
                APP_PARAMETER_OPEN_TOKEN + param.getName().replaceFirst("#kwarg_", "") + APP_PARAMETER_CLOSE_TOKEN;
            ArrayList<String> tmp = processParam(param, streamValues, pythonInterpreter);
            String value = String.join(DUMMY_SEPARATOR, tmp);
            paramsString = paramsString.replaceAll(replacement, value);
        }
        paramsString = String.join(DUMMY_SPACE_REPLACE, paramsString.split(DUMMY_SEPARATOR));
        return paramsString.split(DUMMY_SPACE_REPLACE);
    }

    /**
     * Replaces parameter names with their values in the working dir string.
     *
     * @param parameters Binary parameters
     * @param workingDir original working dir string
     * @return formatted string where param names are replaces with the values.
     * @throws InvokeExecutionException Exception processing parameters.
     */
    public static File getUpdatedWorkingDir(List<? extends InvocationParam> parameters, String workingDir)
        throws InvokeExecutionException {
        if (!(workingDir.contains(APP_PARAMETER_OPEN_TOKEN_ORIG) && workingDir.contains(APP_PARAMETER_CLOSE_TOKEN))) {
            return new File(workingDir);
        } else {
            return new File(String.join(" ", buildAppParams(parameters, workingDir, "python3")));
        }

    }

    // PRIVATE STATIC METHODS
    private static ArrayList<String> processParam(InvocationParam param, StdIOStream streamValues,
        String pythonInterpreter) throws InvokeExecutionException {

        ArrayList<String> binaryParamFields = new ArrayList<>();
        switch (param.getStdIOStream()) {
            case STDIN:
                streamValues.setStdIn((String) param.getValue());
                break;
            case STDOUT:
                streamValues.setStdOut((String) param.getValue());
                break;
            case STDERR:
                streamValues.setStdErr((String) param.getValue());
                break;
            case UNSPECIFIED:
                if (!param.getPrefix().equals(Constants.PREFIX_SKIP)) {
                    if (param.getValue() != null && param.getValue().getClass().isArray()) {
                        addArrayParam(param, binaryParamFields);
                    } else if (param.getValue() != null && param.getValue() instanceof Collection<?>) {
                        addCollectionParam(param, binaryParamFields);
                    } else {
                        // The value can be serialized directly
                        addDirectParam(param, binaryParamFields, pythonInterpreter);
                    }
                }
                break;
        }
        return binaryParamFields;
    }

    private static void addArrayParam(InvocationParam param, ArrayList<String> binaryParamFields)
        throws InvokeExecutionException {
        try {
            if (param.getPrefix() != null && !param.getPrefix().isEmpty()
                && !param.getPrefix().equals(Constants.PREFIX_EMPTY)) {
                binaryParamFields.add(param.getPrefix());
            }
            binaryParamFields.addAll(serializeArrayParam(param.getValue()));
        } catch (Exception e) {
            // Exception serializing to string the object
            throw new InvokeExecutionException(ERROR_PARAM_NOT_STRING, e);
        }
    }

    private static void addCollectionParam(InvocationParam param, ArrayList<String> binaryParamFields)
        throws InvokeExecutionException {

        try {
            if (param.getPrefix() != null && !param.getPrefix().isEmpty()
                && !param.getPrefix().equals(Constants.PREFIX_EMPTY)) {
                binaryParamFields.add(param.getPrefix());
            }
            binaryParamFields.addAll(serializeCollectionParam((Collection<?>) param.getValue()));
        } catch (Exception e) {
            // Exception serializing to string the object
            throw new InvokeExecutionException(ERROR_PARAM_NOT_STRING, e);
        }
    }

    private static void addDirectParam(InvocationParam param, ArrayList<String> binaryParamFields,
        String pythonInterpreter) throws InvokeExecutionException {

        if (param.getPrefix() != null && !param.getPrefix().isEmpty()
            && !param.getPrefix().equals(Constants.PREFIX_EMPTY)) {

            // Add parameters with prefix
            switch (param.getType()) {
                case FILE_T:
                    // Add prefix and file name
                    binaryParamFields.add(param.getPrefix() + param.getOriginalName());
                    break;
                case COLLECTION_T:
                    // TODO: Handle collections instead of passing the collection dXvY
                    binaryParamFields.add(param.getPrefix() + String.valueOf(param.getValue()));
                    break;
                case DICT_COLLECTION_T:
                    // TODO: Handle dictionary collections instead of passing the dictionary collection dXvY
                    binaryParamFields.add(param.getPrefix() + String.valueOf(param.getValue()));
                    break;
                case STREAM_T:
                    // No need to check prefix
                    DistroStream<?> ds = (DistroStream<?>) param.getValue();
                    switch (ds.getStreamType()) {
                        case FILE:
                            FileDistroStream fds = (FileDistroStream) ds;
                            binaryParamFields.add(param.getPrefix() + fds.getBaseDir());
                            break;
                        case OBJECT:
                            // For an ods we send its alias (can be null)
                            ObjectDistroStream<?> ods = (ObjectDistroStream<?>) ds;
                            binaryParamFields.add(param.getPrefix() + ods.getAlias());
                            break;
                        default:
                            throw new InvokeExecutionException(ERROR_STREAM);
                    }
                    break;
                case EXTERNAL_STREAM_T:
                    // No need
                    String serializedFile = (String) param.getValue();
                    String baseDir = null;
                    try {
                        baseDir = ExternalStreamHandler.getExternalStreamProperty(pythonInterpreter, serializedFile,
                            "base_dir");
                    } catch (ExternalPropertyException epe) {
                        throw new InvokeExecutionException(ERROR_EXT_STREAM_BASE_DIR, epe);
                    }
                    if (baseDir == null || baseDir.isEmpty()) {
                        throw new InvokeExecutionException(ERROR_EXT_STREAM_BASE_DIR);
                    }
                    binaryParamFields.add(param.getPrefix() + baseDir);
                    break;
                case STRING_64_T:
                    byte[] decodedBytes = Base64.getDecoder().decode(param.getValue().toString());
                    String tmp = param.getPrefix() + new String(decodedBytes);
                    binaryParamFields.add(tmp);
                    break;
                default:
                    String value = String.valueOf(param.getValue());
                    if (value.equals(DUMMY_PYTHON_EMPTY_STRING)) {
                        value = "";
                    }
                    binaryParamFields.add(param.getPrefix() + value);
                    break;
            }
        } else {
            // Add parameters without prefix
            switch (param.getType()) {
                case FILE_T:
                    // Add file name
                    try {
                        if (param.getContentType().equals("Future")) {
                            Serializer.Format[] priorities = new Serializer.Format[1];
                            priorities[0] = Serializer.Format.PYBINDING;
                            Object oVal = Serializer.deserialize(param.getValue().toString(), priorities);
                            if (oVal == null) {
                                throw new Exception(
                                    "Couldn't deserialize future object parameter with name" + param.getName());
                            }
                            binaryParamFields.add(oVal.toString());
                        } else {
                            binaryParamFields.add(param.getOriginalName());
                        }
                    } catch (Exception e) {
                        throw new InvokeExecutionException(ERROR_PARAM_NOT_STRING, e);
                    }
                    break;
                case COLLECTION_T:
                    // TODO: Handle collections instead of passing the collection dXvY
                    binaryParamFields.add(String.valueOf(param.getValue()));
                    break;
                case DICT_COLLECTION_T:
                    // TODO: Handle dictionary collections instead of passing the dictionary collection dXvY
                    binaryParamFields.add(String.valueOf(param.getValue()));
                    break;
                case STREAM_T:
                    // No need to check prefix
                    DistroStream<?> ds = (DistroStream<?>) param.getValue();
                    switch (ds.getStreamType()) {
                        case FILE:
                            // For an FDS we send the base dir path
                            FileDistroStream fds = (FileDistroStream) ds;
                            binaryParamFields.add(fds.getBaseDir());
                            break;
                        case OBJECT:
                            // For an ODS we send its alias (can be null)
                            ObjectDistroStream<?> ods = (ObjectDistroStream<?>) ds;
                            binaryParamFields.add(ods.getAlias());
                            break;
                        default:
                            throw new InvokeExecutionException(ERROR_STREAM);
                    }
                    break;
                case EXTERNAL_STREAM_T:
                    String serializedFile = (String) param.getValue();
                    String baseDir = null;
                    try {
                        baseDir = ExternalStreamHandler.getExternalStreamProperty(pythonInterpreter, serializedFile,
                            "base_dir");
                    } catch (ExternalPropertyException epe) {
                        throw new InvokeExecutionException(ERROR_EXT_STREAM_BASE_DIR, epe);
                    }
                    if (baseDir == null || baseDir.isEmpty()) {
                        throw new InvokeExecutionException(ERROR_EXT_STREAM_BASE_DIR);
                    }
                    binaryParamFields.add(baseDir);
                    break;
                case STRING_64_T:
                    byte[] decodedBytes = Base64.getDecoder().decode(param.getValue().toString());
                    String tmp = new String(decodedBytes);
                    // encoded strings have and extra character ('#') to avoid empty string errors
                    binaryParamFields.add(tmp.substring(1));
                    break;
                default:
                    String value = String.valueOf(param.getValue());
                    if (value.equals(DUMMY_PYTHON_EMPTY_STRING)) {
                        value = "";
                    }
                    binaryParamFields.add(value);
                    break;
            }
        }
    }

    private static ArrayList<String> serializeArrayParam(Object value) throws Exception {
        ArrayList<String> serializedValue = new ArrayList<String>();

        if (value instanceof int[]) {
            int[] arrayValues = (int[]) value;
            for (int paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof boolean[]) {
            boolean[] arrayValues = (boolean[]) value;
            for (boolean paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof byte[]) {
            byte[] arrayValues = (byte[]) value;
            for (byte paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof short[]) {
            short[] arrayValues = (short[]) value;
            for (short paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof char[]) {
            int[] arrayValues = (int[]) value;
            for (int paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof long[]) {
            long[] arrayValues = (long[]) value;
            for (long paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof float[]) {
            float[] arrayValues = (float[]) value;
            for (float paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof double[]) {
            double[] arrayValues = (double[]) value;
            for (double paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof Integer[]) {
            Integer[] arrayValues = (Integer[]) value;
            for (Integer paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof Boolean[]) {
            Boolean[] arrayValues = (Boolean[]) value;
            for (Boolean paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof Character[]) {
            Character[] arrayValues = (Character[]) value;
            for (Character paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof Byte[]) {
            Byte[] arrayValues = (Byte[]) value;
            for (Byte paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof Short[]) {
            Short[] arrayValues = (Short[]) value;
            for (Short paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof Long[]) {
            Long[] arrayValues = (Long[]) value;
            for (Long paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof Float[]) {
            Float[] arrayValues = (Float[]) value;
            for (Float paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof Double[]) {
            Double[] arrayValues = (Double[]) value;
            for (Double paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof String[]) {
            String[] arrayValues = (String[]) value;
            for (String paramValue : arrayValues) {
                serializedValue.add(String.valueOf(paramValue));
            }
        } else if (value instanceof Object[]) {
            Object[] arrayValues = (Object[]) value;
            for (Object paramValue : arrayValues) {
                serializedValue.addAll(serializeArrayParam(paramValue));
            }
        } else {
            serializedValue.add(String.valueOf(value));
        }

        return serializedValue;
    }

    private static ArrayList<String> serializeCollectionParam(Collection<?> value) throws Exception {
        ArrayList<String> serializedValue = new ArrayList<>();

        for (Iterator<?> iterator = value.iterator(); iterator.hasNext();) {
            serializedValue.add(String.valueOf(iterator.next()));
        }

        return serializedValue;
    }

    // OBJECT API

    /**
     * Executes a given command {@code cmd} with the stream redirections {@code streamValues}.
     *
     * @param cmd Command to execute.
     * @param stdIOStreamValues Stream values.
     * @param sandbox Execution sandbox.
     * @param outLog Execution output stream.
     * @param errLog Execution error stream.
     * @param pythonPath Execution PYTHONPATH.
     * @param failByEV Whether to fail by exit value or not.
     * @return Exit value as object.
     * @throws InvokeExecutionException Error execution the binary.
     */
    public Object executeCMD(String[] cmd, StdIOStream stdIOStreamValues, ExecutionSandbox sandbox, PrintStream outLog,
        PrintStream errLog, String pythonPath, boolean failByEV) throws InvokeExecutionException {

        // Retrieve COMPSs properties
        final String theoreticalHostnames = System.getProperty(Invoker.COMPSS_HOSTNAMES);
        final int theoreticalNumNodes = Integer.valueOf(System.getProperty(Invoker.COMPSS_NUM_NODES));
        final int theoreticalNumThreads = Integer.valueOf(System.getProperty(Invoker.COMPSS_NUM_THREADS));
        final int theoreticalNumProcs = Integer.valueOf(System.getProperty(Invoker.COMPSS_NUM_PROCS));

        // Re-compute real task properties
        final Map<String, Integer> hostnames2numThreads = new HashMap<>();
        for (String hostname : theoreticalHostnames.split(",")) {
            int nt;
            if (hostnames2numThreads.containsKey(hostname)) {
                nt = hostnames2numThreads.get(hostname) + 1;
            } else {
                nt = 1;
            }
            hostnames2numThreads.put(hostname, nt);
        }
        final int uniqueNumNodes = hostnames2numThreads.size();
        int maxNumProcsPerNode = Collections.max(hostnames2numThreads.values());
        // nm: make sure next line commented doesn't make it fail with MPMD..
        // maxNumProcsPerNode *= theoreticalNumProcs;
        outLog.println("[BINARY EXECUTION WRAPPER] CMD: setting maxNumProcsPerNode to: " + maxNumProcsPerNode);

        // Re-set COMPSs properties
        // We do not reset COMPSs properties because it does not have to match SLURM
        // System.setProperty(Invoker.COMPSS_NUM_NODES, String.valueOf(uniqueNumNodes));
        // System.setProperty(Invoker.COMPSS_NUM_THREADS, String.valueOf(maxNumThreads));
        // System.setProperty(Invoker.OMP_NUM_THREADS, String.valueOf(maxNumThreads));

        // Prepare process builder with command and working directory
        // String execCmd = "bash -c " + String.join(" ", cmd).trim();
        // String execCmd = "bash " + "-c '" + String.join(" ", cmd).trim() + "'";
        // outLog.println("[BINARY EXECUTION WRAPPER] EXEC CMD ------------------------------------ " + execCmd);
        StringBuilder command = new StringBuilder();
        for (String s : cmd) {
            if (s.contains("$")) {
                s = s.replaceAll("\\$", "\"\\\\\\$\"");
            }
            command.append(" ").append(s.replaceAll(" ", "\\\\ ").replaceAll("#", "\\\\#"));
        }
        // outLog.println("[BINARY EXECUTION WRAPPER] EXEC CMD ------------------------------------ " + command);

        String[] execCmd = { "bash",
            "-c",
            command.toString() };
        // ProcessBuilder builder = new ProcessBuilder(cmd);
        ProcessBuilder builder = new ProcessBuilder(execCmd);

        builder.directory(sandbox.getFolder());
        outLog.println("[BINARY EXECUTION WRAPPER] CMD " + cmd[0]);

        // Setup process environment -- Tracing entries
        Tracer.prepareEnvironment(builder.environment(), false);

        // Setup process environment -- COMPSs entries
        // WARN: THE COMPSS ENVIRONMENT DOES NOT HAVE TO MATCH SLURM CONFIGURATION
        builder.environment().put(Invoker.COMPSS_HOSTNAMES, theoreticalHostnames);
        builder.environment().put(Invoker.COMPSS_NUM_NODES, String.valueOf(theoreticalNumNodes));
        builder.environment().put(Invoker.COMPSS_NUM_THREADS, String.valueOf(theoreticalNumThreads));
        builder.environment().put(Invoker.OMP_NUM_THREADS, String.valueOf(theoreticalNumThreads));
        builder.environment().put(Invoker.COMPSS_BINDED_CPUS, System.getProperty(Invoker.COMPSS_BINDED_CPUS));
        builder.environment().put(Invoker.COMPSS_BINDED_GPUS, System.getProperty(Invoker.COMPSS_BINDED_GPUS));
        // Setup process environment -- Extra entries
        if (pythonPath != null) {
            builder.environment().put("PYTHONPATH", pythonPath);
            builder.environment().put("SINGULARITYENV_PYTHONPATH", pythonPath);
        }

        // Setup process environment -- SLURM entries (for elasticity with MPI in supercomputers)
        // WARN: WE ONLY RESET SLURM ENVIRONMENT BUT NOT COMPSS ENVIRONMENT
        // HOWEVER, INFORMATION IN COMPSS AND SLURM CAN BE INCONSISTENT
        final String tasksPerNode = String.valueOf(maxNumProcsPerNode) + "(x" + String.valueOf(uniqueNumNodes) + ")";
        final String hostnamesString = String.join(",", hostnames2numThreads.keySet());
        final int totalProcs = uniqueNumNodes * theoreticalNumProcs;

        builder.environment().put("SLURM_NODELIST", hostnamesString);
        builder.environment().put("SLURM_JOB_NODELIST", hostnamesString);
        builder.environment().put("SLURM_NNODES", String.valueOf(uniqueNumNodes));
        builder.environment().put("SLURM_JOB_NUM_NODES", String.valueOf(uniqueNumNodes));
        builder.environment().put("SLURM_JOB_CPUS_PER_NODE", tasksPerNode);
        builder.environment().put("SLURM_NTASKS", String.valueOf(totalProcs));
        builder.environment().put("SLURM_NPROCS", String.valueOf(totalProcs));
        builder.environment().put("SLURM_TASKS_PER_NODE", tasksPerNode);
        builder.environment().put("SLURM_OVERLAP", "1");
        builder.environment().put("SLURM_MEM_PER_NODE", "0");

        builder.environment().remove("SLURM_STEP_NODELIST");
        builder.environment().remove("SLURM_STEP_CPUS_PER_NODE");
        builder.environment().remove("SLURM_STEP_NUM_TASKS");
        builder.environment().remove("SLURM_STEP_TASKS_PER_NODE");
        builder.environment().remove("SLURM_STEPID");
        builder.environment().remove("SLURM_STEP_ID");
        builder.environment().remove("SLURM_STEP_NUM_NODES");
        builder.environment().remove("SLURM_STEP_LAUNCHER_PORT");
        builder.environment().remove("SLURM_STEP_RESV_PORTS");
        builder.environment().remove("SLURM_NODEID");
        builder.environment().remove("SLURM_LOCALID");
        builder.environment().remove("SLURM_GTIDS");
        builder.environment().remove("SLURM_CPU_BIND");
        builder.environment().remove("SLURM_CPU_BIND_LIST");
        builder.environment().remove("SLURM_CPU_BIND_TYPE");
        builder.environment().remove("SLURM_LAUNCH_NODE_IPADDR");
        builder.environment().remove("SLURM_SRUN_COMM_PORT");
        builder.environment().remove("SLURM_SRUN_COMM_HOST");
        builder.environment().remove("SLURM_TASK_PID");
        builder.environment().remove("SLURM_DISTRIBUTION");
        builder.environment().remove("SLURM_PROCID");
        builder.environment().remove("SLURM_TOPOLOGY_ADDR");
        builder.environment().remove("SLURM_TOPOLOGY_ADDR_PATTERN");
        builder.environment().remove("SLURM_PRIO_PROCESS");
        builder.environment().remove("SLURMD_NODENAME");
        builder.environment().remove("SLURM_MEM_PER_CPU");
        builder.environment().remove("SLURM_SUBMIT_HOST");

        // Log environment
        // outLog.println("PB ENVIRONMENT ---------------------------");
        // for (Entry<String, String> entry : builder.environment().entrySet()) {
        // outLog.println("-- " + entry.getKey() + " : " + entry.getValue());
        // }
        // outLog.println("PB ENVIRONMENT END -----------------------");

        // Setup STD redirections
        final String fileInPath = stdIOStreamValues.getStdIn();
        if (fileInPath != null) {
            builder.redirectInput(new File(fileInPath));
        }
        final String fileOutPath = stdIOStreamValues.getStdOut();
        if (fileOutPath != null) {
            builder.redirectOutput(Redirect.appendTo(new File(fileOutPath)));
        }
        final String fileErrPath = stdIOStreamValues.getStdErr();
        if (fileErrPath != null) {
            builder.redirectError(Redirect.appendTo(new File(fileErrPath)));
        }

        // Launch command
        int exitValue = -1;
        try {
            outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
            outLog.println("[BINARY EXECUTION WRAPPER] Executing binary command");
            this.process = builder.start();
            /*
             * outLog.println("[BINARY EXECUTION WRAPPER] USING RUNTIME.EXEC ------------------------------------");
             * String[] execCmd = new String[cmd.length + 2]; execCmd[0] = "bash"; execCmd[1] = "-c"; execCmd[2] =
             * String.join(" ") System.arraycopy(cmd, 0, execCmd, 2, cmd.length); this.process =
             * Runtime.getRuntime().exec(execCmd); this.process = Runtime.getRuntime().exec(execCmd);
             */

            // Disable inputs to process
            this.process.getOutputStream().close();

            // Log binary execution
            logBinaryExecution(this.process, fileOutPath, fileErrPath, outLog, errLog);

            // Wait and retrieve exit value
            exitValue = this.process.waitFor();

            // Print all process execution information
            outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
            outLog.println("[BINARY EXECUTION WRAPPER] CMD EXIT VALUE: " + exitValue);
            outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
        } catch (IOException | InvokeExecutionException | InterruptedException e) {
            errLog.println(ERROR_PROC_EXEC);
            throw new InvokeExecutionException(ERROR_PROC_EXEC, e);
        }

        if (failByEV && exitValue != 0) {
            throw new InvokeExecutionException(ERROR_EXIT_VALUE);
        }

        // Return exit value if requested, null if none
        return exitValue;
    }

    /**
     * Closes any stream parameter of the task.
     *
     * @param parameters Task parameters.
     * @param pythonInterpreter Currently loaded Python interpreter.
     * @throws StreamCloseException When an internal error occurs when closing the stream.
     */
    public void closeStreams(List<? extends InvocationParam> parameters, String pythonInterpreter)
        throws StreamCloseException {
        for (InvocationParam p : parameters) {
            if (p.isWriteFinalValue()) {
                switch (p.getType()) {
                    case STREAM_T:
                        // OUT Stream
                        closeStream(p);
                        break;
                    case EXTERNAL_STREAM_T:
                        // External OUT stream
                        closeExternalStream(p, pythonInterpreter);
                        break;
                    default:
                        // Nothing to do
                        break;
                }
            }
        }
    }

    /**
     * Cancels the running process.
     */
    public void cancelProcess() {

        if (this.process != null) {
            if (this.process.getClass().getName().equals("java.lang.UNIXProcess")) {
                try {
                    Field f = this.process.getClass().getDeclaredField("pid");
                    f.setAccessible(true);
                    int pid = f.getInt(this.process);
                    System.out.println("Killing process " + pid); // NOSONAR need to be printed in job out/err
                    Runtime.getRuntime().exec("kill -" + PB_KILL_SIGNAL + " " + pid);
                } catch (Throwable e) {
                    System.err.println("Error geting pid." + e.getMessage());
                }
            } else {
                this.process.destroy();
            }
        }
    }

    // PRIVATE OBJECT METHODS

    private void logBinaryExecution(Process process, String fileOutPath, String fileErrPath, PrintStream outLog,
        PrintStream errLog) throws InvokeExecutionException {

        StreamGobbler errorGobbler = null;
        StreamGobbler outputGobbler = null;
        outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
        outLog.println("[BINARY EXECUTION WRAPPER] CMD OUTPUT:");
        if (process != null) {
            if (fileOutPath == null) {
                outputGobbler =
                    new StreamGobbler(process.getInputStream(), outLog, LogManager.getLogger(Loggers.WORKER), false);
                outputGobbler.start();
            } else {
                try (InputStream outputStream = new FileInputStream(fileOutPath)) {
                    outputGobbler =
                        new StreamGobbler(outputStream, outLog, LogManager.getLogger(Loggers.WORKER), false);
                    outputGobbler.start();
                } catch (IOException ioe) {
                    errLog.println(ERROR_OUTPUTREADER);
                    ioe.printStackTrace(errLog);
                    throw new InvokeExecutionException(ERROR_OUTPUTREADER, ioe);
                }
            }

        }

        errLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
        errLog.println("[BINARY EXECUTION WRAPPER] CMD ERROR:");
        if (process != null) {
            if (fileErrPath == null) {
                errorGobbler =
                    new StreamGobbler(process.getErrorStream(), errLog, LogManager.getLogger(Loggers.WORKER), true);
                errorGobbler.start();
            } else {
                try (InputStream errStream = new FileInputStream(fileErrPath)) {
                    errorGobbler = new StreamGobbler(errStream, errLog, LogManager.getLogger(Loggers.WORKER), true);
                    errorGobbler.start();
                } catch (IOException ioe) {
                    throw new InvokeExecutionException(ERROR_ERRORREADER, ioe);
                }
            }
        }

        if (outputGobbler != null) {
            try {
                outputGobbler.join();
            } catch (InterruptedException e) {
                errLog.println("Error waiting for output gobbler to end");
                e.printStackTrace(errLog);
            }
        }
        outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
        if (errorGobbler != null) {
            try {
                errorGobbler.join();
            } catch (InterruptedException e) {
                errLog.println("Error waiting for error gobbler to end");
                e.printStackTrace(errLog);
            }
        }
        errLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
    }

    private void closeStream(InvocationParam p) {
        DistroStream<?> ds = (DistroStream<?>) p.getValue();
        ds.close();
    }

    private void closeExternalStream(InvocationParam p, String pythonInterpreter) throws StreamCloseException {
        // External OUT stream
        String serializedFile = p.getValue().toString();
        String streamId = null;
        try {
            streamId = ExternalStreamHandler.getExternalStreamProperty(pythonInterpreter, serializedFile, "id");
        } catch (ExternalPropertyException epe) {
            throw new StreamCloseException(ERROR_EXT_STREAM_GET_ID);
        }

        // Close stream
        if (streamId != null) {
            CloseStreamRequest req = new CloseStreamRequest(streamId);
            DistroStreamClient.request(req);

            req.waitProcessed();
            int error = req.getErrorCode();
            if (error != 0) {
                String internalError = "ERROR CODE = " + error + " ERROR MESSAGE = " + req.getErrorMessage();
                throw new StreamCloseException(ERROR_EXT_STREAM_CLOSURE + internalError);
            }
            // No need to process the answer message. Checking the error is enough.
        } else {
            throw new StreamCloseException(ERROR_EXT_STREAM_GET_ID);
        }
    }

}
