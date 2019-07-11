/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.exceptions.ExternalPropertyException;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.util.ExternalStreamHandler;
import es.bsc.compss.util.StreamGobbler;
import es.bsc.compss.util.Tracer;
import es.bsc.distrostreamlib.DistroStream;
import es.bsc.distrostreamlib.api.files.FileDistroStream;
import es.bsc.distrostreamlib.client.DistroStreamClient;
import es.bsc.distrostreamlib.requests.CloseStreamRequest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;


public class BinaryRunner {

    private static final String ERROR_PARAM_NOT_STRING = "ERROR: Binary parameter cannot be serialized to string";
    private static final String ERROR_STREAM = "ERROR: Object and PSCO streams are not supported in non-native tasks";

    private static final String ERROR_OUTPUTREADER = "ERROR: Cannot retrieve command output";
    private static final String ERROR_ERRORREADER = "ERROR: Cannot retrieve command error";
    private static final String ERROR_PROC_EXEC = "ERROR: Exception executing Binary command";

    private static final String ERROR_EXT_STREAM_BASE_DIR = "ERROR: Cannot retrieve base_dir from External Stream";
    private static final String ERROR_EXT_STREAM_CLOSURE = "ERROR: Cannot close External Stream due to internal error.";
    private static final String ERROR_EXT_STREAM_GET_ID = "ERROR: Cannot close External Stream due to innvalid Id";


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
            InvocationParam target, StdIOStream streamValues, String pythonInterpreter)
            throws InvokeExecutionException {

        ArrayList<String> binaryParams = new ArrayList<>();
        for (InvocationParam param : parameters) {
            binaryParams.addAll(processParam(param, streamValues, pythonInterpreter));
        }
        if (target != null) {
            binaryParams.addAll(processParam(target, streamValues, pythonInterpreter));
        }
        return binaryParams;
    }

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
                case STREAM_T:
                    // No need to check prefix
                    DistroStream<?> ds = (DistroStream<?>) param.getValue();
                    switch (ds.getStreamType()) {
                        case FILE:
                            FileDistroStream fds = (FileDistroStream) ds;
                            binaryParamFields.add(param.getPrefix() + fds.getBaseDir());
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
                default:
                    binaryParamFields.add(param.getPrefix() + String.valueOf(param.getValue()));
                    break;
            }
        } else {
            // Add parameters without prefix
            switch (param.getType()) {
                case FILE_T:
                    // Add file name
                    binaryParamFields.add(param.getOriginalName());
                    break;
                case STREAM_T:
                    // No need to check prefix
                    DistroStream<?> ds = (DistroStream<?>) param.getValue();
                    switch (ds.getStreamType()) {
                        case FILE:
                            FileDistroStream fds = (FileDistroStream) ds;
                            binaryParamFields.add(fds.getBaseDir());
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
                    binaryParamFields.add(baseDir);
                    break;
                default:
                    binaryParamFields.add(String.valueOf(param.getValue()));
                    break;
            }
        }
    }

    /**
     * Executes a given command {@code cmd} with the stream redirections {@code streamValues}.
     *
     * @param cmd Command to execute.
     * @param stdIOStreamValues Stream values.
     * @param taskSandboxWorkingDir Execution sandbox.
     * @param outLog Execution output stream.
     * @param errLog Execution error stream.
     * @return Exit value as object.
     * @throws InvokeExecutionException Error execution the binary.
     */
    public static Object executeCMD(String[] cmd, StdIOStream stdIOStreamValues, File taskSandboxWorkingDir,
            PrintStream outLog, PrintStream errLog, String pythonPath) throws InvokeExecutionException {

        // Prepare command working dir, environment and STD redirections
        ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.directory(taskSandboxWorkingDir);

        builder.environment().remove(Tracer.LD_PRELOAD);
        builder.environment().put(Invoker.COMPSS_HOSTNAMES, System.getProperty(Invoker.COMPSS_HOSTNAMES));
        builder.environment().put(Invoker.COMPSS_NUM_NODES, System.getProperty(Invoker.COMPSS_NUM_NODES));
        builder.environment().put(Invoker.COMPSS_NUM_THREADS, System.getProperty(Invoker.COMPSS_NUM_THREADS));
        builder.environment().put(Invoker.OMP_NUM_THREADS, System.getProperty(Invoker.OMP_NUM_THREADS));
        builder.environment().put("PYTHONPATH", pythonPath);

        String fileInPath = stdIOStreamValues.getStdIn();
        if (fileInPath != null) {
            builder.redirectInput(new File(fileInPath));
        }
        String fileOutPath = stdIOStreamValues.getStdOut();
        if (fileOutPath != null) {
            builder.redirectOutput(Redirect.appendTo(new File(fileOutPath)));
        }
        String fileErrPath = stdIOStreamValues.getStdErr();
        if (fileErrPath != null) {
            builder.redirectError(Redirect.appendTo(new File(fileErrPath)));
        }

        // Launch command
        Process process = null;
        int exitValue = -1;
        try {
            outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
            outLog.println("[BINARY EXECUTION WRAPPER] Executing binary command");
            process = builder.start();

            // Disable inputs to process
            process.getOutputStream().close();

            // Log binary execution
            logBinaryExecution(process, fileOutPath, fileErrPath, outLog, errLog);

            // Wait and retrieve exit value
            exitValue = process.waitFor();

            // Print all process execution information
            outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
            outLog.println("[BINARY EXECUTION WRAPPER] CMD EXIT VALUE: " + exitValue);
            outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
        } catch (IOException | InvokeExecutionException | InterruptedException e) {
            errLog.println(ERROR_PROC_EXEC);
            throw new InvokeExecutionException(ERROR_PROC_EXEC, e);
        }

        // Return exit value if requested, null if none
        return exitValue;
    }

    /**
     * Executes a given command {@code cmd} with the stream redirections {@code streamValues}.
     *
     * @param cmd Command to execute.
     * @param stdIOStreamValues Stream values.
     * @param taskSandboxWorkingDir Execution sandbox.
     * @param outLog Execution output stream.
     * @param errLog Execution error stream.
     * @return Exit value as object.
     * @throws InvokeExecutionException Error execution the binary.
     */
    public static Object executeCMD(String[] cmd, StdIOStream stdIOStreamValues, File taskSandboxWorkingDir,
            PrintStream outLog, PrintStream errLog) throws InvokeExecutionException {

        // Prepare command working dir, environment and STD redirections
        ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.directory(taskSandboxWorkingDir);

        builder.environment().remove(Tracer.LD_PRELOAD);
        builder.environment().put(Invoker.COMPSS_HOSTNAMES, System.getProperty(Invoker.COMPSS_HOSTNAMES));
        builder.environment().put(Invoker.COMPSS_NUM_NODES, System.getProperty(Invoker.COMPSS_NUM_NODES));
        builder.environment().put(Invoker.COMPSS_NUM_THREADS, System.getProperty(Invoker.COMPSS_NUM_THREADS));
        builder.environment().put(Invoker.OMP_NUM_THREADS, System.getProperty(Invoker.OMP_NUM_THREADS));

        String fileInPath = stdIOStreamValues.getStdIn();
        if (fileInPath != null) {
            builder.redirectInput(new File(fileInPath));
        }
        String fileOutPath = stdIOStreamValues.getStdOut();
        if (fileOutPath != null) {
            builder.redirectOutput(Redirect.appendTo(new File(fileOutPath)));
        }
        String fileErrPath = stdIOStreamValues.getStdErr();
        if (fileErrPath != null) {
            builder.redirectError(Redirect.appendTo(new File(fileErrPath)));
        }

        // Launch command
        Process process = null;
        int exitValue = -1;
        try {
            outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
            outLog.println("[BINARY EXECUTION WRAPPER] Executing binary command");
            process = builder.start();

            // Disable inputs to process
            process.getOutputStream().close();

            // Log binary execution
            logBinaryExecution(process, fileOutPath, fileErrPath, outLog, errLog);

            // Wait and retrieve exit value
            exitValue = process.waitFor();

            // Print all process execution information
            outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
            outLog.println("[BINARY EXECUTION WRAPPER] CMD EXIT VALUE: " + exitValue);
            outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
        } catch (IOException | InvokeExecutionException | InterruptedException e) {
            errLog.println(ERROR_PROC_EXEC);
            throw new InvokeExecutionException(ERROR_PROC_EXEC, e);
        }

        // Return exit value if requested, null if none
        return exitValue;
    }

    private static void logBinaryExecution(Process process, String fileOutPath, String fileErrPath, PrintStream outLog,
            PrintStream errLog) throws InvokeExecutionException {

        StreamGobbler errorGobbler = null;
        StreamGobbler outputGobbler = null;
        outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
        outLog.println("[BINARY EXECUTION WRAPPER] CMD OUTPUT:");
        if (process != null) {
            if (fileOutPath == null) {
                outputGobbler = new StreamGobbler(process.getInputStream(), outLog,
                        LogManager.getLogger(Loggers.WORKER));
                outputGobbler.start();
            } else {
                try (FileInputStream outputStream = new FileInputStream(fileOutPath)) {
                    outputGobbler = new StreamGobbler(outputStream, outLog, LogManager.getLogger(Loggers.WORKER));
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
                errorGobbler = new StreamGobbler(process.getErrorStream(), errLog,
                        LogManager.getLogger(Loggers.WORKER));
                errorGobbler.start();
            } else {
                try (FileInputStream errStream = new FileInputStream(fileErrPath)) {
                    errorGobbler = new StreamGobbler(errStream, errLog, LogManager.getLogger(Loggers.WORKER));
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
                e.printStackTrace();
            }
        }
        outLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
        if (errorGobbler != null) {
            try {
                errorGobbler.join();
            } catch (InterruptedException e) {
                errLog.println("Error waiting for error gobbler to end");
                e.printStackTrace();
            }
        }
        errLog.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
    }

    /**
     * Closes any stream parameter of the task.
     * 
     * @param parameters Task parameters.
     * @param pythonInterpreter Currently loaded Python interpreter.
     * @throws StreamCloseException When an internal error occurs when closing the stream.
     */
    public static void closeStreams(List<? extends InvocationParam> parameters, String pythonInterpreter)
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

    private static void closeStream(InvocationParam p) {
        DistroStream<?> ds = (DistroStream<?>) p.getValue();
        ds.close();
    }

    private static void closeExternalStream(InvocationParam p, String pythonInterpreter) throws StreamCloseException {
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

}
