package integratedtoolkit.worker.invokers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import integratedtoolkit.exceptions.InvokeExecutionException;
import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.parameter.Stream;
import integratedtoolkit.util.Tracer;


public class BinaryRunner {

    private static final String ERROR_PARAM_NOT_STRING = "ERROR: Binary parameter cannot be serialized to string";
    private static final String ERROR_OUTPUTREADER = "ERROR: Cannot retrieve command output";
    private static final String ERROR_ERRORREADER = "ERROR: Cannot retrieve command error";
    private static final String ERROR_PROC_EXEC = "ERROR: Exception executing Binary command";


    /**
     * Converts the values to the cmd standard and calculates with are the streamValues
     * 
     * @param values
     * @param paramStreams
     * @param streamValues
     * @return
     * @throws InvokeExecutionException
     */
    public static ArrayList<String> createCMDParametersFromValues(Object[] values, Stream[] paramStreams, String[] prefixes,
            StreamSTD streamValues) throws InvokeExecutionException {

        ArrayList<String> binaryParams = new ArrayList<>();
        for (int i = 0; i < values.length; ++i) {
            switch (paramStreams[i]) {
                case STDIN:
                    streamValues.setStdIn((String) values[i]);
                    break;
                case STDOUT:
                    streamValues.setStdOut((String) values[i]);
                    break;
                case STDERR:
                    streamValues.setStdErr((String) values[i]);
                    break;
                case UNSPECIFIED:
                    if (!prefixes[i].equals(Constants.PREFIX_SKIP)) {
                        if (values[i] != null && values[i].getClass().isArray()) {
                            try {
                                if (prefixes[i] != null && !prefixes[i].isEmpty() && !prefixes[i].equals(Constants.PREFIX_EMTPY)) {
                                    binaryParams.add(prefixes[i]);
                                }
                                binaryParams.addAll(serializeArrayParam(values[i]));
                            } catch (Exception e) {
                                // Exception serializing to string the object
                                throw new InvokeExecutionException(ERROR_PARAM_NOT_STRING, e);
                            }
                        } else if (values[i] != null && values[i] instanceof Collection<?>) {
                            try {
                                if (prefixes[i] != null && !prefixes[i].isEmpty() && !prefixes[i].equals(Constants.PREFIX_EMTPY)) {
                                    binaryParams.add(prefixes[i]);
                                }
                                binaryParams.addAll(serializeCollectionParam((Collection<?>) values[i]));
                            } catch (Exception e) {
                                // Exception serializing to string the object
                                throw new InvokeExecutionException(ERROR_PARAM_NOT_STRING, e);
                            }
                        } else {
                            // The value can be serialized to string directly
                            if (prefixes[i] != null && !prefixes[i].isEmpty() && !prefixes[i].equals(Constants.PREFIX_EMTPY)) {
                                binaryParams.add(prefixes[i] + String.valueOf(values[i]));
                            } else {
                                binaryParams.add(String.valueOf(values[i]));
                            }
                        }
                    }
                    break;
            }
        }

        return binaryParams;
    }

    /**
     * Executes a given command @cmd with the stream redirections @streamValues
     * 
     * @param cmd
     * @param hasReturn
     * @param streamValues
     * @return
     * @throws InvokeExecutionException
     */
    public static Object executeCMD(String[] cmd, boolean hasReturn, StreamSTD streamValues, File taskSandboxWorkingDir)
            throws InvokeExecutionException {

        // Prepare command execution with redirections
        ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.directory(taskSandboxWorkingDir);
        builder.environment().remove(Tracer.LD_PRELOAD);

        String fileInPath = streamValues.getStdIn();
        if (fileInPath != null) {
            builder.redirectInput(new File(fileInPath));
        }
        String fileOutPath = streamValues.getStdOut();
        if (fileOutPath != null) {
            builder.redirectOutput(Redirect.appendTo(new File(fileOutPath)));
        }
        String fileErrPath = streamValues.getStdErr();
        if (fileErrPath != null) {
            builder.redirectError(Redirect.appendTo(new File(fileErrPath)));
        }

        // Launch command
        Process process = null;
        int exitValue = -1;
        try {
            System.out.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
            System.out.println("[BINARY EXECUTION WRAPPER] Executing binary command");
            process = builder.start();
            exitValue = process.waitFor();
        } catch (Exception e) {
            throw new InvokeExecutionException(ERROR_PROC_EXEC, e);
        } finally {
            // Log binary execution
            logBinaryExecution(process, exitValue, fileOutPath, fileErrPath);
        }

        // Return exit value if requested, null if none
        return hasReturn ? exitValue : null;
    }

    private static void logBinaryExecution(Process process, int exitValue, String fileOutPath, String fileErrPath)
            throws InvokeExecutionException {

        // Print all process execution information
        System.out.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
        System.out.println("[BINARY EXECUTION WRAPPER] CMD EXIT VALUE: " + exitValue);

        System.out.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
        System.out.println("[BINARY EXECUTION WRAPPER] CMD OUTPUT:");
        if (process != null) {
            try (BufferedReader outputReader = (fileOutPath != null) ? new BufferedReader(new FileReader(fileOutPath))
                    : new BufferedReader(new InputStreamReader(process.getInputStream()));) {

                String line = null;
                while ((line = outputReader.readLine()) != null) {
                    System.out.println(line);
                }
            } catch (IOException ioe) {
                throw new InvokeExecutionException(ERROR_OUTPUTREADER, ioe);
            }
        }
        System.out.println("[BINARY EXECUTION WRAPPER] ------------------------------------");

        System.err.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
        System.err.println("[BINARY EXECUTION WRAPPER] CMD ERROR:");
        if (process != null) {
            try (BufferedReader errorReader = (fileErrPath != null) ? new BufferedReader(new FileReader(fileErrPath))
                    : new BufferedReader(new InputStreamReader(process.getErrorStream()));) {

                String line = null;
                while ((line = errorReader.readLine()) != null) {
                    System.err.println(line);
                }
            } catch (IOException ioe) {
                throw new InvokeExecutionException(ERROR_ERRORREADER, ioe);
            }
        }
        System.err.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
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
        ArrayList<String> serializedValue = new ArrayList<String>();

        for (Iterator<?> iterator = value.iterator(); iterator.hasNext();) {
            serializedValue.add(String.valueOf(iterator.next()));
        }

        return serializedValue;
    }


    public static class StreamSTD {

        private String stdIn = null;
        private String stdOut = null;
        private String stdErr = null;


        public StreamSTD() {
            // Nothing to do since all attributes have been initialized
        }

        public String getStdIn() {
            return stdIn;
        }

        public String getStdOut() {
            return stdOut;
        }

        public String getStdErr() {
            return stdErr;
        }

        public void setStdIn(String stdIn) {
            this.stdIn = stdIn;
        }

        public void setStdOut(String stdOut) {
            this.stdOut = stdOut;
        }

        public void setStdErr(String stdErr) {
            this.stdErr = stdErr;
        }

    }

}
