package integratedtoolkit.worker.invokers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;


public class BinaryRunner {

    private static final String ERROR_PARAM_NOT_STRING = "ERROR: Binary parameter cannot be serialized to string";
    private static final String ERROR_CLOSE_READER = "ERROR: Cannot close reader CMD output";
    private static final String ERROR_PROC_EXEC = "ERROR: Exception executing MPI command";


    public static ArrayList<String> createCMDParametersFromValues(Object[] values) throws InvokeExecutionException {
        ArrayList<String> binaryParams = new ArrayList<>();
        for (int i = 0; i < values.length; ++i) {
            if (values[i] != null && values[i].getClass().isArray()) {
                try {
                    binaryParams.addAll(serializeArrayParam(values[i]));
                } catch (Exception e) {
                    // Exception serializing to string the object
                    throw new InvokeExecutionException(ERROR_PARAM_NOT_STRING, e);
                }
            } else if (values[i] != null && values[i] instanceof Collection<?>) {
                try {
                    binaryParams.addAll(serializeCollectionParam((Collection<?>) values[i]));
                } catch (Exception e) {
                    // Exception serializing to string the object
                    throw new InvokeExecutionException(ERROR_PARAM_NOT_STRING, e);
                }
            } else {
                // The value can be serialized to string directly
                binaryParams.add(String.valueOf(values[i]));
            }
        }

        return binaryParams;
    }

    public static String executeCMD(String[] cmd) throws InvokeExecutionException {
        // Launch command
        BinaryExecutionResult result = new BinaryExecutionResult();
        Process p;
        BufferedReader reader = null;
        try {
            // Execute command
            p = Runtime.getRuntime().exec(cmd);

            // Wait for completion and retrieve exitValue
            int exitValue = p.waitFor();
            result.setExitValue(exitValue);

            // Store any process output
            StringBuffer output = new StringBuffer();
            reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";
            while ((line = reader.readLine()) != null) {
                output.append(line + "\n");
            }
            result.setOutputMessage(output.toString());

            // Store any process error
            StringBuffer error = new StringBuffer();
            reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            line = "";
            while ((line = reader.readLine()) != null) {
                error.append(line + "\n");
            }
            result.setErrorMessage(error.toString());
        } catch (Exception e) {
            throw new InvokeExecutionException(ERROR_PROC_EXEC, e);
        } finally {
            // Close reader
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new InvokeExecutionException(ERROR_CLOSE_READER, e);
                }
            }

            // Print all process execution information
            System.out.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
            System.out.println("[BINARY EXECUTION WRAPPER] CMD EXIT VALUE: " + result.getExitValue());

            System.out.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
            System.out.println("[BINARY EXECUTION WRAPPER] CMD OUTPUT:");
            System.out.println(result.getOutputMessage());

            System.err.println("[BINARY EXECUTION WRAPPER] ------------------------------------");
            System.err.println("[BINARY EXECUTION WRAPPER] CMD ERROR:");
            System.err.println(result.getErrorMessage());
        }

        // Return
        return result.getOutputMessage();
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


    private static class BinaryExecutionResult {

        private int exitValue;
        private String errorMessage;
        private String outputMessage;


        public BinaryExecutionResult() {
            this.exitValue = -1;
            this.errorMessage = "";
            this.outputMessage = "";
        }

        public int getExitValue() {
            return exitValue;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public String getOutputMessage() {
            return outputMessage;
        }

        public void setExitValue(int exitValue) {
            this.exitValue = exitValue;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public void setOutputMessage(String outputMessage) {
            this.outputMessage = outputMessage;
        }
    }

}
