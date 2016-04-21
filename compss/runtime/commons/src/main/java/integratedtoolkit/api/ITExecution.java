package integratedtoolkit.api;


public interface ITExecution {

    // Parameter types
    public enum ParamType {

        FILE_T,
        BOOLEAN_T,
        CHAR_T,
        STRING_T,
        BYTE_T,
        SHORT_T,
        INT_T,
        LONG_T,
        FLOAT_T,
        DOUBLE_T,
        OBJECT_T,
        PSCO_T,
        SCO_T;
    }

    // Parameter directions
    public enum ParamDirection {

        IN,
        OUT,
        INOUT;
    }
    
    public void registerCE(String methodClass,
            String methodName,
            boolean hasTarget,
            boolean hasReturn,
            String constraints,
            int parameterCount,
            Object... parameters);

    // Method
    public int executeTask(Long appId,
            String methodClass,
            String methodName,
            boolean priority,
            boolean hasTarget,
            int parameterCount,
            Object... parameters);

    // Service
    public int executeTask(Long appId,
            String namespace,
            String service,
            String port,
            String operation,
            boolean priority,
            boolean hasTarget,
            int parameterCount,
            Object... parameters);

    public void noMoreTasks(Long appId, boolean terminate);

}
