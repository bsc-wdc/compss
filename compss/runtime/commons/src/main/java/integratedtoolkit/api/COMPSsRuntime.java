package integratedtoolkit.api;

public interface COMPSsRuntime {

    // Parameter types
    public enum DataType {
        BOOLEAN_T, 
        CHAR_T, 
        BYTE_T, 
        SHORT_T, 
        INT_T, 
        LONG_T, 
        FLOAT_T, 
        DOUBLE_T, 
        STRING_T,   // Java: String , Bindings: String / small serialized object
        FILE_T,     // Java: File , Bindings: File / Object serialized
        OBJECT_T,   // Java: OBJ / SCO , Bindings: does not exist
        PSCO_T,     // Java: PSCOs
        EXTERNAL_PSCO_T  // Bindings: PSCOs
    }

    // Parameter directions
    public enum DataDirection {
        IN, 
        OUT,
        INOUT
    }


    /*
     * ************************************ 
     * START AND STOP METHODS
     ************************************/
    /**
     * Starts the COMPSs Runtime
     */
    public void startIT();

    /**
     * Stops the COMPSs Runtime
     * 
     * @param terminate
     */
    public void stopIT(boolean terminate);

    /*
     * ************************************ 
     * CONFIGURATION
     ************************************/
    /**
     * Returns the COMPSs Application Directory
     * 
     * @return
     */
    public String getApplicationDirectory();

    /**
     * Registers a new CoreElement in the Runtime
     * 
     * @param methodClass
     * @param methodName
     * @param hasTarget
     * @param hasReturn
     * @param constraints
     * @param parameterCount
     * @param parameters
     */
    public void registerCE(String methodClass, String methodName, boolean hasTarget, boolean hasReturn, String constraints,
            int parameterCount, Object... parameters);

    /*
     * ************************************ 
     * TASK METHODS
     ************************************/
    /**
     * New Method task
     * 
     * @param appId
     * @param methodClass
     * @param methodName
     * @param priority
     * @param hasTarget
     * @param parameterCount
     * @param parameters
     * @return
     */
    public int executeTask(Long appId, String methodClass, String methodName, boolean priority, boolean hasTarget, int parameterCount,
            Object... parameters);
    
    /**
     * New Method task (from loader)
     * 
     * @param appId
     * @param methodClass
     * @param methodName
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param hasTarget
     * @param parameterCount
     * @param parameters
     * @return
     */
    public int executeTask(Long appId, String methodClass, String methodName, boolean isPrioritary, int numNodes, boolean isReplicated, 
             boolean isDistributed, boolean hasTarget, int parameterCount, Object... parameters);

    /**
     * New Service task
     * 
     * @param appId
     * @param namespace
     * @param service
     * @param port
     * @param operation
     * @param priority
     * @param hasTarget
     * @param parameterCount
     * @param parameters
     * @return
     */
    public int executeTask(Long appId, String namespace, String service, String port, String operation, boolean priority, boolean hasTarget,
            int parameterCount, Object... parameters);
    
    /**
     * New service task (from loader) 
     * 
     * @param appId
     * @param namespace
     * @param service
     * @param port
     * @param operation
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param hasTarget
     * @param parameterCount
     * @param parameters
     * @return
     */
    public int executeTask(Long appId, String namespace, String service, String port, String operation, boolean isPrioritary, 
            int numNodes, boolean isReplicated, boolean isDistributed, boolean hasTarget, int parameterCount, Object... parameters);

    /**
     * Notifies the Runtime that there are no more tasks created by the current appId
     * 
     * @param appId
     * @param terminate
     */
    public void noMoreTasks(Long appId, boolean terminate);

    /**
     * Freezes the task generation until all previous tasks have been executed
     * 
     * @param appId
     */
    public void waitForAllTasks(Long appId);

    /*
     * ************************************ 
     * DATA ACCESS METHODS
     ************************************/
    /**
     * Returns the renaming of the file version opened
     * 
     * @param fileName
     * @param mode
     * @return
     */
    public String openFile(String fileName, DataDirection mode);

    /**
     * Deletes the specified version of a file
     * 
     * @param fileName
     * @return
     */
    public boolean deleteFile(String fileName);

    /*
     * ************************************ 
     * TOOLS ACCESS FOR BINDINGS
     ************************************/
    /**
     * Emits a tracing event
     * 
     * @param type
     * @param id
     */
    public void emitEvent(int type, long id);

}
