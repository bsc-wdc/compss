package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.api.ITExecution;
import integratedtoolkit.nio.NIOParam;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.exceptions.JobExecutionException;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.NIOTracer;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Iterator;


public class JavaExecutor extends Executor {

    @Override
    String createSandBox() {
        //SandBox not supported
        return null;
    }

    @Override
    void executeTask(String sandBox, NIOTask nt, NIOWorker nw) throws Exception {
        /* Tracing related information */
        int taskType = nt.getTaskType() +1 ;  // +1 Because Task iD can't be 0 (0 signals end task)
        int taskId = nt.getTaskId();

        /* Task information */
        boolean debug = NIOWorker.workerDebug;
        String className = nt.getClassName();
        String methodName = nt.getMethodName();
        boolean hasTarget = nt.isHasTarget();
        int numParams = nt.getNumParams();
        
        /* Parameters information*/
        Class<?> types[];
        Object values[];
        if (hasTarget) {
            // The target object of the last parameter before the return value (if any)
            types = new Class[numParams - 1];
            values = new Object[numParams - 1];
        } else {
            types = new Class[numParams];
            values = new Object[numParams];
        }

        boolean isFile[] = new boolean[numParams];
        String renamings[] = new String[numParams];
        
        boolean writeFinalValue[] = new boolean[numParams];
        for (int i = 0; i < writeFinalValue.length; ++i) {
        	// Initialization to false because basic types aren't nor written nor preserved
        	writeFinalValue[i] = false;
        }
        
        /* Parse the parameter types and values */
        Object target = null;
        Iterator<NIOParam> params = nt.getParams().iterator();
        for (int i = 0; i < numParams; i++) {
            NIOParam np = params.next();
            
            // We need to use wrapper classes for basic types, reflection will unwrap automatically
            switch (np.getType()) {
                case FILE_T:
                    types[i] = String.class;
                    values[i] = np.getValue();
                    writeFinalValue[i] = np.isWriteFinalValue();
                    break;
                case OBJECT_T:
                    String renaming = renamings[i] = np.getValue().toString();
                    String name = renaming;
                    writeFinalValue[i] = np.isWriteFinalValue();
                    
                    Object o = nw.getObject(name);
                    if (hasTarget && i == numParams - 1) { // last parameter is the target object
                        if (o == null) {
                            throw new JobExecutionException("Target object with renaming " + name + ", method " + methodName + ", class " + className + " is null!" + "\n");
                        }
                        target = o;
                    } else {
                        if (o == null) {
                            throw new JobExecutionException("Object parameter " + i + " with renaming " + name + ", method " + methodName + ", class " + className + " is null!" + "\n");
                        }
                        types[i] = o.getClass();
                        values[i] = o;
                    }
                    break;
                case BOOLEAN_T:
                    types[i] = boolean.class;
                    values[i] = np.getValue();
                    break;
                case CHAR_T:
                    types[i] = char.class;
                    values[i] = np.getValue();
                    break;
                case STRING_T:
                    types[i] = String.class;
                    values[i] = np.getValue();
                    break;
                case BYTE_T:
                    types[i] = byte.class;
                    values[i] = np.getValue();
                    break;
                case SHORT_T:
                    types[i] = short.class;
                    values[i] = np.getValue();
                    break;
                case INT_T:
                    types[i] = int.class;
                    values[i] = np.getValue();
                    break;
                case LONG_T:
                    types[i] = long.class;
                    values[i] = np.getValue();
                    break;
                case FLOAT_T:
                    types[i] = float.class;
                    values[i] = np.getValue();
                    break;
                case DOUBLE_T:
                    types[i] = double.class;
                    values[i] = np.getValue();
                    break;
            }
            
            isFile[i] = (np.getType().equals(ITExecution.ParamType.FILE_T));
        }

        
        /* DEBUG information */
        if (debug) {
            // Print request information
            System.out.println("WORKER - Parameters of execution:");
            System.out.println("  * Method class: " + className);
            System.out.println("  * Method name: " + methodName);
            System.out.print("  * Parameter types:");
            for (int i = 0; i < types.length; i++) {
                System.out.print(" " + types[i].getName());

            }
            System.out.println();
            System.out.print("  * Parameter values:");
            for (Object v : values) {
                System.out.print(" " + v);
            }
            System.out.println();
        }

        /* Use reflection to get the requested method */
        Class<?> methodClass = null;
        Method method = null;
        try {
            methodClass = Class.forName(className);
        } catch (Exception e) {
            throw new JobExecutionException("Can not get class by reflection", e);
        }
        try {
            method = methodClass.getMethod(methodName, types);
        } catch (Exception e) {
            throw new JobExecutionException("Can not get method by reflection", e);
        }
        
        // TRACING: Emit start task       
        if (tracing) {
            NIOTracer.emitEventAndCounters(taskType, NIOTracer.getTaskEventsType());
            NIOTracer.emitEvent(taskId, NIOTracer.getTaskSchedulingType());
        }

        /* Invoke the requested method */
        Object retValue = null;  
        retValue = method.invoke(target, values);

        // TRACING: Emit end task 
        if (tracing) {
            NIOTracer.emitEventAndCounters(NIOTracer.EVENT_END, NIOTracer.getTaskEventsType());
            NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getTaskSchedulingType());
        }

        
        //Check if all the output files have been actually created (in case user has forgotten)
        //No need to distinguish between IN or OUT files, because IN files will exist, and if there's one or more missing,
        //they will be necessarily out.
        boolean allOutFilesCreated = true;
        for (int i = 0; i < numParams; i++) {
        	if(isFile[i]) {
        		String filepath = (String) values[i];
        		File f = new File(filepath);
        		if(!f.exists()) {
        			String errMsg = "ERROR: File with path '" + values[i] + 
								    "' has not been generated by task '" + nt.getMethodName() + "'" + 
		        					"' (in class '" + nt.getClassName() + "' at method '" + 
        							nt.getMethodName() + "', parameter number: " + (i+1) + " )";
        			System.out.println(errMsg);
        			System.err.println(errMsg);
        			allOutFilesCreated = false;
        		}
        	}
        }
        if(!allOutFilesCreated) {
        	throw new JobExecutionException("ERROR: One or more OUT files have not been created by task '" + nt.getMethodName() + "'");
        }
        
        // Write to disk the updated object parameters, if any (including the target)
        for (int i = 0; i < numParams; i++) {
            if (writeFinalValue[i]) {
            	// The parameter is a file or an object that MUST be stored
            	Object res = (hasTarget && i == numParams - 1) ? target : values[i];
            	nw.storeInCache(renamings[i], res);
            }
        }

        // Serialize the return value if existing
        if (retValue != null) {
            String renaming = (String) nt.getParams().getLast().getValue();
            // Always stored because it can only be a OUT object
            nw.storeInCache(renaming.substring(renaming.lastIndexOf('/') + 1), retValue);
        }

    }

    @Override
    void removeSandBox(String sandBox) {
        //SandBox not supported
    }

}
