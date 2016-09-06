package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.ITConstants;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.nio.NIOParam;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.exceptions.JobExecutionException;
import integratedtoolkit.nio.exceptions.SerializedObjectException;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.util.RequestQueue;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.concurrent.Semaphore;

import storage.CallbackEvent;
import storage.CallbackHandler;
import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


public class JavaExecutor extends Executor {

	private static final String ERROR_CLASS_REFLECTION = "Cannot get class by reflection";
	private static final String ERROR_METHOD_REFLECTION = "Cannot get method by reflection";
	private static final String ERROR_TASK_EXECUTION = "ERROR: Exception executing task (user code)";
	private static final String ERROR_CALLBACK_INTERRUPTED = "ERROR: External callback interrupted";
	private static final String ERROR_SERIALIZED_OBJ = "ERROR: Cannot obtain object";
	private static final String ERROR_PERSISTENT_OBJ = "ERROR: Cannot getById persistent object";
	private static final String ERROR_STORAGE_CALL = "ERROR: External executeTask call failed";
	private static final String ERROR_OUT_FILES = "ERROR: One or more OUT files have not been created by task '";
	private static final String ERROR_EXTERNAL_EXECUTION = "ERROR: External Task Execution failed";
	private static final String ERROR_EXTERNAL_NO_PSCO = "ERROR: External ExecuteTask can only be used with target PSCOs";
	private static final String WARN_RET_VALUE_EXCEPTION = "WARN: Exception on externalExecution return value";

	private final boolean debug;


	public JavaExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue) {
		super(nw, pool, queue);

		this.debug = NIOWorker.isWorkerDebugEnabled;
	}

	@Override
	public void setEnvironmentVariables(String hostnames, int numNodes, int cus, MethodResourceDescription reqs){
		System.setProperty(Constants.COMPSS_HOSTNAMES, hostnames);
		System.setProperty(Constants.COMPSS_NUM_NODES, String.valueOf(numNodes));
		System.setProperty(Constants.COMPSS_NUM_THREADS, String.valueOf(cus));		
		
	}

	@Override
	public void executeTask(NIOWorker nw, NIOTask nt, String outputsBasename) throws JobExecutionException {
		/* Register outputs **************************************** */
		NIOWorker.registerOutputs(outputsBasename);

		/* TRY TO PROCESS THE TASK ******************************** */
		System.out.println("[JAVA EXECUTOR] executeTask - Begin task execution");
		try {
			processTask(nw, nt);
		} catch (JobExecutionException jee) {
			System.out.println("[JAVA EXECUTOR] executeTask - Error in task execution");
			System.err.println("[JAVA EXECUTOR] executeTask - Error in task execution");
			jee.printStackTrace();
			throw jee;
		} finally {
			System.out.println("[JAVA EXECUTOR] executeTask - End task execution");
			NIOWorker.unregisterOutputs();
		}
	}

	@Override
	public void finish() {
		// Nothing to do since everything is deleted in each task execution
		logger.info("Executor finished");
	}

	private void processTask(NIOWorker nw, NIOTask nt) throws JobExecutionException {
		/* Tracing related information ***************************** */
		int taskType = nt.getTaskType() + 1; // +1 Because Task ID can't be 0 (0 signals end task)
		int taskId = nt.getTaskId();

		/* Task information **************************************** */
		String className = nt.getClassName();
		String methodName = nt.getMethodName();
		boolean hasTarget = nt.isHasTarget();
		int numParams = nt.getNumParams();

		/* Parameters information ********************************** */
		int totalNumberOfParams = hasTarget ? numParams - 1 : numParams; // Don't count target if needed (i.e. obj.func())
		Class<?>[] types = new Class[totalNumberOfParams];
		Object[] values = new Object[totalNumberOfParams];
		String[] renamings = new String[numParams];
		boolean[] isFile = new boolean[numParams];
		boolean[] canBePSCO = new boolean[numParams];
		boolean[] writeFinalValue = new boolean[numParams]; // By default the boolean initializer is in false
															// False because basic types aren't nor written nor
															// preserved
		TargetParam target = new TargetParam();

		/* Parse the parameters ************************************ */
		Iterator<NIOParam> params = nt.getParams().iterator();
		for (int i = 0; i < numParams; i++) {
			NIOParam np = params.next();
			processParameter(np, i, nw, className, methodName, numParams, hasTarget, target, types, values, renamings, isFile, canBePSCO,
					writeFinalValue);
		}

		/* DEBUG information *************************************** */
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

		/* Use reflection to get the requested method *************** */
		Method method = getMethod(className, methodName, types);

		/* Invoke the requested method ****************************** */
		// TRACING: Emit start task
		if (NIOTracer.isActivated()) {
			NIOTracer.emitEventAndCounters(taskType, NIOTracer.getTaskEventsType());
			NIOTracer.emitEvent(taskId, NIOTracer.getTaskSchedulingType());
		}
		Object retValue = null;
		try {
			retValue = invokeMethod(nw, method, target, values);
		} catch (JobExecutionException jee) {
			throw jee;
		} finally {
			// TRACING: Emit end task
			if (NIOTracer.isActivated()){
				NIOTracer.emitEventAndCounters(NIOTracer.EVENT_END, NIOTracer.getTaskEventsType());
				NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getTaskSchedulingType());
			 }
		}

		/* Check SCO persistence for return and target ************** */
		checkSCOPersistence(nw, nt, numParams, hasTarget, target, retValue, renamings, values, canBePSCO, writeFinalValue);

		/* Check files existence ************************************ */
		checkJobFiles(className, methodName, numParams, values, isFile);

		/* Write to disk the updated values ************************* */
		writeUpdatedParameters(nw, nt, numParams, hasTarget, target, retValue, renamings, values, writeFinalValue);
	}

	private void processParameter(NIOParam np, int i, NIOWorker nw, String className, String methodName, int numParams, boolean hasTarget,
			TargetParam target, Class<?>[] types, Object[] values, String[] renamings, boolean[] isFile, boolean[] canBePSCO,
			boolean[] writeFinalValue) throws JobExecutionException {

		// We need to use wrapper classes for basic types, reflection will unwrap automatically
		switch (np.getType()) {
			case BOOLEAN_T:
				types[i] = boolean.class;
				values[i] = np.getValue();
				break;
			case CHAR_T:
				types[i] = char.class;
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
			case STRING_T:
				types[i] = String.class;
				values[i] = np.getValue();
				break;
			case FILE_T:
				types[i] = String.class;
				values[i] = np.getValue();
				writeFinalValue[i] = np.isWriteFinalValue();
				break;
			case OBJECT_T:
				renamings[i] = np.getValue().toString();
				writeFinalValue[i] = np.isWriteFinalValue();

				// Get object
				Object obj;
				try {
					obj = nw.getObject(renamings[i]);
				} catch (SerializedObjectException soe) {
					throw new JobExecutionException(ERROR_SERIALIZED_OBJ, soe);
				}
				
				// Check if object is null
				if (obj == null) {
					// Try if renaming refers to a PSCOId that is not catched
					// This happens when 2 tasks have an INOUT PSCO that is persisted within the 1st task
					try {
						obj = nw.getPersistentObject(renamings[i]);
					} catch (StorageException se) {
						throw new JobExecutionException(ERROR_SERIALIZED_OBJ, se);
					}
				}
				
				// Check if object is still null
				if (obj == null) {
					StringBuilder sb = new StringBuilder();
					if (hasTarget && i == numParams - 1) {
						sb.append("Target object with renaming ");
					} else {
						sb.append("Object parameter ").append(i).append(" with renaming ");
					}
					sb.append(renamings[i]);
					sb.append(", method ").append(methodName);
					sb.append(", class ").append(className);
					sb.append(" is null!").append("\n");
					
					throw new JobExecutionException(sb.toString());
				}

				// Store information as target or as normal parameter
				if (hasTarget && i == numParams - 1) {
					// Last parameter is the target object
					target.setValue(obj);
				} else {
					// Any other parameter
					types[i] = obj.getClass();
					values[i] = obj;
				}
				break;
			case PSCO_T:
				renamings[i] = np.getValue().toString();
				writeFinalValue[i] = np.isWriteFinalValue();

				// Get ID
				String id = renamings[i];

				// Get Object
				try {
					obj = nw.getPersistentObject(id);
				} catch (StorageException e) {
					throw new JobExecutionException(ERROR_PERSISTENT_OBJ + " with id " + id, e);
				}
				
				// Check if object is null
				if (obj == null) {
					StringBuilder sb = new StringBuilder();
					if (hasTarget && i == numParams - 1) {
						sb.append("Target PSCO with renaming ");
					} else {
						sb.append("PSCO parameter ").append(i).append(" with renaming ");
					}
					sb.append(renamings[i]);
					sb.append(", method ").append(methodName);
					sb.append(", class ").append(className);
					sb.append(" is null!").append("\n");
					
					throw new JobExecutionException(sb.toString());
				}

				// Store information as target or as normal parameter
				if (hasTarget && i == numParams - 1) {
					// Last parameter is the target object
					target.setValue(obj);
				} else {
					// Any other parameter
					types[i] = obj.getClass();
					values[i] = obj;
				}
				break;
		}

		isFile[i] = (np.getType().equals(DataType.FILE_T));
		canBePSCO[i] = (np.getType().equals(DataType.OBJECT_T)) || (np.getType().equals(DataType.PSCO_T));
	}

	private Method getMethod(String className, String methodName, Class<?>[] types) throws JobExecutionException {
		Class<?> methodClass = null;
		Method method = null;
		try {
			methodClass = Class.forName(className);
		} catch (Exception e) {
			throw new JobExecutionException(ERROR_CLASS_REFLECTION, e);
		}
		try {
			method = methodClass.getMethod(methodName, types);
		} catch (Exception e) {
			throw new JobExecutionException(ERROR_METHOD_REFLECTION, e);
		}

		return method;
	}

	private Object invokeMethod(NIOWorker nw, Method method, TargetParam target, Object[] values) throws JobExecutionException {
		/* Invoke the requested method ******************************* */
		Object retValue = null;

		if (NIOWorker.getExecutionType().equals(ITConstants.EXECUTION_INTERNAL)) {
			// Invoke the requested method from COMPSs
			retValue = internalExecution(nw, method, target, values);
		} else {
			retValue = externalExecution(nw, method, target, values);
		}

		return retValue;
	}

	private Object internalExecution(NIOWorker nw, Method method, TargetParam target, Object[] values) throws JobExecutionException {
		Object retValue = null;

		if (NIOTracer.isActivated()) {
			NIOTracer.emitEvent(NIOTracer.Event.STORAGE_INVOKE.getId(), NIOTracer.Event.STORAGE_INVOKE.getType());
		}

		try {
			logger.info("Invoked " + method.getName() + " of " + target + " in " + nw.getHostName());
			retValue = method.invoke(target.getValue(), values);
		} catch (Exception e) {
			throw new JobExecutionException(ERROR_TASK_EXECUTION, e);
		} finally {
			if (NIOTracer.isActivated()) {
				NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.STORAGE_INVOKE.getType());
			}
		}

		return retValue;
	}

	private Object externalExecution(NIOWorker nw, Method method, TargetParam target, Object[] values) throws JobExecutionException {
		// Invoke the requested method from the external platform
		
		// WARN: ExternalExecution is only supported for methods with PSCO as target object
		
		// Check and retrieve target PSCO Id
		String id = null;
		try {
			id = ((StubItf) target.getValue()).getID();
		} catch (Exception e) {
			throw new JobExecutionException(ERROR_EXTERNAL_NO_PSCO, e);
		}
		if (id == null) {
			throw new JobExecutionException(ERROR_EXTERNAL_NO_PSCO);
		}

		// Call Storage executeTask
		if (logger.isDebugEnabled()) {
			logger.info("External ExecuteTask " + method.getName() + " with target PSCO Id " + id + " in " + nw.getHostName());
		} else {
			logger.info("External ExecuteTask " + method.getName());
		}

		if (NIOTracer.isActivated()) {
			NIOTracer.emitEvent(NIOTracer.Event.STORAGE_EXECUTETASK.getId(), NIOTracer.Event.STORAGE_EXECUTETASK.getType());
		}

		PSCOCallbackHandler callback = new PSCOCallbackHandler();
		try {
			String call_result = StorageItf.executeTask(id, method, values, nw.getHostName(), callback);

			logger.debug(call_result);

			// Wait for execution
			callback.waitForCompletion();
		} catch (StorageException e) {
			throw new JobExecutionException(ERROR_STORAGE_CALL, e);
		} catch (InterruptedException e) {
			throw new JobExecutionException(ERROR_CALLBACK_INTERRUPTED, e);
		} finally {
			if (NIOTracer.isActivated()){
				NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.STORAGE_EXECUTETASK.getType());
			}
		}
		
		// Process the return status
		CallbackEvent.EventType callStatus = callback.getStatus();
		if (!callStatus.equals(CallbackEvent.EventType.SUCCESS)) {
			throw new JobExecutionException(ERROR_EXTERNAL_EXECUTION);
		}

		// Process return value
		Object retValue = null;
		if (method.getReturnType().getName().compareTo(void.class.getName()) != 0) {
			try {
				retValue = callback.getResult();
			} catch (StorageException e) {
				logger.warn(WARN_RET_VALUE_EXCEPTION, e);
				retValue = null;
			}
		}

		return retValue;
	}

	private void checkSCOPersistence(NIOWorker nw, NIOTask nt, int numParams, boolean hasTarget, TargetParam target, Object retValue,
			String[] renamings, Object[] values, boolean[] canBePSCO, boolean[] writeFinalValue) {

		// Check all parameters and target
		for (int i = 0; i < numParams; i++) {
			if (canBePSCO[i] && writeFinalValue[i]) {
				// Get information as target or as normal parameter
				Object obj = null;
				if (hasTarget && i == numParams - 1) {
					obj = target.getValue();
				} else {
					obj = values[i];
				}

				// Check if it is a PSCO and has been persisted in task
				String id = null;
				try {
					StubItf psco = (StubItf) obj;
					id = psco.getID();
				} catch (Exception e) {
					// No need to raise an exception because normal objects are not PSCOs
					id = null;
				}

				// Update to PSCO if needed
				if (id != null) {
					// Object has been persisted, we store the PSCO and change the value to its ID
					nw.storePersistentObject(id, obj);

					if (hasTarget && i == numParams - 1) {
						target.setValue(id);
					} else {
						values[i] = id;
					}
					nt.getParams().get(i).setType(DataType.PSCO_T);
					nt.getParams().get(i).setValue(id);
				}
			}
		}

		// Check return
		if (retValue != null) {
			// Check if it is a PSCO and has been persisted in task
			String id = null;
			try {
				StubItf psco = (StubItf) retValue;
				id = psco.getID();
			} catch (Exception e) {
				// No need to raise an exception because normal objects are not PSCOs
				id = null;
			}

			// Update to PSCO if needed
			if (id != null) {
				// Object has been persisted
				nt.getParams().getLast().setType(DataType.PSCO_T);
				nt.getParams().getLast().setValue(id);
			}
		}
	}

	private void checkJobFiles(String className, String methodName, int numParams, Object[] values, boolean[] isFile)
			throws JobExecutionException {

		// Check if all the output files have been actually created (in case user has forgotten)
		// No need to distinguish between IN or OUT files, because IN files will exist, and
		// if there's one or more missing, they will be necessarily out.
		boolean allOutFilesCreated = true;
		for (int i = 0; i < numParams; i++) {
			if (isFile[i]) {
				String filepath = (String) values[i];
				File f = new File(filepath);
				if (!f.exists()) {
					String errMsg = "ERROR: File with path '" + values[i] + "' has not been generated by task '" + methodName + "'"
							+ "' (in class '" + className + "' at method '" + methodName + "', parameter number: " + (i + 1) + " )";
					System.out.println(errMsg);
					System.err.println(errMsg);
					allOutFilesCreated = false;
				}
			}
		}

		if (!allOutFilesCreated) {
			throw new JobExecutionException(ERROR_OUT_FILES + methodName + "'");
		}
	}

	private void writeUpdatedParameters(NIOWorker nw, NIOTask nt, int numParams, boolean hasTarget, TargetParam target, Object retValue,
			String[] renamings, Object[] values, boolean[] writeFinalValue) {

		// Write to disk the updated object parameters, if any (including the target)
		for (int i = 0; i < numParams; i++) {
			if (writeFinalValue[i]) {
				Object res = (hasTarget && i == numParams - 1) ? target.getValue() : values[i];
				// Update task params for TaskResult command
				nt.getParams().get(i).setValue(res);
				// The parameter is a file, an object or PSCO Id that MUST be stored
				nw.storeObject(renamings[i], res);
			}
		}

		// Serialize the return value if existing
		// PSCOs are already stored, skip them
		if (retValue != null) {
			String renaming = (String) nt.getParams().getLast().getValue();
			// Always stored because it can only be a OUT object
			nw.storeObject(renaming.substring(renaming.lastIndexOf('/') + 1), retValue);
		}
	}


	/**
	 * Class to get the Storage Callback
	 * 
	 */
	private class PSCOCallbackHandler extends CallbackHandler {

		private CallbackEvent event;
		private Semaphore sem;
		
		public PSCOCallbackHandler() {
			this.sem = new Semaphore(0);
		}

		@Override
		protected void eventListener(CallbackEvent e) {
			this.event = e;
			logger.debug("Received event task finished with callback id " + event.getRequestID());

			synchronized (this) {
				this.notifyAll();
			}
			this.sem.release();
		}
		
		public void waitForCompletion() throws InterruptedException{
			this.sem.acquire();
		}
		
		public CallbackEvent.EventType getStatus() {
			return this.event.getType();
		}

		public Object getResult() throws StorageException {
			return StorageItf.getResult(event);
		}

	}

	/**
	 * Class to Wrap Target Parameter
	 * 
	 */
	private class TargetParam {

		private Object value = null;


		public TargetParam() {
		}

		public Object getValue() {
			return this.value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

	}

}
