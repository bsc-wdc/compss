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
import integratedtoolkit.types.parameter.PSCOId;
import integratedtoolkit.util.RequestQueue;
import integratedtoolkit.util.Tracer;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;
import javassist.bytecode.Descriptor;
import storage.CallbackEvent;
import storage.CallbackHandler;
import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


public class JavaExecutor extends Executor {
	
	private static final String ERROR_CLASS_REFLECTION = "Cannot get class by reflection";
	private static final String ERROR_METHOD_REFLECTION = "Cannot get method by reflection";
	private static final String ERROR_TASK_EXECUTION = "ERROR: Exception executing task (user code)";
	private static final String ERROR_CLASS_NOT_FOUND = "ERROR: Class not found on external call";
	private static final String ERROR_CALLBACK_INTERRUPTED = "ERROR: External callback interrupted";
	private static final String ERROR_SERIALIZED_OBJ = "ERROR: Cannot serialize object";
	private static final String ERROR_STORAGE_CALL = "ERROR: External executeTask call failed";
	private static final String ERROR_PSCO_GET_LOC = "ERROR: Exception raised in external getLocations";
	private static final String ERROR_OUT_FILES = "ERROR: One or more OUT files have not been created by task '";
	
	private final boolean debug;


	public JavaExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue) {
		super(nw, pool, queue);
		
		this.debug = NIOWorker.isWorkerDebugEnabled;
	}

	@Override
	public void executeTask(NIOWorker nw, NIOTask nt, String outputsBasename) throws JobExecutionException {
		/* Register outputs *****************************************/
		NIOWorker.registerOutputs(outputsBasename);
		
		/* TRY TO PROCESS THE TASK *********************************/
		System.out.println("[JAVA EXECUTOR] executeTask - Begin task execution");
		try {
			processTask(nw, nt);
		} catch (JobExecutionException jee) {
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
		/* Tracing related information ******************************/
		int taskType = nt.getTaskType() + 1; // +1 Because Task ID can't be 0 (0 signals end task)
		int taskId = nt.getTaskId();

		/* Task information *****************************************/
		String className = nt.getClassName();
		String methodName = nt.getMethodName();
		boolean hasTarget = nt.isHasTarget();
		int numParams = nt.getNumParams();

		/* Parameters information ***********************************/
		int totalNumberOfParams = hasTarget ? numParams - 1 : numParams;	// Don't count target if needed (i.e. obj.func())
		Class<?>[] types = new Class[totalNumberOfParams];
		Object[] values = new Object[totalNumberOfParams];
		boolean[] isFile = new boolean[numParams];
		String[] renamings = new String[numParams];
		boolean[] writeFinalValue = new boolean[numParams];		// By default the boolean initializer is in false
					    										// False because basic types aren't nor written nor preserved
		PSCOId[] pscoIds = new PSCOId[numParams];
		
		/* Parse the parameters *************************************/
		Target target = new Target();
		Iterator<NIOParam> params = nt.getParams().iterator();
		for (int i = 0; i < numParams; i++) {
			NIOParam np = params.next();
			processParameter(nw, className, methodName, hasTarget, target, numParams, 
					np, i, types, values, isFile, renamings, writeFinalValue, pscoIds);
		}

		/* DEBUG information ****************************************/
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

		/* Use reflection to get the requested method ****************/
		Method method = getMethod(className, methodName, types);
		
		/* Invoke the requested method *******************************/
		// TRACING: Emit start task
		if (tracing) {
			NIOTracer.emitEventAndCounters(taskType, NIOTracer.getTaskEventsType());
			NIOTracer.emitEvent(taskId, NIOTracer.getTaskSchedulingType());
		}
		Object retValue = null;
		try {
			retValue = invokeMethod(nw, method, target, values);
		} catch (JobExecutionException jee) {
			throw jee;
		} finally {
			if (tracing) {
				NIOTracer.emitEventAndCounters(NIOTracer.EVENT_END, NIOTracer.getTaskEventsType());
				NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getTaskSchedulingType());
			}
		}
		
		/* Check SCO persistence for return and target ***************/
		checkSCOPersistence(nw, numParams, hasTarget, target, renamings, writeFinalValue, values, pscoIds);
		
		/* Check files existence *************************************/
		checkJobFiles(className, methodName, numParams, values, isFile);
		
		/* Write to disk the updated values **************************/
		writeUpdatedParameters(nw, nt, hasTarget, numParams, writeFinalValue, values, renamings, target, retValue);
	}

	private void processParameter(NIOWorker nw, String className, String methodName, boolean hasTarget, Target target,
			int numParams, NIOParam np, int i, Class<?>[] types, Object[] values, boolean[] isFile, 
			String[] renamings, boolean[] writeFinalValue, PSCOId[] pscoIds) throws JobExecutionException {

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
				Object o;
				try {
					o = nw.getObject(renamings[i]);
				} catch (SerializedObjectException e) {
					throw new JobExecutionException(ERROR_SERIALIZED_OBJ, e);
				}
				if (hasTarget && i == numParams - 1) { // last parameter is the target object
					if (o == null) {
						throw new JobExecutionException("Target object with renaming " + renamings[i] 
								+ ", method " + methodName + ", class " + className + " is null!" + "\n");
					}
					target.setNpTarget(np);
					target.setTarget(o);
				} else {
					if (o == null) {
						throw new JobExecutionException("Object parameter " + i + " with renaming " + renamings[i] + ", method "
								+ methodName + ", class " + className + " is null!" + "\n");
					}
					types[i] = o.getClass();
					values[i] = o;
				}
				break;
			case SCO_T:
			case PSCO_T:
				renamings[i] = np.getValue().toString();
				writeFinalValue[i] = np.isWriteFinalValue();
				Object obj;
				try {
					obj = nw.getObject(renamings[i]);
				} catch (SerializedObjectException e) {
					throw new JobExecutionException(ERROR_SERIALIZED_OBJ, e);
				}
				PSCOId pscoId = null;
				if (obj instanceof PSCOId) {
					pscoId = (PSCOId) obj;
					if (!writeFinalValue[i]) {
						if ((NIOWorker.getExecutionType().compareTo(ITConstants.COMPSs) != 0)
								&& !pscoId.getBackends().contains(nw.getHostName())) {

							if (tracing) {
								NIOTracer.emitEvent(Tracer.Event.STORAGE_NEWREPLICA.getId(), Tracer.Event.STORAGE_NEWREPLICA.getType());
							}

							try {
								// Replicate PSCO
								StorageItf.newReplica(pscoId.getId(), nw.getHostName());
							} catch (StorageException e) {
								throw new JobExecutionException(
										"Error New Replica: parameter " + i + " with id " + pscoId.getId()
												+ ", method " + methodName + ", class " + className
												+ ", exception " + e.getMessage() + "\n");
							} finally {
								if (tracing) {
									NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_NEWREPLICA.getType());
								}
							}

						}
					} else {
						if (tracing) {
							NIOTracer.emitEvent( Tracer.Event.STORAGE_NEWVERSION.getId(), Tracer.Event.STORAGE_NEWVERSION.getType());
						}

						try {
							// New PSCO Version
							String newId = StorageItf.newVersion(pscoId.getId(), nw.getHostName());
							// Modify the PSCO Identifier
							pscoId.setId(newId);
						} catch (StorageException e) {
							throw new JobExecutionException(
									"Error New Version: parameter " + i + " with id " + pscoId.getId()
											+ ", method " + methodName + ", class " + className + ", exception " + e.getMessage() + "\n");
						} finally {
							if (tracing) {
								NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_NEWVERSION.getType());
							}
						}
					}

					// GET PSCO BY ID
					if (tracing) {
						NIOTracer.emitEvent(Tracer.Event.STORAGE_GETBYID.getId(), Tracer.Event.STORAGE_GETBYID.getType());
					}

					try {
						obj = StorageItf.getByID(pscoId.getId());
					} catch (StorageException e) {
						throw new JobExecutionException(
								"Error GetByID: parameter " + i + " with id "
										+ pscoId.getId() + ", method "
										+ methodName + ", class " + className
										+ ", exception " + e.getMessage() + "\n");
					} finally {
						if (tracing) {
							NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_GETBYID.getType());
						}
					}
				}

				// Store parameter information (as target if needed)
				if (hasTarget && i == numParams - 1) { // last parameter is the target object
					if (obj == null) {
						throw new JobExecutionException("Target SCO with renaming " + renamings[i]
										+ ", method " + methodName + ", class " + className + " is null!" + "\n");
					}
					target.setNpTarget(np);
					target.setTarget(obj);
					target.setTargetPscoId(pscoId);
				} else {
					if (obj == null) {
						throw new JobExecutionException("SCO parameter " + i + " with renaming " + renamings[i] + ", method "
								+ methodName + ", class " + className + " is null!" + "\n");
					}
					types[i] = obj.getClass();
					values[i] = obj;
					pscoIds[i] = pscoId;
				}
				break;				
		}

		isFile[i] = (np.getType().equals(DataType.FILE_T));
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
	
	private Object invokeMethod(NIOWorker nw, Method method, Target target, Object[] values) throws JobExecutionException {
		/* Invoke the requested method ********************************/
		Object retValue = null;

		if (target.getTargetPscoId() == null) {
			// Invoke the requested method from COMPSs
			retValue = internalExecution(nw, method, target, values);
		} else {
			retValue = externalExecution(nw, method, target, values);
		}
		
		return retValue;
	}
	
	private Object internalExecution(NIOWorker nw, Method method, Target target, Object[] values) throws JobExecutionException {
		Object retValue = null;
		if (tracing) {
			NIOTracer.emitEvent(Tracer.Event.STORAGE_INVOKE.getId(), Tracer.Event.STORAGE_INVOKE.getType());
		}

		try {
			retValue = method.invoke(target.getTarget(), values);
			logger.info("Invoked " + method.getName() + " of " + target.getTarget() + " in " + nw.getHostName());
		} catch (Exception e) {
			throw new JobExecutionException(ERROR_TASK_EXECUTION, e);
		} finally {
			if (tracing) {
				NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_INVOKE.getType());
			}
		}
		
		return retValue;
	}
	
	private Object externalExecution(NIOWorker nw, Method method, Target target, Object[] values) throws JobExecutionException {
		// Invoke the requested method from the external platform
		if (tracing) {
			NIOTracer.emitEvent(Tracer.Event.STORAGE_EXECUTETASK.getId(), Tracer.Event.STORAGE_EXECUTETASK.getType());
		}

		int n = method.getParameterAnnotations().length;
		ClassPool pool = ClassPool.getDefault();
		Class<?>[] cParams = method.getParameterTypes();
		CtClass[] ctParams = new CtClass[n];
		for (int i = 0; i < n; i++) {
			try {
				ctParams[i] = pool.getCtClass(((Class<?>) cParams[i]).getName());
			} catch (NotFoundException e) {
				throw new JobExecutionException(ERROR_CLASS_NOT_FOUND + " " + cParams[i].getName(), e);
			}
		}

		String descriptor;
		try {
			descriptor = method.getName() + Descriptor.ofMethod(pool.getCtClass(method.getReturnType().getName()), ctParams);
		} catch (NotFoundException e) {
			throw new JobExecutionException(ERROR_CLASS_NOT_FOUND + " " + method.getReturnType().getName(), e);
		}
		

		// Call Storage executeTask
		logger.info("executeTask " + descriptor + " with " + target.getTarget() + " in " + nw.getHostName());
		PSCOCallbackHandler callback = new PSCOCallbackHandler();
		try {
			String call_result = StorageItf.executeTask(((PSCOId) target.getTargetPscoId()).getId(), descriptor, values, nw.getHostName(), callback);
			logger.debug(call_result);
			
			callback.wait();
		} catch (StorageException e) {
			throw new JobExecutionException(ERROR_STORAGE_CALL, e);
		} catch (InterruptedException e) {
			throw new JobExecutionException(ERROR_CALLBACK_INTERRUPTED, e);
		} finally {
			if (tracing) {
				NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_EXECUTETASK.getType());
			}
		}

		Object retValue = null;
		if (method.getReturnType().getName().compareTo(void.class.getName()) != 0) {
			try {
				retValue = callback.getResult();
			} catch (StorageException e) {
				retValue = null;
			}
		}
		
		return retValue;
	}
	
	private void checkSCOPersistence(NIOWorker nw, int numParams, boolean hasTarget, Target target,
			String[] renamings, boolean[] writeFinalValue, Object[] values, PSCOId[] pscoIds) throws JobExecutionException {
		
		/* Check target SCO Persistence *******************************/
		if (hasTarget && (target.getNpTarget().getType() == DataType.SCO_T) && (target.getTargetPscoId() == null)) {
			String renaming = renamings[numParams - 1] = target.getNpTarget().getValue().toString();
			String name = renaming;
			writeFinalValue[numParams - 1] = target.getNpTarget().isWriteFinalValue();
			StubItf sco;
			try {
				sco = (StubItf) nw.getObject(name);
			} catch (SerializedObjectException e) {
				throw new JobExecutionException(ERROR_SERIALIZED_OBJ, e);
			}
			String id = null;
			try {
				id = sco.getID();
			} catch (Exception e) {
				id = null;
			}
			if (id != null) {
				target.setTargetPscoId(new PSCOId(sco, id));
				List<String> backends;
				try {
					backends = StorageItf.getLocations(target.getTargetPscoId().getId());
				} catch (StorageException e) {
					throw new JobExecutionException(ERROR_PSCO_GET_LOC, e);
				}
				target.getTargetPscoId().setBackends(backends);
				target.getNpTarget().setValue(target.getTargetPscoId());
			}
		}

		// Write to disk the target PSCO
		if (target.getTargetPscoId() != null) {
			target.setTarget(target.getTargetPscoId());
		}

		for (int i = 0; i < numParams; i++) {
			if (pscoIds[i] != null) {
				// Put the new SCO Identifier as modified value.
				values[i] = pscoIds[i];
			}
		}
	}
	
	private void checkJobFiles(String className, String methodName, int numParams, Object[] values, 
			boolean[] isFile) throws JobExecutionException {
		
		// Check if all the output files have been actually created (in case user has forgotten)
		// No need to distinguish between IN or OUT files, because IN files will exist, and 
		// if there's one or more missing, they will be necessarily out.
		boolean allOutFilesCreated = true;
		for (int i = 0; i < numParams; i++) {
			if (isFile[i]) {
				String filepath = (String) values[i];
				File f = new File(filepath);
				if (!f.exists()) {
					String errMsg = "ERROR: File with path '" + values[i] + "' has not been generated by task '"
							+ methodName + "'" + "' (in class '" + className + "' at method '" + methodName 
							+ "', parameter number: " + (i + 1) + " )";
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
	
	private void writeUpdatedParameters(NIOWorker nw, NIOTask nt, boolean hasTarget, int numParams, 
			boolean[] writeFinalValue, Object[] values, String[] renamings, Target target, Object retValue) {
		
		// Write to disk the updated object parameters, if any (including the target)
		for (int i = 0; i < numParams; i++) {
			if (writeFinalValue[i]) {
				// The parameter is a file or an object that MUST be stored
				Object res = (hasTarget && i == numParams - 1) ? target.getTarget() : values[i];
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
	
	
	private class Target {
		
		private NIOParam npTarget = null;
		private Object target = null;
		private PSCOId targetPscoId = null;
		
		public Target() {
			
		}

		public NIOParam getNpTarget() {
			return npTarget;
		}

		public void setNpTarget(NIOParam npTarget) {
			this.npTarget = npTarget;
		}

		public Object getTarget() {
			return target;
		}

		public void setTarget(Object target) {
			this.target = target;
		}

		public PSCOId getTargetPscoId() {
			return targetPscoId;
		}

		public void setTargetPscoId(PSCOId targetPscoId) {
			this.targetPscoId = targetPscoId;
		}
		
	}
	

	private class PSCOCallbackHandler extends CallbackHandler {

		private CallbackEvent event;

		@Override
		protected void eventListener(CallbackEvent e) {

			this.event = e;
			logger.debug("Received event task finished with callback id "
					+ event.getRequestID());

			synchronized (this) {
				this.notifyAll();
			}
		}

		public Object getResult() throws StorageException {
			return StorageItf.getResult(event);
		}

	}

}
