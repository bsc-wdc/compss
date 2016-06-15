package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.ITConstants;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.nio.NIOParam;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.exceptions.JobExecutionException;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.types.parameter.PSCOId;
import integratedtoolkit.util.Tracer;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.bytecode.Descriptor;
import storage.CallbackEvent;
import storage.CallbackHandler;
import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


public class JavaExecutor extends Executor {

    @Override
    String createSandBox(String baseWorkingDir) {
        //SandBox not supported
        return null;
    }

    @Override
    void executeTask(String sandBox, NIOTask nt, NIOWorker nw) throws Exception {
        /* Tracing related information */
        int taskType = nt.getTaskType() +1 ;  // +1 Because Task iD can't be 0 (0 signals end task)
        int taskId = nt.getTaskId();

        /* Task information */
        boolean debug = NIOWorker.isWorkerDebugEnabled;
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
        PSCOId pscoIds[] = new PSCOId[numParams];
		PSCOId targetPscoId = null;
		NIOParam npTarget = null;
        
        Iterator<NIOParam> params = nt.getParams().iterator();
        for (int i = 0; i < numParams; i++) {
            NIOParam np = params.next();
            
			String renaming = null;
			String name = null;
			Object o = null;
            
            // We need to use wrapper classes for basic types, reflection will unwrap automatically
            switch (np.getType()) {
                case FILE_T:
                    types[i] = String.class;
                    values[i] = np.getValue();
                    writeFinalValue[i] = np.isWriteFinalValue();
                    break;
    			case SCO_T:
    			case PSCO_T:
    				renaming = renamings[i] = np.getValue().toString();
    				name = renaming;
                    writeFinalValue[i] = np.isWriteFinalValue();
    				o = nw.getObject(name);
    				PSCOId pscoId = null;
    				if (o instanceof PSCOId) {
    					pscoId = (PSCOId) o;															    						
						if (!writeFinalValue[i]) {											
						
							/*
							System.out.print("[Worker] Backends: ");
							for (String backend: pscoId.getBackends()){
								System.out.print(backend +  " ");
							}
							System.out.println();
							System.out.println("[Worker] Hostname: " + nw.getHostName());
							*/
												
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
		    								"Error New Replica: parameter " + i
		    										+ " with id " + pscoId.getId()
		    										+ ", method " + methodName + ", class "
		    										+ className 
		    										+ ", exception " + e.getMessage() + "\n" );
									
								} finally {   
									if (tracing) {
										NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_NEWREPLICA.getType());    									
									}
								}
									
							}												
						} else {
							
							if (tracing) {
								NIOTracer.emitEvent(Tracer.Event.STORAGE_NEWVERSION.getId(), Tracer.Event.STORAGE_NEWVERSION.getType());
							}

							try {
								// New PSCO Version 
								String newId = StorageItf.newVersion(pscoId.getId(), nw.getHostName());
    							// Modify the PSCO Identifier 
    							pscoId.setId(newId);

							} catch (StorageException e) {
	    						throw new JobExecutionException(
	    								"Error New Version: parameter " + i
	    										+ " with id " + pscoId.getId()
	    										+ ", method " + methodName + ", class "
	    										+ className 
	    										+ ", exception " + e.getMessage() + "\n" );
							} finally {
								if (tracing) {
									NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_NEWVERSION.getType());
								}
							}
						}						

						//if (nw.executionType.compareTo(ITConstants.COMPSs) == 0) {

							if (tracing) {
								NIOTracer.emitEvent( Tracer.Event.STORAGE_GETBYID.getId(), Tracer.Event.STORAGE_GETBYID.getType()); 
							}

							try {
								// Get the PSCO by ID
								o = StorageItf.getByID(pscoId.getId());
							} catch (StorageException e) {
	    						throw new JobExecutionException(
	    								"Error GetByID: parameter " + i
	    										+ " with id " + pscoId.getId()
	    										+ ", method " + methodName + ", class "
	    										+ className 
	    										+ ", exception " + e.getMessage() + "\n" );    								
							} finally {
    							if (tracing) {
    								NIOTracer.emitEvent( Tracer.EVENT_END, Tracer.Event.STORAGE_GETBYID.getType());
    							}    								    								
							}

						/*
						} else {
							// We save the loading of the object into the memory
							// space!!!
							o = pscoId;
						}
						*/
    				}

    				if (hasTarget && i == numParams - 1) { // last parameter is the
    														// target object
    					if (o == null) {
    						throw new JobExecutionException("Target SCO with renaming " + name	+ ", method " + methodName + ", class "	+ className + " is null!" + "\n");
    					}
    					npTarget = np;
    					target = o;
    					targetPscoId = pscoId;
    				} else {
    					if (o == null) {
    						throw new JobExecutionException("SCO parameter " + i + " with renaming " + name + ", method " + methodName + ", class " + className	+ " is null!" + "\n");
    					}
    					types[i] = o.getClass();
    					values[i] = o;
    					pscoIds[i] = pscoId;
    				}
    				break;                                        
                case OBJECT_T:
                    renaming = renamings[i] = np.getValue().toString();
                    name = renaming;
                    writeFinalValue[i] = np.isWriteFinalValue();                    
                    o = nw.getObject(name);
                    if (hasTarget && i == numParams - 1) { // last parameter is the target object
                        if (o == null) {
                            throw new JobExecutionException("Target object with renaming " + name + ", method " + methodName + ", class " + className + " is null!" + "\n");
                        }
                        npTarget = np;
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
            
            isFile[i] = (np.getType().equals(DataType.FILE_T));
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
               
		if (targetPscoId == null) {		
			// Invoke the requested method from COMPSs
			if (tracing) {
				NIOTracer.emitEvent(Tracer.Event.STORAGE_INVOKE.getId(), Tracer.Event.STORAGE_INVOKE.getType());
			}
			
			try {
				retValue = method.invoke(target, values);
				logger.info("Invoked " + method.getName() + " of " + target + " in " + nw.getHostName() );
			} catch (Exception e) {
				throw e;
			} finally {
				if (tracing) {
					NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_INVOKE.getType());
				}
			}
			
		} else {
			// Invoke the requested method from the external platform
			if (tracing) {
				NIOTracer.emitEvent(Tracer.Event.STORAGE_EXECUTETASK.getId(), Tracer.Event.STORAGE_EXECUTETASK.getType());
			}

			// int n = method.getParameterCount();
			int n = method.getParameterAnnotations().length;
			ClassPool pool = ClassPool.getDefault();
			Class<?>[] cParams = method.getParameterTypes();
			CtClass[] ctParams = new CtClass[n];
			for (int i = 0; i < n; i++) {
				ctParams[i] = pool.getCtClass(((Class<?>) cParams[i]).getName());
			}

			String descriptor = method.getName()
					+ Descriptor.ofMethod(
							pool.getCtClass(method.getReturnType().getName()),
							ctParams);

			PSCOCallbackHandler callback = new PSCOCallbackHandler();

			// Call Storage executeTask
			logger.info("executeTask " + descriptor +  " with " + target +" in " + nw.getHostName());
			
			try {
				String call_result = StorageItf.executeTask(((PSCOId) targetPscoId).getId(), descriptor, values, nw.getHostName(), callback);
				logger.debug(call_result);
			} catch (StorageException e) {				
				throw e;							
			} finally {
				if (tracing) {
					NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_EXECUTETASK.getType());
				}				
			}

			synchronized (callback) {
				callback.wait();
			}

			retValue = null;
			if (method.getReturnType().getName().compareTo(void.class.getName()) != 0) {
				try {
					retValue = callback.getResult();
				} catch (StorageException e) {
					retValue = null;
				}
			}
		}
		
        // TRACING: Emit end task 
        if (tracing) {
            NIOTracer.emitEventAndCounters(NIOTracer.EVENT_END, NIOTracer.getTaskEventsType());
            NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getTaskSchedulingType());
        }
        
		if (hasTarget && (npTarget.getType() == DataType.SCO_T) && (targetPscoId == null)) {
			String renaming = renamings[numParams - 1] = npTarget.getValue().toString();
			String name = renaming;			
			writeFinalValue[numParams - 1] = npTarget.isWriteFinalValue();
			StubItf sco = (StubItf) nw.getObject(name);
			String id = null;
			try {
				id = sco.getID();
			} catch (Exception e){
				id = null;
			}
			if (id != null) {
				targetPscoId = new PSCOId(sco, id);
				List<String> backends = StorageItf.getLocations(targetPscoId.getId());
				targetPscoId.setBackends(backends);
				npTarget.setValue(targetPscoId);
			}
		}

		// Write to disk the target PSCO
		if (targetPscoId != null) {
			target = targetPscoId;
		}
        
		for (int i = 0; i < numParams; i++) {
			if (pscoIds[i] != null) {
				// Put the new SCO Identifier as modified value.
				values[i] = pscoIds[i];
			}
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
        
		if (debug) {
			// Print request information
			System.out.println("WORKER - End job execution");
		}        
    }

    @Override
    void removeSandBox(String sandBox) {
        //SandBox not supported
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
