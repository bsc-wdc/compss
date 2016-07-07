package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.nio.NIOParam;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.exceptions.JobExecutionException;
import integratedtoolkit.nio.exceptions.SerializedObjectException;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.nio.worker.util.TaskResultReader;
import integratedtoolkit.types.parameter.PSCOId;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.RequestQueue;
import integratedtoolkit.util.Tracer;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import storage.StorageException;
import storage.StorageItf;


public abstract class ExternalExecutor extends Executor {
	
	private static final String ERROR_PIPE_CLOSE 	= "Error on closing pipe ";
	private static final String ERROR_PIPE_QUIT 	= "Error sending quit to pipe ";

	// Piper script properties
	public static final int MAX_RETRIES = 3;
	public static final String TOKEN_SEP 			= " ";
	public static final String TOKEN_NEW_LINE 		= "\n";
	public static final String END_TASK_TAG 		= "endTask";
	public static final String QUIT_TAG 			= "quit";
	private static final String EXECUTE_TASK_TAG 	= "task";
	
	private final String writePipe;				// Pipe for sending executions
	private TaskResultReader taskResultReader;	// Process result reader (initialized by PoolManager, started/stopped by us)
	
	
	public ExternalExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue, String writePipe, TaskResultReader	resultReader) {
		super(nw, pool, queue);
		
		this.writePipe = writePipe;
		this.taskResultReader = resultReader;
		
		// Start task Reader
		this.taskResultReader.start();
	}

    @Override
    public void executeTask(NIOWorker nw, NIOTask nt, String outputsBasename) throws Exception {
        ArrayList<String> args = getTaskExecutionCommand(nw, nt, nw.getWorkingDir());
        addArguments(args, nt, nw);
        String externalCommand = getArgumentsAsString(args);
        
        String command = outputsBasename + NIOWorker.SUFFIX_OUT + TOKEN_SEP
        		+ outputsBasename + NIOWorker.SUFFIX_ERR + TOKEN_SEP
				+ externalCommand;

        executeExternal(nt.getJobId(), command, nt);
    }
    
    @Override
    public void finish() {
    	logger.info("Finishing ExternalExecutor");
    	
    	// Send quit tag to pipe
    	logger.debug("Send quit tag to pipe " + writePipe);
		boolean done = false;
		int retries = 0;
		while (!done && retries < MAX_RETRIES) {
			FileOutputStream output = null;
			try {
				output = new FileOutputStream(writePipe, true);
				String quitCMD = QUIT_TAG + TOKEN_NEW_LINE;
				output.write(quitCMD.getBytes());
				output.flush();
			} catch (Exception e) {
				logger.warn("Error on writing on pipe " + writePipe + ". Retrying " + retries + "/" + MAX_RETRIES);
				++retries;
			} finally {
				if (output != null) {
					try {
						output.close();
					} catch (Exception e) {
						ErrorManager.error(ERROR_PIPE_CLOSE + writePipe, e);
					}
				}
			}
			done = true;
		}
		if (!done) {
			ErrorManager.error(ERROR_PIPE_QUIT + writePipe);
		}

		// ------------------------------------------------------
		// Ask TaskResultReader to stop and wait for it to finish
		logger.debug("Waiting for TaskResultReader");
		Semaphore sem = new Semaphore(0);
		taskResultReader.shutdown(sem);
		try {
			sem.acquire();
		} catch (InterruptedException e) {
			// No need to handle such exceptions
		}
		
		logger.info("End Finishing ExternalExecutor");
    }
    
    public abstract ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox);

    private String getArgumentsAsString(ArrayList<String> args) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String c : args) {
            if (!first) {
                sb.append(" ");
            } else {
                first = false;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private static void addArguments(ArrayList<String> lArgs, NIOTask nt,  NIOWorker nw) throws JobExecutionException, SerializedObjectException {
        lArgs.add(Boolean.toString(tracing));
        lArgs.add(Integer.toString(nt.getTaskId()));
        lArgs.add(Boolean.toString(nt.isWorkerDebug()));
        lArgs.add(nt.getClassName());
        lArgs.add(nt.getMethodName());
        lArgs.add(Boolean.toString(nt.isHasTarget()));
        lArgs.add(Integer.toString(nt.getNumParams()));
        for (NIOParam np : nt.getParams()) {
            DataType type = np.getType();
            lArgs.add(Integer.toString(type.ordinal()));
            switch (type) {
            case FILE_T:
            	lArgs.add(np.getValue().toString());
            	break;
        	case SCO_T:
        	case PSCO_T:
        		String renaming = np.getValue().toString();
        		Object o = nw.getObject(renaming);
        		PSCOId pscoId = null;
        		if (o instanceof PSCOId) {
        			pscoId = (PSCOId) o;					
					if (!np.isWriteFinalValue()) {    							
						if (!pscoId.getBackends().contains(nw.getHostName())){    								
							if (tracing) {
								NIOTracer.emitEvent(Tracer.Event.STORAGE_NEWREPLICA.getId(), Tracer.Event.STORAGE_NEWREPLICA.getType());
							}								
							try {
								// Replicate PSCO
								StorageItf.newReplica(pscoId.getId(), nw.getHostName());
							} catch (StorageException e) {
								throw new JobExecutionException("Error New Replica: parameter with id " + pscoId.getId() +  ", " + e.getMessage());
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
							throw new JobExecutionException("Error New Version: parameter with id " + pscoId.getId() +  ", " + e.getMessage());    						
						} finally {
							if (tracing) {
								NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_NEWVERSION.getType());
							}
						}
					}
	           		lArgs.add(pscoId.getId());
	                lArgs.add(np.isWriteFinalValue() ? "W" : "R");                        		
        		} else {
        			throw new JobExecutionException("Parameter " + o + " must be a PSCO");
        		}
        		break;            	
            case OBJECT_T:
                lArgs.add(np.getValue().toString());
                lArgs.add(np.isWriteFinalValue() ? "W" : "R");                
                break;
            case STRING_T:
                String value = np.getValue().toString();
                String[] vals = value.split(" ");
                int numSubStrings = vals.length;
                lArgs.add(Integer.toString(numSubStrings));
                for (String v : vals) {
                    lArgs.add(v);
                }
                break;
            default:
                lArgs.add(np.getValue().toString());
            }
        }
    }

    private void executeExternal(int jobId, String command, NIOTask nt) throws JobExecutionException {        
        // Emit start task trace
    	int taskType = nt.getTaskType() + 1;  // +1 Because Task ID can't be 0 (0 signals end task)
        int taskId = nt.getTaskId();
        if (tracing) {
            emitStartTask(taskId, taskType);
        }
        
        logger.debug("Starting job process ...");
        // Send executeTask tag to pipe
		boolean done = false;
		int retries = 0;
		while (!done && retries < MAX_RETRIES) {
			FileOutputStream output = null;
			try {
				// Send to pipe : task tID command(jobOut jobErr externalCMD) \n
				String taskCMD = EXECUTE_TASK_TAG + TOKEN_SEP 
						+ jobId + TOKEN_SEP
						+ command + TOKEN_NEW_LINE;
				

		        if (logger.isDebugEnabled()) {
		        	logger.debug("EXECUTOR COMMAND: " + taskCMD);
		        }
				
				output = new FileOutputStream(writePipe, true);
				output.write(taskCMD.getBytes());
				output.flush();
			} catch (Exception e) {
				logger.debug("Error on pipe write. Retry");
				++retries;
			} finally {
				if (output != null) {
					try {
						output.close();
					} catch (Exception e) {
						if (tracing) {
							emitEndTask(taskId);
						}
						throw new JobExecutionException("Job " + jobId + " has failed. Cannot close pipe");
					}
				}
			}
			done = true;
		}
		if (!done) {
			if (tracing) {
				emitEndTask(taskId);
			}
			throw new JobExecutionException("Job " + jobId + " has failed. Cannot write in pipe");
		}
        
		// Retrieving job result
		Semaphore sem = new Semaphore(0);
		taskResultReader.askForTaskEnd(jobId, sem);
		try {
			sem.acquire();
		} catch (InterruptedException e) {
			// No need to handle such exception
		}
		int exitValue = taskResultReader.getExitValue(jobId);

		// Emit end task trace
		if (tracing) {
			emitEndTask(taskId);
		}
        
        logger.debug("Task finished");
        if (exitValue != 0) {
            throw new JobExecutionException("Job " + jobId + " has failed. Exit values is " + exitValue);
        } else {
            logger.debug("Job " + jobId + " has finished with exit value 0");
        }
    }
    
    private void emitStartTask(int taskId, int taskType) {
    	NIOTracer.emitEventAndCounters(taskType, NIOTracer.getTaskEventsType());
        NIOTracer.emitEvent(taskId, NIOTracer.getTaskSchedulingType());
        NIOTracer.emitEvent(taskId, NIOTracer.getSyncType());
        //NIOTracer.emitEvent(NIOTracer.Event.PROCESS_CREATION.getId(), NIOTracer.Event.PROCESS_CREATION.getType());
    }
    
    private void emitEndTask(int taskId) {
    	NIOTracer.emitEvent(taskId, NIOTracer.getSyncType());
        //NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.PROCESS_DESTRUCTION.getType());
        NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getTaskSchedulingType());
        NIOTracer.emitEventAndCounters(NIOTracer.EVENT_END, NIOTracer.getTaskEventsType());
    }
    
}
