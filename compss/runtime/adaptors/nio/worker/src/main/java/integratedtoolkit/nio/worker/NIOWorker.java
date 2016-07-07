package integratedtoolkit.nio.worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import storage.StorageException;
import storage.StorageItf;
import es.bsc.comm.CommException;
import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.stage.Transfer.Destination;
import integratedtoolkit.ITConstants;
import integratedtoolkit.nio.NIOAgent;
import integratedtoolkit.nio.NIOParam;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOURI;
import integratedtoolkit.nio.commands.CommandDataReceived;
import integratedtoolkit.nio.commands.Data;
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOMessageHandler;
import integratedtoolkit.nio.commands.CommandShutdownACK;
import integratedtoolkit.nio.commands.CommandTaskDone;
import integratedtoolkit.nio.commands.workerFiles.CommandWorkerDebugFilesDone;
import integratedtoolkit.nio.exceptions.SerializedObjectException;
import integratedtoolkit.nio.worker.components.DataManager;
import integratedtoolkit.nio.worker.components.ExecutionManager;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Serializer;


public class NIOWorker extends NIOAgent {
	
	// General configuration attributes
	public static boolean isWorkerDebugEnabled;
	
	private static final int MAX_RETRIES = 5;
	
	// Logger
	private static final Logger wLogger = Logger.getLogger(Loggers.WORKER);
	private static final boolean loggerDebug = wLogger.isDebugEnabled();
	private static final String EXECUTION_MANAGER_ERR 		= "Error starting ExecutionManager";
	private static final String DATA_MANAGER_ERROR 			= "Error starting DataManager";
	private static final String ERROR_INCORRECT_NUM_PARAMS 	= "Error: Incorrect number of parameters";
	
	// Application dependent attributes
	private final String deploymentId;
	private final String lang;
	private final String host;
	
	private final String workingDir;
	private final String installDir;
	
	private final String appDir;
	private final String libraryPath;
	private final String classpath;
	private final String pythonpath;
	
	// Storage attributes
	private static String executionType;
	
	// Internal components
	private final ExecutionManager executionManager;
	private final DataManager dataManager;

	// Processes to capture out/err of each job
	private static ThreadPrintStream out;
	private static ThreadPrintStream err;
	public static final String SUFFIX_OUT = ".out";
	public static final String SUFFIX_ERR = ".err";

	static {
		try {
			out = new ThreadPrintStream(SUFFIX_OUT, System.out);
			err = new ThreadPrintStream(SUFFIX_ERR, System.err);
			System.setErr(err);
			System.setOut(out);
		} catch (Exception e) {
			wLogger.error("Exception", e);
		}
	}

	
	public NIOWorker(int numJobThreads, int snd, int rcv, int masterPort, 
			String appUuid, String lang, String hostName, String workingDir, String installDir,
			String appDir, String libPath, String classpath, String pythonpath) {
		
		super(snd, rcv, masterPort);
		
		// Log worker creation
		wLogger.info("NIO Worker init");
		
		// Set attributes	
		this.deploymentId = appUuid;
		this.lang = lang;
		this.host = hostName;
		this.workingDir = (workingDir.endsWith(File.separator) ? workingDir : workingDir + File.separator);
		this.installDir = (installDir.endsWith(File.separator) ? installDir : installDir + File.separator);
		this.appDir = appDir.equals("null") ? "" : appDir;
		this.libraryPath = libPath.equals("null") ? "" : libPath;
		this.classpath = classpath.equals("null") ? "" : classpath;
		this.pythonpath = pythonpath.equals("null") ? "" : pythonpath;

		// Set master node to null (will be set afterwards to the right value)
		this.masterNode = null;
		
		// Start DataManager
		this.dataManager = new DataManager();
		try {
			this.dataManager.init();
		} catch (Exception e) {
			ErrorManager.error(DATA_MANAGER_ERROR, e);
		}
		
		// Start Execution Manager
		this.executionManager = new ExecutionManager(this, numJobThreads);
		
		if (tracing_level == NIOTracer.BASIC_MODE) {
			NIOTracer.enablePThreads();
		}
		
		try {
			this.executionManager.init();
		} catch (Exception e) {
			ErrorManager.error(EXECUTION_MANAGER_ERR, e);
		}
		
		if (tracing_level == NIOTracer.BASIC_MODE) {
			NIOTracer.disablePThreads();
		}
	}

	@Override
	public void setWorkerIsReady(String nodeName) {
		// implemented on NIOAdaptor to notify that the worker is up and ready
	}

	@Override
	public void setMaster(NIONode master) {
		if (masterNode == null) {
			masterNode = new NIONode(master.ip, masterPort);
		}
	}

	@Override
	public boolean isMyUuid(String uuid) {
		return uuid.equals(this.deploymentId);
	}
	
	public static String getExecutionType() {
		return executionType;
	}
	
	public static boolean isTracingEnabled() {
		return tracing;
	}
	
	public String getLang() {
		return this.lang;
	}
	
	public String getWorkingDir() {
		return workingDir;
	}
	
	public String getInstallDir() {
		return this.installDir;
	}
	
	public String getAppDir() {
		return this.appDir;
	}
	
	public String getLibPath() {
		return this.libraryPath;
	}
	
	public String getClasspath() {
		return this.classpath;
	}
	
	public String getPythonpath() {
		return this.pythonpath;
	}

	@Override
	public void receivedNewTask(NIONode master, NIOTask task, LinkedList<String> obsoleteFiles) {
		wLogger.info("Received Job " + task);
		
		if (tracing) {
			NIOTracer.emitEvent(NIOTracer.Event.RECEIVED_NEW_TASK.getId(), NIOTracer.Event.RECEIVED_NEW_TASK.getType());
		}

		// Remove obsolete
		if (obsoleteFiles != null) {
			removeObsolete(obsoleteFiles);
		}

		// Demand files
		wLogger.info("Checking parameters");
		TransferringTask tt = new TransferringTask(task);
		int i = 0;
		for (NIOParam param : task.getParams()) {
			i++;
			if (param.getData() != null) {
				// Parameter has associated data
				wLogger.debug("- Checking transfers for data of parameter " + (String) param.getValue());

				switch (param.getType()) {
            	/* OBJECTS */
                case OBJECT_T:
                case SCO_T:
                case PSCO_T:
                	wLogger.debug("   - " + (String) param.getValue() + " registered as object.");

					boolean catched = false;
					boolean locationsInCache = false;
					boolean existInHost = false;
					boolean askTransfer = false;
					// Try if parameter is in cache
					wLogger.debug("   - Checking if "
							+ (String) param.getValue() + " is in cache.");
					catched = dataManager.checkPresence((String) param.getValue());
					if (!catched) {
						// Try if any of the object locations is in cache
						wLogger.debug("   - Checking if " + (String) param.getValue() + " locations are catched");
						for (NIOURI loc : param.getData().getSources()) {
							if (dataManager.checkPresence(loc.getPath())) {
								// Object found
								wLogger.debug("   - Parameter " + i + "(" + (String) param.getValue() + ") location found in cache.");
								try {
									if (param.isPreserveSourceData()) {
										wLogger.debug("   - Parameter " + i + "(" + (String) param.getValue()
												+ ") preserves sources. CACHE-COPYING");
										Object o = Serializer.deserialize(loc.getPath());
										storeInCache((String) param.getValue(), o);
									} else {
										wLogger.debug("   - Parameter " + i + "(" + (String) param.getValue() + ") erases sources. CACHE-MOVING");
										Object o = dataManager.get(loc.getPath());
										dataManager.remove(loc.getPath());
										storeInCache((String) param.getValue(), o);
									}
									locationsInCache = true;
								} catch (IOException ioe) {
									// If exception is raised, locationsInCache
									// remains false. We log the exception and
									// try host files
									wLogger.error("IOException", ioe);
								} catch (ClassNotFoundException e) {
									// If exception is raised, locationsInCache
									// remains false. We log the exception and
									// try host files
									wLogger.error("ClassNotFoundException", e);
								}
								// Stop looking for locations
								break;
							}
						}

						if (!locationsInCache) {
							// Try if any of the object locations is in the host
							wLogger.debug("   - Checking if " + (String) param.getValue() + " locations are in host");
							NIOURI loc = param.getData().getURIinHost(host);
							if (loc != null) {
								wLogger.debug("   - Parameter " + i + "(" + (String) param.getValue() + ") found at host.");
								try {
									File source = new File(workingDir + File.separator + loc.getPath());
									File target = new File(workingDir + File.separator + param.getValue().toString());
									if (param.isPreserveSourceData()) {
										wLogger.debug("   - Parameter " + i + "(" + (String) param.getValue() + ") preserves sources. COPYING");
										wLogger.debug("         Source: " + source);
										wLogger.debug("         Target: " + target);
										Files.copy(source.toPath(), target.toPath());
									} else {
										wLogger.debug("   - Parameter " + i + "(" + (String) param.getValue() + ") erases sources. MOVING");
										wLogger.debug("         Source: " + source);
										wLogger.debug("         Target: " + target);
										source.renameTo(target);
									}
									// Move object to cache
									Object o = Serializer.deserialize((String) param.getValue());
									storeInCache((String) param.getValue(), o);
									existInHost = true;
								} catch (IOException ioe) {
									// If exception is raised, existInHost
									// remains false. We log the exception and
									// try transfer
									wLogger.error("IOException", ioe);
								} catch (ClassNotFoundException e) {
									// If exception is raised, existInHost
									// remains false. We log the exception and
									// try transfer
									wLogger.error("ClassNotFoundException", e);
								}
							}

							if (!existInHost) {
								// We must transfer the file
								askTransfer = true;
							}
						}
					}

					// Request the transfer if needed
					if (askTransfer) {
						wLogger.info("- Parameter " + i + "(" + (String) param.getValue() + ") does not exist, requesting data transfer");
						DataRequest dr = new WorkerDataRequest(tt, param.getType(), param.getData(), (String) param.getValue());
						addTransferRequest(dr);
					} else {
						// If no transfer, decrease the parameter counter (we
						// already have it)
						wLogger.info("- Parameter " + i + "(" + (String) param.getValue() + ") already exists.");
						--tt.params;
					}
					break;

				/* FILES */
				case FILE_T:
					wLogger.debug("   - " + (String) param.getValue() + " registered as file.");

					boolean exists = false;
					boolean locationsInHost = false;
					askTransfer = false;

					// Try if parameter is in the host
					wLogger.debug("   - Checking if file " + (String) param.getValue() + " exists.");
					File f = new File(param.getValue().toString());
					exists = f.exists();
					if (!exists) {
						// Try if any of the locations is in the same host
						wLogger.debug("   - Checking if " + (String) param.getValue() + " exists in worker");
						NIOURI loc = param.getData().getURIinHost(host);
						if (loc != null) {
							// Data is already present at host
							wLogger.debug("   - Parameter " + i + "(" + (String) param.getValue() + ") found at host.");
							try {
								File source = new File(loc.getPath());
								File target = new File(param.getValue().toString());
								if (param.isPreserveSourceData()) {
									wLogger.debug("   - Parameter " + i + "(" + (String) param.getValue() + ") preserves sources. COPYING");
									wLogger.debug("         Source: " + source);
									wLogger.debug("         Target: " + target);
									Files.copy(source.toPath(), target.toPath());
								} else {
									wLogger.debug("   - Parameter " + i + "(" + (String) param.getValue() + ") erases sources. MOVING");
									wLogger.debug("         Source: " + source);
									wLogger.debug("         Target: " + target);
									Files.move(source.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE);
								}
								locationsInHost = true;
								
							} catch (IOException ioe) {
								wLogger.error("IOException", ioe);
							}
						}

						if (!locationsInHost) {
							// We must transfer the file
							askTransfer = true;
						}
					} else {
						// Check if it is not currently transferred
						if (getDataRequests(param.getData().getName()) != null) {
							askTransfer = true;
						}
					}

					// Request the transfer if needed
					if (askTransfer) {
						wLogger.info("- Parameter " + i + "(" + (String) param.getValue() + ") does not exist, requesting data transfer");
						DataRequest dr = new WorkerDataRequest(tt, param.getType(), param.getData(), (String) param.getValue());
						addTransferRequest(dr);
					} else {
						// If no transfer, decrease the parameter counter (we already have it)
						wLogger.info("- Parameter " + i + "(" + (String) param.getValue() + ") already exists.");
						--tt.params;
					}
					break;

				/* OTHERS: Strings or basic types */
				default:
					// The master should have erased all these parameters so we
					// are never entering this section
					// In any case, there is nothing to do for these type of
					// parameters
					break;
				}
			} else {
				// OUT parameter. Has no associated data. Decrease the parameter
				// counter (we already have it)
				--tt.params;
			}
		}

        // Request the transfers
    	if (tracing) {
        		NIOTracer.emitEvent(tt.task.getTaskId(), NIOTracer.getTaskTransfersType());
   		}
    	requestTransfers();
    	if (tracing) {
        		NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getTaskTransfersType());
    	}

        if (tt.params == 0) {
                executeTask(tt.task);
        }
        if (tracing) {
        	NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.RECEIVED_NEW_TASK.getType());
        }
	}

	@Override
	protected void handleDataToSendNotAvailable(Connection c, Data d) {
		ErrorManager.warn("Data " + d.getName() + "in this worker " + this.getHostName() + " could not be sent to master.");
		c.finishConnection();
	}

	// This is called when the master couldn't send a data to the worker.
	// The master abruptly finishes the connection. The NIOMessageHandler
	// handles this as an error,
	// which treats with its function handleError, and notifies the worker in
	// this case.
	public void handleRequestedDataNotAvailableError(LinkedList<DataRequest> failedRequests, String dataId) {

		for (DataRequest dr : failedRequests) { // For every task pending on
												// this request, flag it as an
												// error
			WorkerDataRequest wdr = (WorkerDataRequest) dr;
			wdr.task.params--;

			// mark as an error task. When all the params've been consumed,
			// sendTaskDone unsuccessful
			wdr.task.error = true;
			if (wdr.task.params == 0) {
				sendTaskDone(wdr.task.task, false);
			}

			// Create job*_[NEW|RESUBMITTED|RESCHEDULED].[out|err]
			// If we don't create this when the task fails to retrieve a value,
			// the master will try to get the out of this job, and it will get
			// blocked.
			// Same for the worker when sending, throwing an error when trying
			// to read the job out, which wouldnt exist

			String baseJobPath = workingDir + File.separator + "jobs" + File.separator 
					+ "job" + wdr.task.task.getJobId() + "_" + wdr.task.task.getHist();
			File fout = new File(baseJobPath + ".out");
			File ferr = new File(baseJobPath + ".err");
			if (!fout.exists() || !ferr.exists()) {
				FileOutputStream fos = null;
				try {
					String errorMessage = "Worker closed because the data " + dataId + " couldn't be retrieved.";
					fos = new FileOutputStream(fout);
					fos.write(errorMessage.getBytes());
					fos.close();
					fos = new FileOutputStream(ferr);
					fos.write(errorMessage.getBytes());
					fos.close();
				} catch (IOException e) {
					wLogger.error("IOException", e);
				} finally {
					if (fos != null) {
						try {
							fos.close();
						} catch (IOException e) {
							wLogger.error("IOException", e);
						}
					}
				}
			}
		}

	}

	@Override
	public void receivedValue(Destination type, String dataId, Object object, LinkedList<DataRequest> achievedRequests) {
		if (type == Transfer.Destination.OBJECT) {
			wLogger.info("Received data " + dataId + " with associated object " + object);
			storeInCache(dataId, object);
		} else {
			wLogger.info("Received data " + dataId);
		}
		for (DataRequest dr : achievedRequests) {
			WorkerDataRequest wdr = (WorkerDataRequest) dr;
			wdr.task.params--;
			if (tracing) {
                		NIOTracer.emitDataTransferEvent(NIOTracer.TRANSFER_END);
            		}
			if (wdr.task.params == 0) {
				if (!wdr.task.error) {
					executeTask(wdr.task.task);
				} else {
					sendTaskDone(wdr.task.task, false);
				}
			}
		}
	}

	public void sendTaskDone(NIOTask nt, boolean successful) {
		int taskID = nt.getJobId();

		// Notify task done
		int retries = 0;
		Connection c = null;
		while (retries < MAX_RETRIES) {
			try {
				c = tm.startConnection(masterNode);
				break;
			} catch (Exception e) {
				if (retries >= MAX_RETRIES) {
					wLogger.error("Exception sending Task notification", e);
					return;
				} else {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e1) {
						// Nothing to do
					}
					retries++;
				}
			}
		}
		
		CommandTaskDone cmd;
        if (executionType.compareTo(ITConstants.COMPSs) == 0) {
        	cmd = new CommandTaskDone(this, taskID, successful);
        } else {
        	cmd = new CommandTaskDone(this, nt, successful);
        }        
        c.sendCommand(cmd);
		
		if (isWorkerDebugEnabled) {
			c.sendDataFile(workingDir + "/jobs/job" + nt.getJobId() + "_" + nt.getHist() + ".out");
			c.sendDataFile(workingDir + "/jobs/job" + nt.getJobId() + "_" + nt.getHist() + ".err");
		} else {
			if (!successful) {
				c.sendDataFile(workingDir + "/jobs/job" + nt.getJobId() + "_" + nt.getHist() + ".out");
				c.sendDataFile(workingDir + "/jobs/job" + nt.getJobId() + "_" + nt.getHist() + ".err");
			}
		}
		c.finishConnection();
	}

	// Check if this task is ready to execute
	private void executeTask(NIOTask task) {
		if (isWorkerDebugEnabled) {
			wLogger.debug("Enqueueing job " + task.getJobId() + " for execution.");
		}

		// Execute the job
		executionManager.enqueue(task);

		// Notify the master that the data has been transfered
		// The message is sent after the task enqueue because the connection can
		// have N pending task transfer and will wait until they
		// are finished to send all the answers (blocking the task execution)
		if (isWorkerDebugEnabled) {
			wLogger.debug("Notifying presence of all data for job " + task.getJobId() + ".");
		}

		CommandDataReceived cdr = new CommandDataReceived(this, task.getTransferGroupId());
		for (int retries = 0; retries < MAX_RETRIES; retries++) {
			if (tryNofiyDataReceived(cdr)) {
				return;
			} else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// Nothing to do
				}
			}
		}
	}

	private boolean tryNofiyDataReceived(CommandDataReceived cdr) {
		Connection c = tm.startConnection(masterNode);
		c.sendCommand(cdr);
		c.finishConnection();

		return true;
	}

	// Remove obsolete files and objects
	public void removeObsolete(LinkedList<String> obsolete) {
		try {
			for (String name : obsolete) {
				if (name.startsWith(File.separator)) {
					File f = new File(name);
					f.delete();
				} else {
					removeFromCache(name);
				}
			}
		} catch (Exception e) {
			wLogger.error("Exception", e);
		}
	}

	public void receivedUpdateSources(Connection c) {

	}

	// Shutdown the worker, at this point there are no active transfers
	public void shutdown(Connection closingConnection) {
		wLogger.debug("Entering shutdown method on worker");
		try {
			// Stop the Execution Manager
			executionManager.stop();
			
			// Stop the Data Manager
			dataManager.stop();

			// Finish the main thread
			if (closingConnection != null) {
				closingConnection.sendCommand(new CommandShutdownACK());
				closingConnection.finishConnection();
			}
			tm.shutdown(closingConnection);
			
			// End storage
			String storageConf = System.getProperty(ITConstants.IT_STORAGE_CONF);
			if ((storageConf != null) && (storageConf.compareTo("") != 0) && (storageConf.compareTo("null") != 0)) {
				try {
					StorageItf.finish();
				} catch (StorageException e) {
					wLogger.error("Error releasing storage library: " + e.getMessage());
				}
			}

			// Remove working Dir
            String wDir = workingDir.substring(0, workingDir.indexOf(deploymentId) + deploymentId.length());
            removeFolder(wDir);



		} catch (Exception e) {
			wLogger.error("Exception", e);
		}
		wLogger.debug("Finish shutdown method on worker");
	}

    private void removeFolder(String sandBox) throws IOException {

        File wdirFile = new File(sandBox);
        remove(wdirFile);
    }

    private void remove(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                for (File child : f.listFiles()) {
                    remove(child);
                }
            }
            Files.delete(f.toPath());
        }
    }

	public Object getObject(String s) throws SerializedObjectException {
		String realName = s.substring(s.lastIndexOf('/') + 1);
		return dataManager.get(realName);
	}

	public String getObjectAsFile(String s) {
		// This method should never be called in the worker side
		wLogger.warn("getObjectAsFile has been called in the worker side!");

		return null;
	}

	public void storeInCache(String name, Object value) {
		dataManager.store(name, value);
	}

	public void removeFromCache(String name) {
		dataManager.remove(name);
	}

	public static void registerOutputs(String path) {
		err.registerThread(path);
		out.registerThread(path);
	}

	public static void unregisterOutputs() {
		err.unregisterThread();
		out.unregisterThread();
	}

	@Override
	public void receivedTaskDone(Connection c, int jobID, NIOTask nt, boolean successful) {
		// Should not receive this call
	}

	@Override
	public void copiedData(int transfergroupID) {
		// Should not receive this call
	}

	@Override
	public void shutdownNotification(Connection c) {
		// Never orders the shutdown of a worker peer
	}

	public String getHostName() {
		return this.host;
	}

	@Override
	public void waitUntilTracingPackageGenerated() {
		// Nothing to do

	}

	@Override
	public void notifyTracingPackageGeneration() {
		// Nothing to do
	}

	@Override
	public void waitUntilWorkersDebugInfoGenerated() {
		// Nothing to do

	}

	@Override
	public void notifyWorkersDebugInfoGeneration() {
		// Nothing to do
	}

	@Override
	public void generateWorkersDebugInfo(Connection c) {
		// Freeze output
		String outSource = workingDir + File.separator + "log" + File.separator + "worker_" + host + ".out";
		String outTarget = workingDir + File.separator + "log" + File.separator + "static_" + "worker_" + host + ".out";
		if (new File(outSource).exists()) {
			try {
				Files.copy(new File(outSource).toPath(), new File(outTarget).toPath());
			} catch (Exception e) {
				wLogger.error("Exception", e);
			}
		} else {
			FileOutputStream fos = null;
			try {
				fos = new FileOutputStream(outTarget);
				fos.write("Empty file".getBytes());
				fos.close();
			} catch (Exception e) {
				wLogger.error("Exception", e);
			} finally {
				if (fos != null) {
					try {
						fos.close();
					} catch (Exception e) {
						wLogger.error("Exception", e);
					}
				}
			}
		}

		// Freeze error
		String errSource = workingDir + File.separator + "log" + File.separator + "worker_" + host + ".err";
		String errTarget = workingDir + File.separator + "log" + File.separator + "static_" + "worker_" + host + ".err";
		if (new File(errSource).exists()) {
			try {
				Files.copy(new File(errSource).toPath(), new File(errTarget).toPath());
			} catch (Exception e) {
				wLogger.error("Exception", e);
			}
		} else {
			FileOutputStream fos = null;
			try {
				fos = new FileOutputStream(errTarget);
				fos.write("Empty file".getBytes());
				fos.close();
			} catch (Exception e) {
				wLogger.error("Exception", e);
			} finally {
				if (fos != null) {
					try {
						fos.close();
					} catch (Exception e) {
						wLogger.error("Exception", e);
					}
				}
			}
		}

		// End
		c.sendCommand(new CommandWorkerDebugFilesDone());
		c.finishConnection();
	}
	
	private class WorkerDataRequest extends DataRequest {

		private final TransferringTask task;

		public WorkerDataRequest(TransferringTask task, DataType type, Data source, String target) {
			super(type, source, target);
			this.task = task;
		}

	}

	private static class TransferringTask {

		NIOTask task;
		int params;
		boolean error;

		public TransferringTask(NIOTask task) {
			this.task = task;
			params = task.getParams().size();
		}
	}
	
	public static void main(String[] args) {
		/* **************************************
		 * Configure logger
		 * **************************************/
		PropertyConfigurator.configure(System.getProperty(ITConstants.LOG4J));
		
		
		/* **************************************
		 * Get arguments
		 * **************************************/
		if (args.length != 19) {
			if (loggerDebug) {
				wLogger.debug("Received parameters: ");
				for (int i = 0; i < args.length; ++i) {
					wLogger.debug("Param " + i + ":  " + args[i]);
				}
			}
			
			ErrorManager.fatal(ERROR_INCORRECT_NUM_PARAMS);
		}
		
		isWorkerDebugEnabled = Boolean.valueOf(args[0]);
		
		int jobThreads = new Integer(args[1]);
		int maxSnd = new Integer(args[2]);
		int maxRcv = new Integer(args[3]);
		
		String workerIP = args[4];
		int wPort = new Integer(args[5]);
		int mPort = new Integer(args[6]);
		
		String appUuid = args[7];
		String lang = args[8];
		String workingDir = args[9];
		String installDir = args[10];
		String appDir = args[11];
		String libPath = args[12];
		String classpath = args[13];
		String pythonpath = args[14];
		
		String trace = args[15];
		String host = args[16];
		
		String storageConf = args[17];		
		executionType = args[18];
		
		
		/* **************************************
		 * Print args 
		 * **************************************/		
		if (isWorkerDebugEnabled) {
			wLogger.debug("jobThreads: " + String.valueOf(jobThreads));
			wLogger.debug("maxSnd: " + String.valueOf(maxSnd));
			wLogger.debug("maxRcv: " + String.valueOf(maxRcv));
			
			wLogger.debug("WorkerName: " + workerIP);
			wLogger.debug("WorkerPort: " + String.valueOf(wPort));
			wLogger.debug("MasterPort: " + String.valueOf(mPort));
			
			wLogger.debug("App uuid: " + appUuid);
			wLogger.debug("WorkingDir:" + workingDir);
			wLogger.debug("Install Dir: " + installDir);
			
			wLogger.debug("Tracing: " + trace);
			wLogger.debug("Host: " + host);
			
			wLogger.debug("LibraryPath: " + libPath);
			wLogger.debug("Classpath: " + classpath);
			wLogger.debug("Pythonpath: " + pythonpath);
			
			wLogger.debug("StorageConf: " + storageConf);
			wLogger.debug("executionType: " + executionType);
		}
		
		/* **************************************
		 * Configure Storage
		 * **************************************/
		System.setProperty(ITConstants.IT_STORAGE_CONF, storageConf);
		try {
			if (storageConf.compareTo("null") == 0) {
				wLogger.warn("No storage configuration file passed");
			} else {
				StorageItf.init(storageConf);
			}
		} catch (StorageException e) {
			ErrorManager.fatal("Error loading storage configuration file: " + storageConf, e);
		}

		/* **************************************
		 * Configure tracing
		 * **************************************/
		System.setProperty(ITConstants.IT_TRACING, trace);
		tracing = Integer.parseInt(trace) > 0;
		tracing_level = Integer.parseInt(trace);

        // Initialize tracing system
    	if (tracing){
        		NIOTracer.emitEvent(NIOTracer.Event.START.getId(), NIOTracer.Event.START.getType());

        		try {
            		tracingID = Integer.parseInt(host);
            		NIOTracer.setWorkerInfo(installDir , workerIP, workingDir, tracingID);
        		} catch (Exception e) {
            		wLogger.error("No valid hostID provided to the tracing system. Provided ID: " + host);
        		}
    	}

		
    	/* **************************************
		 * LAUNCH THE WORKER
		 * **************************************/    	
		NIOWorker nw = new NIOWorker(jobThreads, maxSnd, maxRcv, mPort, appUuid, lang, workerIP, workingDir, 
				installDir, appDir, libPath, classpath, pythonpath);
		
		NIOMessageHandler mh = new NIOMessageHandler(nw);

		// Initialize the Transfer Manager
		wLogger.debug("  Initializing the TransferManager structures...");
		try {
			tm.init(NIOEventManagerClass, null, mh);
		} catch (CommException ce) {
			wLogger.error("Error initializing Transfer Manager on worker " + nw.getHostName(), ce);
			// Shutdown the Worker since the error it is not recoverable
			nw.shutdown(null);
			return;
		}

		// Start the Transfer Manager thread (starts the EventManager)
		wLogger.debug("  Starting TransferManager Thread");
		tm.start();
		try {
			tm.startServer(new NIONode(null, wPort));
		} catch (CommException ce) {
			wLogger.error("Error starting TransferManager Server at Worker" + nw.getHostName(), ce);
			nw.shutdown(null);
			return;
		}

		if (tracing) {
			NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.START.getType());
       	}

		
		/* **************************************
		 * JOIN AND END
		 * **************************************/
		// Wait for the Transfer Manager thread to finish (the shutdown is received on that thread)
		try {
			tm.join();
		} catch (InterruptedException ie) {
			wLogger.warn("TransferManager interrupted", ie);
		}
	}

}
