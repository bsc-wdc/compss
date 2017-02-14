package integratedtoolkit.nio.worker.executors;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.ITConstants.Lang;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOParam;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.exceptions.JobExecutionException;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.util.RequestQueue;
import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.resources.MethodResourceDescription;


public abstract class Executor implements Runnable {

    protected static final Logger logger = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    protected static final boolean workerDebug = logger.isDebugEnabled();
    private static final String ERROR_OUT_FILES = "ERROR: One or more OUT files have not been created by task with Method Definition [";

    // Attached component NIOWorker
    private final NIOWorker nw;
    // Attached component Jobs thread Pool
    protected final JobsThreadPool pool;
    // Attached component Request queue
    protected final RequestQueue<NIOTask> queue;


    public Executor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue) {
        logger.info("Executor init");
        this.nw = nw;
        this.pool = pool;
        this.queue = queue;
    }

    /**
     * Thread main code which enables the request processing
     */
    public void run() {
        // Main loop to process requests
        processRequests();

        // Close language specific properties
        finish();

        // Notify pool of thread end
        if (pool != null) {
            pool.threadEnd();
        }
    }

    private void processRequests() {
        NIOTask nt;
        while (true) {
            nt = queue.dequeue(); // Get tasks until there are no more tasks pending

            if (nt == null) {
                logger.debug("Dequeued job is null");
                break;
            }

            if (workerDebug) {
                logger.debug("Dequeuing job " + nt.getJobId());
            }

            boolean success = executeTask(nt);

            if (workerDebug) {
                logger.debug("Job " + nt.getJobId() + " finished (success: " + success + ")");
            }

            nw.sendTaskDone(nt, success);
        }
    }

    private boolean executeTask(NIOTask nt) {
        switch (Lang.valueOf(nt.getLang().toUpperCase())) {
            case JAVA:
            case PYTHON:
            case C:
                return execute(nt, nw);
            default:
                logger.error("Incorrect language " + nt.getLang() + " in job " + nt.getJobId());
                // Print to the job.err file
                System.err.println("Incorrect language " + nt.getLang() + " in job " + nt.getJobId());
                return false;
        }
    }

    public final boolean execute(NIOTask nt, NIOWorker nw) {
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.Event.TASK_RUNNING.getId(), NIOTracer.Event.TASK_RUNNING.getType());
        }

        String workingDir = nw.getWorkingDir();

        // Set outputs paths (Java will register them, ExternalExec will redirect processes outputs)
        String outputsBasename = workingDir + "jobs" + File.separator + "job" + nt.getJobId() + "_" + nt.getHist();

        // Sets the process environment variables (just in case its a MPI or OMPSs task)
        List<String> slaveWorkersHostnames = nt.getSlaveWorkersNodeNames();
        
        Set<String> hostnames = new HashSet<String>();
        hostnames.add(nw.getHostName());
        hostnames.addAll(nt.getSlaveWorkersNodeNames());
        boolean firstElement = true;
        StringBuilder hostnamesSTR = new StringBuilder();
        for (Iterator<String> it = hostnames.iterator(); it.hasNext(); ) {
            String hostname = it.next();
            if (firstElement) {
                firstElement = false;
                hostnamesSTR.append(hostname);
            } else {
                hostnamesSTR.append(",").append(hostname);
            }
        }

        int numNodes = slaveWorkersHostnames.size() + 1;
        int cus = nt.getResourceDescription().getTotalCPUComputingUnits();
        
        setEnvironmentVariables(hostnamesSTR.toString(), numNodes, cus, nt.getResourceDescription());

        // Execute task
        File sandbox = null;
        try {
        	sandbox = createSandBox(nt);
            int[] assignedCoreUnits = nw.bindCoreUnits(nt.getJobId(), nt.getResourceDescription().getTotalCPUComputingUnits());
            int[] assignedGPUs = nw.bindGPUs(nt.getJobId(), nt.getResourceDescription().getTotalGPUComputingUnits());
            logger.debug("Binding renamed files to sandboxed original names for Job "+ nt.getJobId() );
            bindOriginalFilenamesToRenames(nt, sandbox);
            logger.debug("Executing Task of Job "+ nt.getJobId());
            executeTask(nw, nt, outputsBasename, assignedCoreUnits, assignedGPUs);
            logger.debug("Removing renamed files to sandboxed original names for Job "+ nt.getJobId() );
            removeOriginalFilenames(nt);
            logger.debug("Checking generated files for Job "+ nt.getJobId() );
            checkJobFiles(nt);
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        } finally {
        	if (sandbox!= null && sandbox.exists()){
        		try {
					Files.deleteIfExists(sandbox.toPath());
				} catch (IOException e) {
					logger.warn(" Error deleting sandbox" + e.getMessage());
				}
        	}
            nw.releaseCoreUnits(nt.getJobId());
            nw.releaseGPUs(nt.getJobId());
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.TASK_RUNNING.getType());
            }
        }
    }
    
    /** Creates a sandbox for a task
     * @param nt task description
     * @return Sandbox dir
     * @throws Exception
     */
    private File createSandBox(NIOTask nt) throws Exception {
    	String workingDir = nw.getWorkingDir();
		File sandboxBasePath = new File(workingDir + "sandBox" + File.separator + "job_" + nt.getJobId());
		
		if (sandboxBasePath.exists()){
			sandboxBasePath.delete();
		}
    	Files.createDirectories(sandboxBasePath.toPath());
    	return sandboxBasePath;
    }
    
	/** Create symbolic links from files with the original name in task sandbox to the renamed file
	 * @param nt task description
	 * @param sandbox created sandbox
	 * @throws Exception returns exception is a problem occurs during creation
	 */
	private void bindOriginalFilenamesToRenames(NIOTask nt, File sandbox) throws Exception{
    	for (NIOParam param: nt.getParams()) {
            if (param.getType().equals(DataType.FILE_T)){
            	String renamedFilePath = (String)param.getValue();
            	File renamedFile = new File(renamedFilePath);
            	if (renamedFile.getName().equals(param.getOriginalName())){
            			param.setOriginalName(renamedFilePath);
            	}else{
            		String newOrigFilePath = sandbox.getAbsolutePath()+File.separator+param.getOriginalName();
            		logger.debug("Setting Original Name to " + newOrigFilePath);
            		param.setOriginalName(newOrigFilePath);
            		File newOrigFile = new File(newOrigFilePath);
            		if (renamedFile.exists()){
            			//IN or INOUT File creating a simbolinc link
            			logger.debug("Creating symlink" + newOrigFile.toPath() + " pointing to " + renamedFile.toPath());
            			Files.createSymbolicLink(newOrigFile.toPath(), renamedFile.toPath());
            		}
            	}
            }
    	}
    	
	}
	/** Undo symbolic links and renames done with the original names in task sandbox to the renamed file
	 * @param nt task description
	 * @throws Exception returns exception is an unexpected case is found.
	 */
	private void removeOriginalFilenames(NIOTask nt) throws Exception{
    	for (NIOParam param: nt.getParams()) {
            if (param.getType().equals(DataType.FILE_T)){
            	
            	String renamedFilePath = (String)param.getValue();
            	String newOriginalFilePath = param.getOriginalName();
            	logger.debug("Treating file " + renamedFilePath );
            	if (!renamedFilePath.equals(newOriginalFilePath)){
            		File newOrigFile = new File(newOriginalFilePath);
            		File renamedFile = new File(renamedFilePath);
            		if (renamedFile.exists() && Files.isSymbolicLink(newOrigFile.toPath())){
            			//If a symbolic link is created remove it (IN INOUT)
            			logger.debug("Deleting symlink" + newOrigFile.toPath());
            			Files.delete(newOrigFile.toPath());
            		}else if (!renamedFile.exists() && newOrigFile.exists() && !Files.isSymbolicLink(newOrigFile.toPath())){
            			//If an output file is created move to the renamed path (OUT Case)
            			logger.debug("Moving "+ newOrigFile.toPath().toString() + " to " + renamedFile.toPath().toString());
            			Files.move(newOrigFile.toPath(), renamedFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
            		}else {
            			//Unexpected case
            			logger.error("Unexpected case: A Problem occurred with File " + renamedFilePath +
            				". Either this file or the original name " + newOriginalFilePath+ " do not exist.");
            			System.err.println("Unexpected case: A Problem occurred with File " + renamedFilePath +
            				". Either this file or the original name " + newOriginalFilePath+ " do not exist.");
            			throw new JobExecutionException("A Problem occurred with File " + renamedFilePath +
            				". Either this file or the original name " + newOriginalFilePath+ " do not exist.");
            		}
            	}
            }
    	}		
	}
    

	private void checkJobFiles(NIOTask nt) throws JobExecutionException {
        // Check if all the output files have been actually created (in case user has forgotten)
        // No need to distinguish between IN or OUT files, because IN files will exist, and
        // if there's one or more missing, they will be necessarily out.
        boolean allOutFilesCreated = true;
        for (NIOParam param: nt.getParams()) {
            if (param.getType().equals(DataType.FILE_T)){
                String filepath = (String) param.getValue();
                File f = new File(filepath);
                if (!f.exists()) {
                    StringBuilder errMsg = new StringBuilder();
                    errMsg.append("ERROR: File with path '").append(filepath);
                    errMsg.append("' not generated by task with Method Definition ").append(nt.getMethodImplementation().getMethodDefinition());
                    System.out.println(errMsg.toString());
                    System.err.println(errMsg.toString());
                    allOutFilesCreated = false;
                }
            }
        }

        if (!allOutFilesCreated) {
            throw new JobExecutionException(ERROR_OUT_FILES + nt.getMethodImplementation().getMethodDefinition());
        }
    }
    

    public abstract void setEnvironmentVariables(String hostnames, int numNodes, int cus, MethodResourceDescription reqs);

    public abstract void executeTask(NIOWorker nw, NIOTask nt, String outputsBasename, int[] assignedCoreUnits, int[] assignedGPUs) throws Exception;

    public abstract void finish();

}
