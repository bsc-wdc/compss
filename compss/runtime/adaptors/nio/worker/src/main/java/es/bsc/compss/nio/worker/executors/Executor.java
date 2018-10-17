/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.nio.worker.executors;

import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.exceptions.JobExecutionException;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.components.ExecutionManager.BinderType;
import es.bsc.compss.nio.worker.util.JobsThreadPool;
import es.bsc.compss.util.RequestQueue;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.implementations.BinaryImplementation;
import es.bsc.compss.types.implementations.DecafImplementation;
import es.bsc.compss.types.implementations.MPIImplementation;
import es.bsc.compss.types.implementations.OmpSsImplementation;
import es.bsc.compss.types.implementations.OpenCLImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;


public abstract class Executor implements Runnable {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    protected static final boolean WORKER_DEBUG = LOGGER.isDebugEnabled();
    private static final String ERROR_OUT_FILES = "ERROR: One or more OUT files have not been created by task with Method Definition [";
    private static final String WARN_ATOMIC_MOVE = "WARN: AtomicMoveNotSupportedException. File cannot be atomically moved. Trying to move without atomic";

    // Attached component NIOWorker
    private final NIOWorker nw;
    
    private File sandBoxDir;
    // Attached component Jobs thread Pool
    protected final JobsThreadPool pool;
    // Attached component Request queue
    protected final RequestQueue<NIOTask> queue;


    public Executor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue) {
        LOGGER.info("Executor init");
        this.nw = nw;
        this.pool = pool;
        this.queue = queue;
    }

    /**
     * Thread main code which enables the request processing
     */
    public void run() {
        this.sandBoxDir = new File(this.nw.getWorkingDir()+ "sandbox_"+ Thread.currentThread().getId());
        if (!sandBoxDir.exists()) {
            try {
                Files.createDirectories(sandBoxDir.toPath());
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
                throw(new RuntimeException("Error creating computing thread sandbox",e.getCause()));
            }
        }
        
        start();
        
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
                LOGGER.debug("Dequeued job is null");
                break;
            }

            if (WORKER_DEBUG) {
                LOGGER.debug("Dequeuing job " + nt.getJobId());
            }
            boolean success = executeTask(nt);
            if (WORKER_DEBUG) {
                LOGGER.debug("Job " + nt.getJobId() + " finished (success: " + success + ")");
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
                LOGGER.error("Incorrect language " + nt.getLang() + " in job " + nt.getJobId());
                // Print to the job.err file
                System.err.println("Incorrect language " + nt.getLang() + " in job " + nt.getJobId());
                return false;
        }
    }

    public final boolean execute(NIOTask nt, NIOWorker nw) {
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.Event.TASK_RUNNING.getId(), NIOTracer.Event.TASK_RUNNING.getType());
        }

        // Sets the process environment variables (just in case its a MPI or OMPSs task)
        List<String> hostnames = nt.getSlaveWorkersNodeNames();
        hostnames.add(nw.getHostName());
        int numNodes = hostnames.size();
        int cus = nt.getResourceDescription().getTotalCPUComputingUnits();

        boolean firstElement = true;
        StringBuilder hostnamesSTR = new StringBuilder();
        for (Iterator<String> it = hostnames.iterator(); it.hasNext();) {
            String hostname = it.next();
            // Remove infiniband suffix
            if (hostname.endsWith("-ib0")) {
                hostname = hostname.substring(0, hostname.lastIndexOf("-ib0"));
            }

            // Add one host name per process to launch
            if (firstElement) {
                firstElement = false;
                hostnamesSTR.append(hostname);
                for (int i = 1; i < cus; ++i) {
                    hostnamesSTR.append(",").append(hostname);
                }
            } else {
                for (int i = 0; i < cus; ++i) {
                    hostnamesSTR.append(",").append(hostname);
                }
            }
        }

        setEnvironmentVariables(hostnamesSTR.toString(), numNodes, cus, nt.getResourceDescription());

        // Set outputs paths (Java will register them, ExternalExec will redirect processes outputs)
        String outputsBasename = nw.getWorkingDir() + "jobs" + File.separator + "job" + nt.getJobId() + "_" + nt.getHist();

        TaskWorkingDir twd = null;

        try {
            // Set the Task working directory
            long startCreate = System.currentTimeMillis();
            twd = createTaskSandbox(nt);
            long createDuration = System.currentTimeMillis()- startCreate;
            
            // Bind files to task sandbox working dir
            LOGGER.debug("Binding renamed files to sandboxed original names for Job " + nt.getJobId());
            long startSL = System.currentTimeMillis();
            bindOriginalFilenamesToRenames(nt, twd.getWorkingDir());
            long slDuration = System.currentTimeMillis()- startSL;
           

            // Bind Computing units
            int[] assignedCoreUnits = nw.getExecutionManager().bind(nt.getJobId(), nt.getResourceDescription().getTotalCPUComputingUnits(),
                    BinderType.CPU);
            int[] assignedGPUs = nw.getExecutionManager().bind(nt.getJobId(), nt.getResourceDescription().getTotalGPUComputingUnits(),
                    BinderType.GPU);
            int[] assignedFPGAs = nw.getExecutionManager().bind(nt.getJobId(), nt.getResourceDescription().getTotalFPGAComputingUnits(),
                    BinderType.FPGA);

            // Execute task
            LOGGER.debug("Executing Task of Job " + nt.getJobId());
            long startExec = System.currentTimeMillis();
            executeTask(nw, nt, outputsBasename, twd.getWorkingDir(), assignedCoreUnits, assignedGPUs, assignedFPGAs);
            long execDuration = System.currentTimeMillis()- startExec;
            // Unbind files from task sandbox working dir
            LOGGER.debug("Removing renamed files to sandboxed original names for Job " + nt.getJobId());
            long startOrig = System.currentTimeMillis();
            removeOriginalFilenames(nt);
            long origFileDuration = System.currentTimeMillis()- startOrig;
            LOGGER.info("[Profile] createSandBox: " + createDuration + " createSimLinks: " + slDuration + " execution" + execDuration + " restoreSimLinks: " + origFileDuration);
            // Check job output files
            LOGGER.debug("Checking generated files for Job " + nt.getJobId());
            checkJobFiles(nt);

            // Return
            return true;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            //Writing in the task .err/.out 
            System.out.println("Exception executing task " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            // Always clean the task sandbox working dir
            cleanTaskSandbox(twd);
            
            // Always release the binded computing units
            nw.getExecutionManager().release(nt.getJobId(), BinderType.CPU);
            nw.getExecutionManager().release(nt.getJobId(), BinderType.GPU);

            // Always end task tracing
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.TASK_RUNNING.getType());
            }
        }
    }

    /**
     * Creates a sandbox for a task
     * 
     * @param nt
     *            task description
     * @return Sandbox dir
     * @throws IOException
     * @throws Exception
     */
    private TaskWorkingDir createTaskSandbox(NIOTask nt) throws IOException {
        // Check if an specific working dir is provided
        String specificWD = null;
        switch (nt.getMethodType()) {
            case BINARY:
                BinaryImplementation binaryImpl = (BinaryImplementation) nt.getMethodImplementation();
                specificWD = binaryImpl.getWorkingDir();
                break;
            case MPI:
                MPIImplementation mpiImpl = (MPIImplementation) nt.getMethodImplementation();
                specificWD = mpiImpl.getWorkingDir();
                break;
            case DECAF:
                DecafImplementation decafImpl = (DecafImplementation) nt.getMethodImplementation();
                specificWD = decafImpl.getWorkingDir();
                break;
            case OMPSS:
                OmpSsImplementation ompssImpl = (OmpSsImplementation) nt.getMethodImplementation();
                specificWD = ompssImpl.getWorkingDir();
                break;
            case OPENCL:
                OpenCLImplementation openclImpl = (OpenCLImplementation) nt.getMethodImplementation();
                specificWD = openclImpl.getWorkingDir();
                break;
            case METHOD:
                specificWD = null;
                break;
        }

        TaskWorkingDir taskWD;
        if (specificWD != null && !specificWD.isEmpty() && !specificWD.equals(Constants.UNASSIGNED)) {
            // Binary has an specific working dir, set it
            File workingDir = new File(specificWD);
            taskWD = new TaskWorkingDir(workingDir, true);

            // Create structures
            Files.createDirectories(workingDir.toPath());
        } else {
            // No specific working dir provided, set default sandbox
            /* Change to create a sandbox per thread not per task
            String completePath = this.nw.getWorkingDir() + "sandBox" + File.separator + "job_" + nt.getJobId();
            File workingDir = new File(completePath);
            taskWD = new TaskWorkingDir(workingDir, false);
            
            // Clean-up previous versions if any
            if (workingDir.exists()) {
                LOGGER.debug("Deleting folder " + workingDir.toString());
                if (!workingDir.delete()) {
                    LOGGER.warn("Cannot delete working dir folder: " + workingDir.toString());
                }
            }

            // Create structures
            if (!workingDir.exists()) {
                Files.createDirectories(workingDir.toPath());
            }
            */
            taskWD = new TaskWorkingDir(sandBoxDir, true);
        }
        return taskWD;
    }

    private void cleanTaskSandbox(TaskWorkingDir twd) {
        if (twd != null && !twd.isSpecific()) {
            // Only clean task sandbox if it is not specific
            File workingDir = twd.getWorkingDir();
            if (workingDir != null && workingDir.exists() && workingDir.isDirectory()) {
                try {
                    LOGGER.debug("Deleting sandbox " + workingDir.toPath());
                    FileUtils.deleteDirectory(workingDir);
                } catch (IOException e) {
                    LOGGER.warn("Error deleting sandbox " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Check whether file1 corresponds to a file with a higher version than file2
     * 
     * @param file1
     *            first file name
     * @param file2
     *            second file name
     * @return True if file1 has a higher version. False otherwise (This includes the case where the name file's format
     *         is not correct)
     */
    private boolean isMajorVersion(String file1, String file2) {
        String[] version1array = file1.split("_")[0].split("v");
        String[] version2array = file2.split("_")[0].split("v");
        if (version1array.length < 2 || version2array.length < 2) {
            return false;
        }
        Integer version1int = null;
        Integer version2int = null;
        try {
            version1int = Integer.parseInt(version1array[1]);
            version2int = Integer.parseInt(version2array[1]);
        } catch (NumberFormatException e) {
            return false;
        }
        if (version1int > version2int) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Create symbolic links from files with the original name in task sandbox to the renamed file
     * 
     * @param nt
     *            task description
     * @param sandbox
     *            created sandbox
     * @throws IOException
     * @throws Exception
     *             returns exception is a problem occurs during creation
     */
    private void bindOriginalFilenamesToRenames(NIOTask nt, File sandbox) throws IOException {
        for (NIOParam param : nt.getParams()) {
            if (param.getType().equals(DataType.FILE_T)) {
                String renamedFilePath = (String) param.getValue();
                File renamedFile = new File(renamedFilePath);

                if (renamedFile.getName().equals(param.getOriginalName())) {
                    param.setOriginalName(renamedFilePath);
                } else {
                    String newOrigFilePath = sandbox.getAbsolutePath() + File.separator + param.getOriginalName();
                    LOGGER.debug("Setting Original Name to " + newOrigFilePath);
                    param.setOriginalName(newOrigFilePath);
                    File newOrigFile = new File(newOrigFilePath);
                    if (renamedFile.exists()) {
                        // IN or INOUT File creating a symbolic link
                        if (!newOrigFile.exists()) {
                            LOGGER.debug("Creating symlink " + newOrigFile.toPath() + " pointing to " + renamedFile.toPath());
                            Files.createSymbolicLink(newOrigFile.toPath(), renamedFile.toPath());
                        } else {
                            if (Files.isSymbolicLink(newOrigFile.toPath())) {
                                Path oldRenamed = Files.readSymbolicLink(newOrigFile.toPath());
                                LOGGER.debug(
                                        "Checking if " + renamedFile.getName() + " is equal to " + oldRenamed.getFileName().toString());
                                if (isMajorVersion(renamedFile.getName(), oldRenamed.getFileName().toString())) {
                                    Files.delete(newOrigFile.toPath());
                                    Files.createSymbolicLink(newOrigFile.toPath(), renamedFile.toPath());
                                }
                            }
                        }
                    }
                }
            }
        }

    }

    /**
     * Undo symbolic links and renames done with the original names in task sandbox to the renamed file
     * 
     * @param nt
     *            task description
     * @throws IOException
     * @throws JobExecutionException
     * @throws Exception
     *             returns exception is an unexpected case is found.
     */
    private void removeOriginalFilenames(NIOTask nt) throws IOException, JobExecutionException {
        for (NIOParam param : nt.getParams()) {
            if (param.getType().equals(DataType.FILE_T)) {
                String renamedFilePath = (String) param.getValue();
                String newOriginalFilePath = param.getOriginalName();
                LOGGER.debug("Treating file " + renamedFilePath);

                if (!renamedFilePath.equals(newOriginalFilePath)) {
                    File newOrigFile = new File(newOriginalFilePath);
                    File renamedFile = new File(renamedFilePath);

                    if (renamedFile.exists()) {
                        // IN, INOUT
                        if (newOrigFile.exists()) {
                            if (Files.isSymbolicLink(newOrigFile.toPath())) {
                                // If a symbolic link is created remove it
                                LOGGER.debug("Deleting symlink " + newOrigFile.toPath());
                                Files.delete(newOrigFile.toPath());
                            } else {
                                // Rewrite inout param by moving the new file to the renaming
                                move(newOrigFile.toPath(), renamedFile.toPath());
                            }
                        } else {
                            // Both files exist and are updated
                            LOGGER.debug("Repeated data for " + renamedFilePath + ". Nothing to do");
                        }
                    } else {
                        // OUT
                        if (newOrigFile.exists()) {
                            if (Files.isSymbolicLink(newOrigFile.toPath())) {
                                // Unexpected case
                                String msg = "ERROR: Unexpected case. A Problem occurred with File " + renamedFilePath
                                        + ". Either this file or the original name " + newOriginalFilePath + " do not exist.";
                                LOGGER.error(msg);
                                System.err.println(msg);
                                throw new JobExecutionException(msg);
                            } else {
                                // If an output file is created move to the renamed path (OUT Case)
                                move(newOrigFile.toPath(), renamedFile.toPath());
                            }
                        } else {
                            // Error output file does not exist
                            String msg = "ERROR: Output file " + newOriginalFilePath + " does not exist";
                            // Unexpected case (except for C binding when not serializing outputs)
                            if (Lang.valueOf(nt.getLang().toUpperCase()) != Lang.C) {
                            	LOGGER.error(msg);
                            	System.err.println(msg);
                            	throw new JobExecutionException(msg);
                            }
                        }
                    }
                }
            }
        }
    }

    private void move(Path origFilePath, Path renamedFilePath) throws IOException {
        if (WORKER_DEBUG){
                LOGGER.debug("Moving " + origFilePath.toString() + " to " + renamedFilePath.toString());
        }
        try {
            Files.move(origFilePath, renamedFilePath, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException amnse) {
            LOGGER.warn(WARN_ATOMIC_MOVE);
            Files.move(origFilePath, renamedFilePath);
        }
    }

    private void checkJobFiles(NIOTask nt) throws JobExecutionException {
        // Check if all the output files have been actually created (in case user has forgotten)
        // No need to distinguish between IN or OUT files, because IN files will exist, and
        // if there's one or more missing, they will be necessarily out.
    	boolean allOutFilesCreated = true;
        for (NIOParam param : nt.getParams()) {
            if (param.getType().equals(DataType.FILE_T)) {
                String filepath = (String) param.getValue();
                File f = new File(filepath);
                //If using C binding we ignore potential errors
                if (!f.exists() && (Lang.valueOf(nt.getLang().toUpperCase()) != Lang.C)) {
                    StringBuilder errMsg = new StringBuilder();
                    errMsg.append("ERROR: File with path '").append(filepath);
                    errMsg.append("' not generated by task with Method Definition ")
                            .append(nt.getMethodImplementation().getMethodDefinition());
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

    public abstract void executeTask(NIOWorker nw, NIOTask nt, String outputsBasename, File taskSandboxWorkingDir, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs) throws Exception;

    public abstract void finish();
    
    public abstract void start();


    private class TaskWorkingDir {

        private final File workingDir;
        private final boolean isSpecific;


        public TaskWorkingDir(File workingDir, boolean isSpecific) {
            this.workingDir = workingDir;
            this.isSpecific = isSpecific;
        }

        public File getWorkingDir() {
            return this.workingDir;
        }

        public boolean isSpecific() {
            return this.isSpecific;
        }
    }

}
