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
package es.bsc.compss.executor;

import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.utils.ExecutionPlatformMirror;
import es.bsc.compss.executor.utils.PipedMirror;
import es.bsc.compss.executor.utils.PipedMirror.PipePair;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.JavaInvoker;
import es.bsc.compss.invokers.OpenCLInvoker;
import es.bsc.compss.invokers.StorageInvoker;
import es.bsc.compss.invokers.binary.BinaryInvoker;
import es.bsc.compss.invokers.binary.DecafInvoker;
import es.bsc.compss.invokers.binary.MPIInvoker;
import es.bsc.compss.invokers.binary.OmpSsInvoker;
import es.bsc.compss.invokers.external.piped.CInvoker;
import es.bsc.compss.invokers.external.persistent.CPersistentInvoker;
import es.bsc.compss.invokers.external.piped.PythonInvoker;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableComputingUnitsException;
import es.bsc.compss.types.implementations.BinaryImplementation;
import es.bsc.compss.types.implementations.DecafImplementation;
import es.bsc.compss.types.implementations.MPIImplementation;
import es.bsc.compss.types.implementations.OmpSsImplementation;
import es.bsc.compss.types.implementations.OpenCLImplementation;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.util.Tracer;
import java.io.PrintStream;


public class Executor implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    private static final boolean WORKER_DEBUG = LOGGER.isDebugEnabled();

    private static final String ERROR_OUT_FILES = "ERROR: One or more OUT files have not been created by task with Method Definition [";
    private static final String WARN_ATOMIC_MOVE = "WARN: AtomicMoveNotSupportedException. File cannot be atomically moved. Trying to move without atomic";

    // Attached component NIOWorker
    private final InvocationContext context;
    // Attached component Request queue
    protected final ExecutorsContext platform;
    //Executor Id
    protected final String id;

    protected PipePair cPipes;
    protected PipePair pyPipes;

    public Executor(InvocationContext context, ExecutorsContext platform, String executorId) {
        LOGGER.info("Executor init");
        this.context = context;
        this.platform = platform;
        this.id = executorId;
    }

    public String getId() {
        return this.id;
    }

    public void start() {
        // Nothing to do since everything is deleted in each task execution
        LOGGER.info("Executor started");
    }

    /**
     * Thread main code which enables the request processing
     */
    @Override
    public void run() {
        start();

        // Main loop to process requests
        processRequests();

        // Close language specific properties
        finish();

    }

    public void finish() {
        // Nothing to do since everything is deleted in each task execution
        LOGGER.info("Executor finished");
    }

    private void processRequests() {
        Execution execution;
        while (true) {
            execution = platform.getJob(); // Get tasks until there are no more tasks pending

            if (execution == null) {
                LOGGER.debug("Dequeued job is null");
                break;
            }
            Invocation invocation = execution.getInvocation();

            if (WORKER_DEBUG) {
                LOGGER.debug("Dequeuing job " + invocation.getJobId());
            }

            boolean success = executeTask(invocation);

            if (WORKER_DEBUG) {
                LOGGER.debug("Job " + invocation.getJobId() + " finished (success: " + success + ")");
            }

            execution.notifyEnd(success);
        }
    }

    private boolean executeTask(Invocation invocation) {
        switch (invocation.getLang()) {
            case JAVA:
            case PYTHON:
            case C:
                return execute(invocation, context);
            default:
                LOGGER.error("Incorrect language " + invocation.getLang() + " in job " + invocation.getJobId());
                // Print to the job.err file
                System.err.println("Incorrect language " + invocation.getLang() + " in job " + invocation.getJobId());
                return false;
        }
    }

    private boolean execute(Invocation invocation, InvocationContext context) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.TASK_RUNNING.getId(), Tracer.Event.TASK_RUNNING.getType());
        }

        TaskWorkingDir twd = null;

        try {
            // Set the Task working directory
            twd = createTaskSandbox(invocation);

            // Bind files to task sandbox working dir
            LOGGER.debug("Binding renamed files to sandboxed original names for Job " + invocation.getJobId());
            bindOriginalFilenamesToRenames(invocation, twd.getWorkingDir());

            // Bind Computing units
            InvocationResources assignedResources = platform.acquireComputingUnits(invocation.getJobId(), invocation.getRequirements());

            // Execute task
            LOGGER.debug("Executing Task of Job " + invocation.getJobId());
            executeTask(context, assignedResources, invocation, twd.getWorkingDir());

            // Unbind files from task sandbox working dir
            LOGGER.debug("Removing renamed files to sandboxed original names for Job " + invocation.getJobId());
            removeOriginalFilenames(invocation);

            // Check job output files
            LOGGER.debug("Checking generated files for Job " + invocation.getJobId());
            checkJobFiles(invocation);

            // Return
            return true;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            //Writing in the task .err/.out 
            context.getThreadOutStream().println("Exception executing task " + e.getMessage());
            e.printStackTrace(context.getThreadErrStream());
            return false;
        } finally {
            // Always clean the task sandbox working dir
            cleanTaskSandbox(twd);
            // Always release the binded computing units
            platform.releaseComputingUnits(invocation.getJobId());

            // Always end task tracing
            if (Tracer.isActivated()) {
                Tracer.emitEvent(Tracer.EVENT_END, Tracer.Event.TASK_RUNNING.getType());
            }
        }
    }

    /**
     * Creates a sandbox for a task
     *
     * @param invocation task description
     * @return Sandbox dir
     * @throws IOException
     * @throws Exception
     */
    private TaskWorkingDir createTaskSandbox(Invocation invocation) throws IOException {
        // Check if an specific working dir is provided
        String specificWD = null;
        switch (invocation.getMethodImplementation().getMethodType()) {
            case BINARY:
                BinaryImplementation binaryImpl = (BinaryImplementation) invocation.getMethodImplementation();
                specificWD = binaryImpl.getWorkingDir();
                break;
            case MPI:
                MPIImplementation mpiImpl = (MPIImplementation) invocation.getMethodImplementation();
                specificWD = mpiImpl.getWorkingDir();
                break;
            case DECAF:
                DecafImplementation decafImpl = (DecafImplementation) invocation.getMethodImplementation();
                specificWD = decafImpl.getWorkingDir();
                break;
            case OMPSS:
                OmpSsImplementation ompssImpl = (OmpSsImplementation) invocation.getMethodImplementation();
                specificWD = ompssImpl.getWorkingDir();
                break;
            case OPENCL:
                OpenCLImplementation openclImpl = (OpenCLImplementation) invocation.getMethodImplementation();
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
            String completePath = this.context.getWorkingDir() + "sandBox" + File.separator + "job_" + invocation.getJobId();
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
            Files.createDirectories(workingDir.toPath());
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
     * @param file1 first file name
     * @param file2 second file name
     * @return True if file1 has a higher version. False otherwise (This includes the case where the name file's format
     * is not correct)
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
     * @param invocation task description
     * @param sandbox created sandbox
     * @throws IOException
     * @throws Exception returns exception is a problem occurs during creation
     */
    private void bindOriginalFilenamesToRenames(Invocation invocation, File sandbox) throws IOException {
        for (InvocationParam param : invocation.getParams()) {
            bindOriginalFilenameToRenames(param, sandbox);
        }
        if (invocation.getTarget() != null) {
            bindOriginalFilenameToRenames(invocation.getTarget(), sandbox);
        }
        for (InvocationParam param : invocation.getResults()) {
            bindOriginalFilenameToRenames(param, sandbox);
        }
    }

    private void bindOriginalFilenameToRenames(InvocationParam param, File sandbox) throws IOException {
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
                    } else if (Files.isSymbolicLink(newOrigFile.toPath())) {
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

    /**
     * Undo symbolic links and renames done with the original names in task sandbox to the renamed file
     *
     * @param invocation task description
     * @throws IOException
     * @throws JobExecutionException
     * @throws Exception returns exception is an unexpected case is found.
     */
    private void removeOriginalFilenames(Invocation invocation) throws IOException, JobExecutionException {
        for (InvocationParam param : invocation.getParams()) {
            removeOriginalFilename(param, invocation.getLang());
        }
        if (invocation.getTarget() != null) {
            removeOriginalFilename(invocation.getTarget(), invocation.getLang());
        }
        for (InvocationParam param : invocation.getResults()) {
            removeOriginalFilename(param, invocation.getLang());
        }
    }

    private void removeOriginalFilename(InvocationParam param, Lang lang) throws IOException, JobExecutionException {
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
                } else { // OUT
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
                        if (lang != Lang.C) {
                            LOGGER.error(msg);
                            System.err.println(msg);
                            throw new JobExecutionException(msg);
                        }
                    }
                }
            }
        }
    }

    private void move(Path origFilePath, Path renamedFilePath) throws IOException {
        LOGGER.debug("Moving " + origFilePath.toString() + " to " + renamedFilePath.toString());
        try {
            Files.move(origFilePath, renamedFilePath, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException amnse) {
            LOGGER.warn(WARN_ATOMIC_MOVE);
            Files.move(origFilePath, renamedFilePath);
        }
    }

    private void checkJobFiles(Invocation invocation) throws JobExecutionException {
        // Check if all the output files have been actually created (in case user has forgotten)
        // No need to distinguish between IN or OUT files, because IN files will exist, and
        // if there's one or more missing, they will be necessarily out.
        boolean allOutFilesCreated = true;
        for (InvocationParam param : invocation.getParams()) {
            if (param.getType().equals(DataType.FILE_T)) {
                String filepath = (String) param.getValue();
                File f = new File(filepath);
                //If using C binding we ignore potential errors
                if (!f.exists() && (invocation.getLang() != Lang.C)) {
                    StringBuilder errMsg = new StringBuilder();
                    errMsg.append("ERROR: File with path '").append(filepath);
                    errMsg.append("' not generated by task with Method Definition ")
                            .append(invocation.getMethodImplementation().getMethodDefinition());
                    System.out.println(errMsg.toString());
                    System.err.println(errMsg.toString());
                    allOutFilesCreated = false;
                }
            }
        }
        if (!allOutFilesCreated) {
            throw new JobExecutionException(ERROR_OUT_FILES + invocation.getMethodImplementation().getMethodDefinition());
        }

    }

    private void executeTask(
            InvocationContext context,
            InvocationResources assignedResources,
            Invocation invocation,
            File taskSandboxWorkingDir
    ) throws JobExecutionException {
        /* Register outputs **************************************** */
        String streamsPath = context.getStandardStreamsPath(invocation);
        context.registerOutputs(streamsPath);
        PrintStream out = context.getThreadOutStream();

        /* TRY TO PROCESS THE TASK ******************************** */
        if (invocation.isDebugEnabled()) {
            out.println("[EXECUTOR] executeTask - Begin task execution");
        }
        try {
            Invoker invoker = null;
            switch (invocation.getMethodImplementation().getMethodType()) {
                case METHOD:
                    switch (invocation.getLang()) {
                        case JAVA:
                            switch (context.getExecutionType()) {
                                case COMPSS:
                                    invoker = new JavaInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                                    break;
                                case STORAGE:
                                    invoker = new StorageInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                                    break;
                                default:
                            }
                            break;
                        case PYTHON:
                            if (pyPipes == null) {
                                //Double checking to avoid syncrhonizations when the pipes are already defined
                                synchronized (PythonInvoker.class) {
                                    if (pyPipes == null) {
                                        PipedMirror mirror = (PipedMirror) platform.getMirror(PythonInvoker.class);
                                        if (mirror == null) {
                                            mirror = PythonInvoker.getMirror(context, platform);
                                            platform.registerMirror(PythonInvoker.class, mirror);
                                        }
                                        pyPipes = mirror.getPipes(id);
                                    }
                                }
                            }
                            invoker = new PythonInvoker(context, invocation, taskSandboxWorkingDir, assignedResources, pyPipes);
                            break;
                        case C:
                            if (context.isPersistentEnabled()) {
                                invoker = new CPersistentInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                            } else {
                                if (cPipes == null) {
                                    //Double checking to avoid syncrhonizations when the pipes are already defined
                                    synchronized (CInvoker.class) {
                                        if (cPipes == null) {
                                            PipedMirror mirror = (PipedMirror) platform.getMirror(CInvoker.class);
                                            if (mirror == null) {
                                                mirror = (PipedMirror) CInvoker.getMirror(context, platform);
                                                platform.registerMirror(CInvoker.class, mirror);
                                            }
                                            cPipes = mirror.getPipes(id);
                                        }
                                    }
                                }
                                invoker = new CInvoker(context, invocation, taskSandboxWorkingDir, assignedResources, cPipes);
                            }
                            break;
                        default:
                            throw new JobExecutionException("Unrecognised lang for a method type invocation");
                    }
                    break;

                case MPI:
                    invoker = new MPIInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case DECAF:
                    invoker = new DecafInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case OMPSS:
                    invoker = new OmpSsInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case OPENCL:
                    invoker = new OpenCLInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case BINARY:
                    invoker = new BinaryInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                default:
                    throw new JobExecutionException("Unrecognised method type");
            }
            invoker.processTask();
        } catch (Exception jee) {
            out.println("[EXECUTOR] executeTask - Error in task execution");
            PrintStream err = context.getThreadErrStream();
            err.println("[EXECUTOR] executeTask - Error in task execution");
            jee.printStackTrace(err);
            throw jee;
        } finally {
            if (invocation.isDebugEnabled()) {
                out.println("[EXECUTOR] executeTask - End task execution");
            }
            context.unregisterOutputs();
        }
    }


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


    public static interface ExecutorsContext {

        public ExecutionPlatformMirror getMirror(Class<?> invoker);

        public void registerMirror(Class<?> invoker, ExecutionPlatformMirror mirror);

        public int getSize();

        public Execution getJob();

        public InvocationResources acquireComputingUnits(int jobId, ResourceDescription requirements) throws UnsufficientAvailableComputingUnitsException;

        public void releaseComputingUnits(int jobId);
    }
}
