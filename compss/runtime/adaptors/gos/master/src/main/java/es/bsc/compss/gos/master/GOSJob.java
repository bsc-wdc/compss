/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.gos.master;

import com.jcraft.jsch.JSchException;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.gos.master.configuration.GOSConfiguration;
import es.bsc.compss.gos.master.monitoring.GOSMonitoring;
import es.bsc.compss.gos.master.sshutils.SSHChannel;
import es.bsc.compss.gos.master.sshutils.SSHHost;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.exceptions.LangNotDefinedException;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.implementations.definition.BinaryDefinition;
import es.bsc.compss.types.implementations.definition.COMPSsDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDefinition;
import es.bsc.compss.types.implementations.definition.DecafDefinition;
import es.bsc.compss.types.implementations.definition.MPIDefinition;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.implementations.definition.MultiNodeDefinition;
import es.bsc.compss.types.implementations.definition.OmpSsDefinition;
import es.bsc.compss.types.implementations.definition.OpenCLDefinition;
import es.bsc.compss.types.implementations.definition.PythonMPIDefinition;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.job.JobImpl;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;


/**
 * Representation of a Job execution for COMPSs with GOS Adaptor.
 */
public class GOSJob extends JobImpl<GOSWorkerNode> {

    // Worker script path
    public static final String SCRIPT_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system"
        + File.separator + "adaptors" + File.separator + "gos" + File.separator;

    private static final String WORKER_SCRIPT_NAME_BATCH = "queues" + File.separator + "submitBatch.sh";

    private static final String WORKER_SCRIPT_NAME_INTERACTIVE = "worker.sh";

    public static final String JOBS_DIR = System.getProperty(COMPSsConstants.LOG_DIR) + "jobs" + File.separator;

    private static final boolean IS_STORAGE_ENABLED = System.getProperty(COMPSsConstants.STORAGE_CONF) != null
        && !System.getProperty(COMPSsConstants.STORAGE_CONF).equals("")
        && !System.getProperty(COMPSsConstants.STORAGE_CONF).equals("null");

    private static final String STORAGE_CONF =
        IS_STORAGE_ENABLED ? System.getProperty(COMPSsConstants.STORAGE_CONF) : "null";

    private static final String PYTHON_INTERPRETER = System.getProperty(COMPSsConstants.PYTHON_INTERPRETER) != null
        ? System.getProperty(COMPSsConstants.PYTHON_INTERPRETER)
        : COMPSsDefaults.PYTHON_INTERPRETER;
    private static final String PYTHON_VERSION = (System.getProperty(COMPSsConstants.PYTHON_VERSION) != null)
        ? System.getProperty(COMPSsConstants.PYTHON_VERSION)
        : COMPSsDefaults.PYTHON_VERSION;
    private static final String PYTHON_VIRTUAL_ENVIRONMENT =
        System.getProperty(COMPSsConstants.PYTHON_VIRTUAL_ENVIRONMENT) != null
            ? System.getProperty(COMPSsConstants.PYTHON_VIRTUAL_ENVIRONMENT)
            : COMPSsDefaults.PYTHON_VIRTUAL_ENVIRONMENT;
    private static final String PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT =
        System.getProperty(COMPSsConstants.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT) != null
            ? System.getProperty(COMPSsConstants.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT)
            : COMPSsDefaults.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT;
    private static final String PYTHON_CUSTOM_EXTRAE_FILE =
        System.getProperty(COMPSsConstants.PYTHON_EXTRAE_CONFIG_FILE) != null
            ? System.getProperty(COMPSsConstants.PYTHON_EXTRAE_CONFIG_FILE)
            : COMPSsDefaults.PYTHON_CUSTOM_EXTRAE_FILE;

    private GOSJobDescription jobDescription;
    private final ArrayList<String> slaveWorkersNodeNames;

    private SSHChannel channel;
    public String jobPrefix;
    private String batchId = "UNASSIGNED";


    /**
     * Creates a new job instance with the given parameters.
     *
     * @param taskId Task Identifier
     * @param task Task description
     * @param impl Task Implementation
     * @param res Assigned Resource
     * @param slaveWorkersNodeNames slaveWorker node names for multi node tasks
     * @param listener Listener to notify job events
     * @param predecessors id of predecessors
     * @param numSuccessors number of successors
     */
    public GOSJob(int taskId, TaskDescription task, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {
        super(taskId, task, impl, res, listener, predecessors, numSuccessors);
        this.slaveWorkersNodeNames = (ArrayList<String>) slaveWorkersNodeNames;
        jobPrefix = "[GOSJob " + getCompositeID() + "] ";
    }

    public String getCompositeID() {
        return "task" + this.getTaskId() + "_job" + this.getJobId() + "_" + this.getHistory();
    }

    public GOSJobDescription getJobDescription() {
        return this.jobDescription;
    }

    /**
     * Creates a new job instance with the given parameters.
     */
    @Override
    public void submitJob() {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.warn("Ignoring notification since the job was cancelled");
            removeTmpData();
            return;
        }
        if (this.isBeingCancelled()) {
            LOGGER.warn("Job is being cancelled");
            notifyFailure(JobEndStatus.SUBMISSION_FAILED);
            return;
        }
        jobPrefix = "[GOSJob " + getCompositeID() + "] ";
        LOGGER.info("Submit GOSJob " + getCompositeID());
        jobDescription = prepareJob();
        String executable = getConfig().getInstallDir() + SCRIPT_PATH;
        if (isBatch()) {
            setCommandArgsBatch(jobDescription);
            executable += WORKER_SCRIPT_NAME_BATCH;
        } else {
            executable += WORKER_SCRIPT_NAME_INTERACTIVE;
        }
        jobDescription.setExecutable(executable);
        boolean correctlySubmitted = launchJob();
        if (correctlySubmitted) {
            addJobMonitor();
            getResourceNode().addRunningJob(this);
        } else {
            return;
        }

        if (DEBUG) {
            LOGGER.debug("Ready to submit job " + jobId + ":");
            LOGGER.debug("  * user: " + getConfig().getUser());
            LOGGER.debug("  * Host: " + getConfig().getHost());
            LOGGER.debug("  * Batch: " + isBatch());
            if (isBatch()) {
                LOGGER.debug("      * Queues: " + jobDescription.getQueues());
                LOGGER.debug("      * MaxExecTime: " + jobDescription.getMaxExecTime());
                LOGGER.debug("      * QOS: " + jobDescription.getQOS());
                LOGGER.debug("      * CFG: " + jobDescription.getCFG());
                LOGGER.debug("      * ProjectName: " + jobDescription.getProjectName());
            }
            LOGGER.debug("  * Executable: " + jobDescription.getExecutable());

            StringBuilder sb = new StringBuilder("  - Arguments:");
            for (String arg : jobDescription.getArguments()) {
                sb.append(" ").append(arg);
            }
            LOGGER.debug(sb.toString());
        }
    }

    private boolean launchJob() {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.error("Ignoring notification since the job was cancelled");
            removeTmpData();
            return false;
        }
        if (this.isBeingCancelled()) {
            LOGGER.warn("Job is being cancelled");
            notifyFailure(JobEndStatus.SUBMISSION_FAILED);
            return false;
        }
        SSHChannel ch;
        boolean ret;
        try {
            if (isBatch()) {
                ch = launchJobBatch();
            } else {
                ch = launchJobInteractive();
            }
            this.setChannel(ch);
            ret = true;
        } catch (JSchException e) {
            LOGGER.error(jobPrefix + "Could not correctly submit.", e);
            notifyFailure(JobEndStatus.SUBMISSION_FAILED);
            ret = false;
        }
        return ret;
    }

    private GOSJobDescription prepareJob() {
        GOSJobDescription jd = new GOSJobDescription(this);
        if (isBatch()) {
            jd.setQueueType((ArrayList<String>) getConfig().getProjectProperty("Queue"));
            jd.setCFG(getConfig().getProjectProperty("FileCFG"));
            jd.setQOS(getConfig().getProjectProperty("QOS"));
            jd.setReservation(getConfig().getProjectProperty("Reservation"));
            jd.setProjectName(getConfig().getProjectProperty("ProjectName"));
            long timeout = getTimeOut();
            if (timeout > 0) {
                jd.setMaxExecTime((long) (timeout / 60));
            } else {
                jd.setMaxExecTime(getConfig().getProjectProperty("MaxExecTime"));
            }
        }
        jd.setHost(getResourceNode().getSSHHost());
        jd.setSandboxDir(getConfig().getSandboxWorkingDir());
        jd.setID(this.getCompositeID());
        try {
            jd.addOutput(File.separator + JOBS_DIR + getCompositeID() + ".out",
                File.separator + JOBS_DIR + getCompositeID() + ".err");
        } catch (IOException e) {
            LOGGER.error(jobPrefix + "Could not create output files.");
        }

        createArguments(jd);
        return jd;
    }

    private SSHChannel launchJobInteractive() throws JSchException {
        SSHChannel ch = getSSHHost().executeCommandInteractive(jobDescription);
        ch.reason = getCompositeID() + "launchJob";
        return ch;
    }

    private SSHChannel launchJobBatch() throws JSchException {
        return getSSHHost().executeCommandBatch(jobDescription);
    }

    private void addJobMonitor() {
        getGOSMonitoring().addJobMonitor(this);
    }

    private GOSMonitoring getGOSMonitoring() {
        return this.getResourceNode().getGosMonitoring();
    }

    public SSHChannel getChannel() {
        return this.channel;
    }

    private GOSConfiguration getConfig() {
        return getResourceNode().getConfig();
    }

    private void createArguments(GOSJobDescription jd) {
        LOGGER.debug("Preparing GOS Job " + this.jobId);
        jd.addArgument("taskIDComposite", getCompositeID());
        jd.addArgument("isBatch", String.valueOf(getConfig().isBatch()));
        jd.addArgument("responseDir", jd.getResponseFileDir());
        jd.addArgument("cancelScriptDir", jd.getCancelScriptDir());

        // Host Configuration
        jd.addArgument("hostname", getHostName());
        jd.addArgument("installDir", getConfig().getInstallDir());
        jd.addArgument("appDir", getConfig().getAppDir());
        jd.addArgument("envScript", getResourceNode().getEnvScriptPath());
        jd.addArgument("LibPath", getResourceNode().getLibPath());
        jd.addArgument("workingDir-sandboxWorkingDir", getConfig().getSandboxWorkingDir());
        jd.addArgument("storageConf", STORAGE_CONF);
        jd.addArgument("streamingBackend", Comm.getStreamingBackend().name());
        jd.addArgument("appHost", Comm.getAppHost().getName());
        jd.addArgument("streamingPort", String.valueOf(Comm.getStreamingPort()));
        jd.addArgument("debug", String.valueOf(DEBUG));

        // Obsolete file
        List<MultiURI> obsoleteFiles = getResource().pollObsoletes();
        if (obsoleteFiles != null) {
            jd.addArgument("numObsoleteFiles", String.valueOf(obsoleteFiles.size()));
            int i = 1;
            for (MultiURI u : obsoleteFiles) {
                jd.addArgument("obsoleteFile" + i++, u.getPath());
            }
        } else {
            jd.addArgument("numObsoleteFiles", "0");
        }

        // Tracing flags
        jd.addArgument("tracing", Boolean.toString(Tracer.isActivated()));
        jd.addArgument("tracer runtime type", String.valueOf(TraceEventType.RUNTIME.code));
        jd.addArgument("sandbox creation id", String.valueOf(TraceEvent.CREATING_TASK_SANDBOX.getId()));
        jd.addArgument("sandbox removal id", String.valueOf(TraceEvent.REMOVING_TASK_SANDBOX.getId()));
        jd.addArgument("event type", String.valueOf(TraceEventType.TASKS_FUNC.code));
        jd.addArgument("tracer task id", String.valueOf(this.taskId));
        int slot = Tracer.isActivated() ? acquireTracingSlot() : -1;
        jd.addArgument("slot", String.valueOf(slot));

        jd.addArgument("PersistentC", "false");
        // arguments for the different implementation
        argumentsMethodImpl(jd, (AbstractMethodImplementation) this.impl);

        // Job arguments
        jd.addArgument("jobId", String.valueOf(this.jobId));
        jd.addArgument("taskId", String.valueOf(this.taskId));
        jd.addArgument("jobHistory", String.valueOf(this.getHistory()));
        // Job time-out and on-failure
        jd.addArgument("timeOut", String.valueOf(getTimeOut()));
        jd.addArgument("onFailure", String.valueOf(getOnFailure()));

        // Slave nodes and cus description

        jd.addArgument("n slaveWorkersNodeNames", "" + slaveWorkersNodeNames.size());
        for (int i = 0; i < slaveWorkersNodeNames.size(); i++) {
            jd.addArgument("slaveWorker" + i, slaveWorkersNodeNames.get(i));
        }

        jd.addArgument("totalCPUComputingUnits",
            String.valueOf(((MethodResourceDescription) this.impl.getRequirements()).getTotalCPUComputingUnits()));
        jd.addArgument("cpuMap", "disabled");
        jd.addArgument("totalGPUComputingUnits",
            String.valueOf(((MethodResourceDescription) this.impl.getRequirements()).getTotalGPUComputingUnits()));
        jd.addArgument("gpuMap", "disabled");
        jd.addArgument("totalFPGAComputingUnits",
            String.valueOf(((MethodResourceDescription) this.impl.getRequirements()).getTotalFPGAComputingUnits()));
        jd.addArgument("fpgaMap", "disabled");

        // Parameters
        int numReturns = this.taskParams.getNumReturns();
        int numParams = this.taskParams.getParameters().size();
        numParams -= numReturns;
        if (this.taskParams.hasTargetObject()) {
            numParams--;
        }
        jd.addArgument("numParams", Integer.toString(numParams));
        jd.addArgument("hasTargetObject", Boolean.toString(taskParams.hasTargetObject()));
        jd.addArgument("numReturns", Integer.toString(numReturns));
        processParameters(jd, taskParams.getParameters());

    }

    private void processParameters(GOSJobDescription jd, List<Parameter> parameters) {
        int i = 1;
        for (Parameter param : parameters) {
            LinkedList<String> singleParamDesc = new LinkedList<>();
            DataType type = param.getType();
            singleParamDesc.add(Integer.toString(type.ordinal()));
            singleParamDesc.add(Integer.toString(param.getStream().ordinal()));
            String prefix = param.getPrefix();
            if (prefix == null || prefix.isEmpty()) {
                prefix = Constants.PREFIX_EMPTY;
            }
            singleParamDesc.add(prefix);

            String paramName = param.getName();
            singleParamDesc.add((paramName == null) ? "null" : (paramName.isEmpty()) ? "null" : paramName);

            String conType = param.getContentType();
            singleParamDesc.add((conType == null) ? "null" : (conType.isEmpty()) ? "null" : conType);
            singleParamDesc.add(Double.toString(param.getWeight()));
            singleParamDesc.add(Boolean.toString(param.isKeepRename()));

            switch (type) {
                case DIRECTORY_T:
                case FILE_T:
                case EXTERNAL_STREAM_T:
                    DependencyParameter dFilePar = (DependencyParameter) param;
                    String originalName = dFilePar.getOriginalName();
                    singleParamDesc.add(originalName);
                    singleParamDesc.add(dFilePar.getDataTarget());
                    jd.killArguments.add(dFilePar.getDataTarget());
                    break;
                case PSCO_T:
                case EXTERNAL_PSCO_T:
                    LOGGER.error("GOS Adaptor does not support PSCO Types");
                    failed(JobEndStatus.SUBMISSION_FAILED);
                    break;
                case OBJECT_T:
                case STREAM_T:
                    DependencyParameter dPar = (DependencyParameter) param;
                    DataAccessId dAccId = dPar.getDataAccessId();
                    singleParamDesc.add(dPar.getDataTarget());
                    if (dAccId.isWrite()) {
                        singleParamDesc.add("W"); // for the worker to know it must write the object to disk
                    } else {
                        singleParamDesc.add("R");
                    }
                    break;
                case BINDING_OBJECT_T:
                    DependencyParameter dExtObjPar = (DependencyParameter) param;
                    // DataAccessId dExtObjAccId = dExtObjPar.getDataAccessId();
                    BindingObject bo = BindingObject.generate(dExtObjPar.getDataTarget());
                    singleParamDesc.add(bo.getId());
                    singleParamDesc.add(Integer.toString(bo.getType()));
                    singleParamDesc.add(Integer.toString(bo.getElements()));
                    break;
                case STRING_T:
                    BasicTypeParameter btParS = (BasicTypeParameter) param;
                    // Check spaces
                    String value = btParS.getValue().toString();
                    int numSubStrings = value.split(" ").length;
                    singleParamDesc.add(Integer.toString(numSubStrings));
                    // todo: why isn't value parsed and added separately?
                    singleParamDesc.add(value);
                    break;
                case STRING_64_T:
                    BasicTypeParameter btp = (BasicTypeParameter) param;
                    // Check spaces
                    String value64 = btp.getValue().toString();
                    singleParamDesc.add("1");
                    singleParamDesc.add(value64);
                    break;
                default:
                    // Basic Types
                    BasicTypeParameter btParB = (BasicTypeParameter) param;
                    singleParamDesc.add(btParB.getValue().toString());
                    break;
            }
            jd.addArgument(singleParamDesc, "parameterDesc" + i++ + "_");
        }

    }

    private int acquireTracingSlot() {
        String host = getConfig().getHost();
        int slot = Tracer.getNextSlot(host);
        jobDescription.addTracingSlot(slot);
        return slot;
    }

    private void argumentsMethodImpl(GOSJobDescription jd, AbstractMethodImplementation absImpl) {
        COMPSsConstants.Lang lang = getLang();
        if (lang == COMPSsConstants.Lang.UNKNOWN) {
            throw new LangNotDefinedException();
        }

        jd.addArgument("lang", lang.toString().toLowerCase());
        jd.addArgument("taskSandboxWorkingDir", getConfig().getSandboxWorkingDir());
        jd.addArgument("javaClasspath", getClasspath());
        jd.addArgument("Pythonpath", getPythonpath());
        jd.addArgument("PYTHON_INTERPRETER", PYTHON_INTERPRETER);
        jd.addArgument("PYTHON_VERSION", PYTHON_VERSION);
        jd.addArgument("PYTHON_VIRTUAL_ENVIRONMENT", PYTHON_VIRTUAL_ENVIRONMENT);
        jd.addArgument("PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT", PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT);
        jd.addArgument("PYTHON_CUSTOM_EXTRAE_FILE", PYTHON_CUSTOM_EXTRAE_FILE);

        jd.addArgument("implType", String.valueOf(absImpl.getMethodType()));
        final int startImplementationArgs = jd.numArgs();
        switch (absImpl.getMethodType()) {
            default:
                throw new RuntimeException("Not supported Method type: " + absImpl.getMethodType());
            case METHOD:
                MethodDefinition methodImpl = (MethodDefinition) absImpl.getDefinition();
                String methodName = methodImpl.getAlternativeMethodName();
                if (methodName == null || methodName.isEmpty()) {
                    methodImpl.setAlternativeMethodName(taskParams.getName());
                }
                methodImpl.appendToArgs(jd.arguments, taskParams.getName());
                break;
            case BINARY:
                BinaryDefinition binaryImpl = (BinaryDefinition) absImpl.getDefinition();
                binaryImpl.appendToArgs(jd.arguments, null);
                break;
            case MPI:
                MPIDefinition mpiImpl = (MPIDefinition) absImpl.getDefinition();
                mpiImpl.appendToArgs(jd.arguments, null);
                break;
            case COMPSs:
                COMPSsDefinition compssImpl = (COMPSsDefinition) absImpl.getDefinition();
                compssImpl.appendToArgs(jd.arguments, null);
                break;
            case DECAF:
                DecafDefinition decafImpl = (DecafDefinition) absImpl.getDefinition();
                decafImpl.appendToArgs(jd.arguments, getResourceNode().getConfig().getAppDir());
                break;
            case MULTI_NODE:
                MultiNodeDefinition multiNodeImpl = (MultiNodeDefinition) absImpl.getDefinition();
                multiNodeImpl.appendToArgs(jd.arguments, null);
                break;
            case OMPSS:
                OmpSsDefinition ompssImpl = (OmpSsDefinition) absImpl.getDefinition();
                ompssImpl.appendToArgs(jd.arguments, null);
                break;
            case OPENCL:
                OpenCLDefinition openclImpl = (OpenCLDefinition) absImpl.getDefinition();
                openclImpl.appendToArgs(jd.arguments, null);
                break;
            case PYTHON_MPI:
                PythonMPIDefinition pythonMPIImpl = (PythonMPIDefinition) absImpl.getDefinition();
                pythonMPIImpl.appendToArgs(jd.arguments, null);
                break;
            case CONTAINER:
                ContainerDefinition containerImpl = (ContainerDefinition) absImpl.getDefinition();
                containerImpl.appendToArgs(jd.arguments, null);
                break;
        }
        jd.fillKeys("implementationArgument");
        final int endImplementationArgs = jd.numArgs() - 1;
        jd.replaceIllegalCharacters(startImplementationArgs, endImplementationArgs);

    }

    @Override
    public void cancelJob() throws Exception {
        LOGGER.debug("GOS Cancel Job " + getCompositeID());
        if (jobDescription != null) {
            SSHHost executingHost = jobDescription.getSSHHost();
            if (executingHost != null) {
                String cancelScript = jobDescription.getCancelScript(getCompositeID());
                SSHChannel ch = executingHost.killJob(this, cancelScript, jobDescription.getOutput(),
                    jobDescription.getOutputError());
                this.setChannel(ch);
            } else {
                throw (new Exception("Job not submitted"));
            }
        } else {
            throw (new Exception("Job not submitted"));
        }
    }

    public String getHostName() {
        return worker.getName();
    }

    @Override
    public TaskType getType() {
        return TaskType.METHOD;
    }

    @Override
    public String toString() {
        AbstractMethodImplementation method = (AbstractMethodImplementation) this.impl;
        String definition = method.getMethodDefinition();
        String methodName = this.taskParams.getName();
        return "GOSJob JobId" + this.jobId + " for method " + methodName + " with definition " + definition;
    }

    public String getResponseFile() {
        return jobDescription.getResponseFileDir() + File.separator + getCompositeID();
    }

    public String getResponseDir() {
        return jobDescription.getResponseFileDir();
    }

    /**
     * Notify success to the job listener.
     */
    public void notifySuccess() {
        LOGGER.info(jobPrefix + "Job done with success");
        getResourceNode().removeRunningJob(this);
        completed();
    }

    @Override
    public void completed() {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.error("Ignoring notification since the job was cancelled");
            removeTmpData();
            return;
        }

        super.registerAllJobOutputsAsExpected();
        super.completed();
    }

    /**
     * Notify failure to the job listener.
     * 
     * @param status the finished status
     */
    public void notifyFailure(JobEndStatus status) {
        LOGGER.error(jobPrefix + "Job produced an error");
        getResourceNode().removeRunningJob(this);
        failed(status);
    }

    @Override
    public void failed(JobEndStatus status) {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.error("Ignoring notification since the job was cancelled");
            removeTmpData();
            return;
        }
        if (this.isBeingCancelled()) {
            super.registerAllJobOutputsAsExpected();
        }
        switch (this.taskParams.getOnFailure()) {
            case IGNORE:
            case CANCEL_SUCCESSORS:
                super.registerAllJobOutputsAsExpected();
                break;
            default:
                // case RETRY:
                // case FAIL:
                removeTmpData();
        }
        super.failed(status);
    }

    /**
     * Sets command args batch.
     *
     * @param jd the jd
     */
    public void setCommandArgsBatch(GOSJobDescription jd) {
        StringBuilder sb = new StringBuilder();
        sb.append(" --exec_time=").append(jd.getMaxExecTime());
        sb.append(" --sc_cfg=").append(jd.getCFG());
        // sb.append(" --queue=").append(jd.getQueues().get(0));
        sb.append(" --num_nodes=").append(taskParams.getNumNodes());
        sb.append(" --reservation=").append(jd.getReservation());
        sb.append(" --job_name=").append(getJobId());
        sb.append(" --qos=").append(jd.getQOS());
        sb.append(" --project_name=").append(jd.getProjectName());

        MethodResourceDescription mrd = (MethodResourceDescription) this.impl.getRequirements();

        sb.append(" --cpus_per_node=").append(mrd.getTotalCPUComputingUnits());
        sb.append(" --gpus_per_node=").append(mrd.getTotalGPUComputingUnits());
        sb.append(" --fpgas_per_node=").append(mrd.getTotalFPGAComputingUnits());
        sb.append(" --job_name=").append(getJobId());
        sb.append(" --master_working_dir=").append(getConfig().getSandboxWorkingDir());

        // Must include worker.sh, because executable will be submitBatch.sh
        String commandArgs = getConfig().getInstallDir() + SCRIPT_PATH + WORKER_SCRIPT_NAME_INTERACTIVE;
        sb.append(" ").append(commandArgs);

        jd.setCommandArgsBatch(sb.toString());
    }

    public void setChannel(SSHChannel ch) {
        this.channel = ch;
    }

    public SSHHost getSSHHost() {
        return getJobDescription().getSSHHost();
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getBatchId() {
        return batchId;
    }

    public boolean isBatch() {
        return getConfig().isBatch();
    }

    /**
     * Returns the path that the ouput of the task after calling the batch queue .
     */
    public String getBatchOutput() {
        if (isBatch()) {
            return getConfig().getSandboxWorkingDir() + GOSWorkerNode.BATCH_OUTPUT_DIR + "/" + getCompositeID();
        }
        return null;
    }

}
