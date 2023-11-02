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
package es.bsc.compss.gat.master;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.location.ProtocolType;
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
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.File;
import org.gridlab.gat.monitoring.Metric;
import org.gridlab.gat.monitoring.MetricDefinition;
import org.gridlab.gat.monitoring.MetricEvent;
import org.gridlab.gat.monitoring.MetricListener;
import org.gridlab.gat.resources.HardwareResourceDescription;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.Job.JobState;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.ResourceDescription;
import org.gridlab.gat.resources.SoftwareDescription;


/**
 * Representation of a Job execution for COMPSs with GAT Adaptor.
 */
public class GATJob extends es.bsc.compss.types.job.JobImpl<GATWorkerNode> implements MetricListener {

    // Worker script path
    private static final String WORKER_SCRIPT_PATH = File.separator + "Runtime" + File.separator + "scripts"
        + File.separator + "system" + File.separator + "adaptors" + File.separator + "gat" + File.separator;
    private static final String WORKER_SCRIPT_NAME = "worker.sh";

    // Storage Conf
    private static final boolean IS_STORAGE_ENABLED = System.getProperty(COMPSsConstants.STORAGE_CONF) != null
        && !System.getProperty(COMPSsConstants.STORAGE_CONF).equals("")
        && !System.getProperty(COMPSsConstants.STORAGE_CONF).equals("null");
    private static final String STORAGE_CONF =
        IS_STORAGE_ENABLED ? System.getProperty(COMPSsConstants.STORAGE_CONF) : "null";

    // Python interpreter
    private static final String PYTHON_INTERPRETER = System.getProperty(COMPSsConstants.PYTHON_INTERPRETER) != null
        ? System.getProperty(COMPSsConstants.PYTHON_INTERPRETER)
        : COMPSsDefaults.PYTHON_INTERPRETER;
    private static final String PYTHON_VERSION =
        System.getProperty(COMPSsConstants.PYTHON_VERSION) != null ? System.getProperty(COMPSsConstants.PYTHON_VERSION)
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

    private static final String JOBS_DIR =
        System.getProperty(COMPSsConstants.LOG_DIR) + "jobs" + java.io.File.separator;

    private static final String JOB_STATUS = "job.status";
    private static final String RES_ATTR = "machine.node";
    private static final String CALLBACK_PROCESSING_ERR = "Error processing callback for job";
    private static final String TERM_ERR = "Error terminating";

    private static final List<GATJob> RUNNING_JOBS = new LinkedList<>();

    // Brokers - TODO: Problem if many resources used
    private Map<String, ResourceBroker> brokers = new TreeMap<String, ResourceBroker>();

    private Job gatJob;
    // GAT context
    private final GATContext context;
    // GAT broker adaptor information
    private final boolean usingGlobus;
    private final boolean userNeeded;
    // Multi node information
    private final List<String> slaveWorkersNodeNames;


    /**
     * New GAT Job instance.
     *
     * @param taskId Associated task Id.
     * @param taskParams Associated task parameters.
     * @param impl Associated implementation.
     * @param res Executing resource.
     * @param listener Task listener.
     * @param context Task context.
     * @param userNeeded Whether a user is needed or not.
     * @param usingGlobus Whether globus is active or not.
     * @param slaveWorkersNodeNames Slave resources for multi-node executions.
     */
    public GATJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res, JobListener listener,
        GATContext context, boolean userNeeded, boolean usingGlobus, List<String> slaveWorkersNodeNames,
        List<Integer> predecessors, Integer numSuccessors) {

        super(taskId, taskParams, impl, res, listener, predecessors, numSuccessors);
        this.context = context;
        this.userNeeded = userNeeded;
        this.usingGlobus = usingGlobus;
        this.slaveWorkersNodeNames = slaveWorkersNodeNames;
    }

    @Override
    public TaskType getType() {
        return TaskType.METHOD;
    }

    @Override
    public void submitJob() throws Exception {
        // Prepare the job
        LOGGER.info("Submit GATJob with ID " + jobId);
        JobDescription jobDescr = null;

        jobDescr = prepareJob();
        // Get a broker for the host
        ResourceBroker broker = null;

        String dest = (String) jobDescr.getResourceDescription().getResourceAttribute(RES_ATTR);
        if ((broker = brokers.get(dest)) == null) {
            broker = GAT.createResourceBroker(context, new URI(dest));
            brokers.put(dest, broker);
        }

        // Submit the job, registering for notifications of job state
        // transitions (associatedJM is the metric listener)
        Job job = null;

        try {
            job = broker.submitJob(jobDescr, this, JOB_STATUS);
            RUNNING_JOBS.add(this);
        } catch (Exception e) {
            if (Tracer.isActivated()) {
                SoftwareDescription sd = jobDescr.getSoftwareDescription();
                releaseTracingSlot(sd);
            }
            throw e;
        }

        // Update mapping
        gatJob = job;
    }

    private int acquireTracingSlot(SoftwareDescription sd) {
        int slot;
        String host = getResourceNode().getHost();
        slot = Tracer.getNextSlot(host);
        sd.addAttribute("slot", slot);
        return slot;
    }

    private void releaseTracingSlot(SoftwareDescription sd) {
        Integer slot = (Integer) sd.getAttributes().get("slot");
        String host = getResourceNode().getHost();
        Tracer.freeSlot(host, slot);
    }

    protected static void stopAll() {
        LOGGER.debug("GAT stopping all jobs");
        for (GATJob job : RUNNING_JOBS) {
            try {
                job.cancelJob();
            } catch (Exception e) {
                LOGGER.error(TERM_ERR, e);
            }
        }
    }

    @Override
    public void cancelJob() throws Exception {
        LOGGER.debug("GAT stop job " + this.jobId);
        if (gatJob != null) {
            MetricDefinition md = gatJob.getMetricDefinitionByName(JOB_STATUS);
            Metric m = md.createMetric();
            gatJob.removeMetricListener(this, m);
            gatJob.stop();
        }
    }

    private JobDescription prepareJob() throws Exception {
        // Get the information related to the job
        LOGGER.debug("Preparing GAT Job " + this.jobId);

        String targetPath = getResourceNode().getInstallDir();
        String targetUser = getResourceNode().getUser();
        if (userNeeded && !targetUser.isEmpty()) {
            targetUser += "@";
        } else {
            targetUser = "";
        }

        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable(targetPath + WORKER_SCRIPT_PATH + WORKER_SCRIPT_NAME);
        ArrayList<String> lArgs = new ArrayList<>();

        // Host Configuration
        GATWorkerNode resource = this.getResourceNode();
        lArgs.add(resource.getName());
        lArgs.add(resource.getInstallDir());
        lArgs.add(resource.getAppDir());
        lArgs.add(resource.getEnvScriptPath());
        lArgs.add(resource.getLibPath());
        lArgs.add(resource.getWorkingDir());
        lArgs.add(STORAGE_CONF);
        lArgs.add(Comm.getStreamingBackend().name());
        lArgs.add(Comm.getAppHost().getName());
        lArgs.add(String.valueOf(Comm.getStreamingPort()));
        lArgs.add(String.valueOf(DEBUG));

        List<MultiURI> obsoleteFiles = getResource().pollObsoletes();
        if (obsoleteFiles != null) {
            lArgs.add("" + obsoleteFiles.size());
            for (MultiURI u : obsoleteFiles) {
                String renaming = u.getPath();
                lArgs.add(renaming);
            }
        } else {
            lArgs.add("0");
        }

        // Tracing flags
        lArgs.add(Boolean.toString(Tracer.isActivated()));
        if (Tracer.isActivated()) {
            lArgs.add(String.valueOf(TraceEventType.RUNTIME.code)); // Runtime event type
            lArgs.add(String.valueOf(TraceEvent.CREATING_TASK_SANDBOX.getId())); // sandbox creation id
            lArgs.add(String.valueOf(TraceEvent.REMOVING_TASK_SANDBOX.getId())); // sandbox removal id

            lArgs.add(String.valueOf(TraceEventType.TASKS_FUNC.code)); // event type
            lArgs.add(String.valueOf(this.taskId));
            int slot = acquireTracingSlot(sd);
            lArgs.add(String.valueOf(slot)); // slot id
        }

        // Implementation Description
        AbstractMethodImplementation absImpl = (AbstractMethodImplementation) this.impl;
        lArgs.add(String.valueOf(absImpl.getMethodType()));
        switch (absImpl.getMethodType()) {
            case METHOD:
                Lang lang = getLang();
                lArgs.add(lang.toString().toLowerCase());
                switch (lang) {
                    case JAVA:
                        lArgs.add(getClasspath());
                        break;
                    case C:
                    case R:
                        break;
                    case PYTHON:
                        lArgs.add(getPythonpath());
                        lArgs.add(PYTHON_INTERPRETER);
                        lArgs.add(PYTHON_VERSION);
                        lArgs.add(PYTHON_VIRTUAL_ENVIRONMENT);
                        lArgs.add(PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT);
                        lArgs.add(PYTHON_CUSTOM_EXTRAE_FILE);
                        break;
                    case UNKNOWN:
                        throw new LangNotDefinedException();
                }

                MethodDefinition methodImpl = (MethodDefinition) absImpl.getDefinition();
                String methodName = methodImpl.getAlternativeMethodName();
                if (methodName == null || methodName.isEmpty()) {
                    methodImpl.setAlternativeMethodName(taskParams.getName());
                }
                methodImpl.appendToArgs(lArgs, taskParams.getName());
                break;
            case BINARY:
                BinaryDefinition binaryImpl = (BinaryDefinition) absImpl.getDefinition();
                binaryImpl.appendToArgs(lArgs, null);
                break;
            case MPI:
                MPIDefinition mpiImpl = (MPIDefinition) absImpl.getDefinition();
                mpiImpl.appendToArgs(lArgs, null);
                break;
            case COMPSs:
                COMPSsDefinition compssImpl = (COMPSsDefinition) absImpl.getDefinition();
                compssImpl.appendToArgs(lArgs, null);
                break;
            case DECAF:
                DecafDefinition decafImpl = (DecafDefinition) absImpl.getDefinition();
                decafImpl.appendToArgs(lArgs, getResourceNode().getAppDir());
                break;
            case MULTI_NODE:
                MultiNodeDefinition multiNodeImpl = (MultiNodeDefinition) absImpl.getDefinition();
                multiNodeImpl.appendToArgs(lArgs, null);
                break;
            case OMPSS:
                OmpSsDefinition ompssImpl = (OmpSsDefinition) absImpl.getDefinition();
                ompssImpl.appendToArgs(lArgs, null);
                break;
            case OPENCL:
                OpenCLDefinition openclImpl = (OpenCLDefinition) absImpl.getDefinition();
                openclImpl.appendToArgs(lArgs, null);
                break;
            case PYTHON_MPI:
                PythonMPIDefinition pythonMPIImpl = (PythonMPIDefinition) absImpl.getDefinition();
                pythonMPIImpl.appendToArgs(lArgs, null);
                break;
            case CONTAINER:
                ContainerDefinition containerImpl = (ContainerDefinition) absImpl.getDefinition();
                containerImpl.appendToArgs(lArgs, null);
                break;
        }

        // Job arguments
        lArgs.add(String.valueOf(this.jobId));
        lArgs.add(String.valueOf(this.taskId));

        // Job time-out and on-failure
        lArgs.add(String.valueOf(getTimeOut()));

        // Slave nodes and cus description
        lArgs.add(String.valueOf(slaveWorkersNodeNames.size()));
        lArgs.addAll(slaveWorkersNodeNames);
        lArgs
            .add(String.valueOf(((MethodResourceDescription) this.impl.getRequirements()).getTotalCPUComputingUnits()));

        // Parameters
        int numReturns = taskParams.getNumReturns();
        int numParams = taskParams.getParameters().size();
        numParams -= numReturns;
        if (taskParams.hasTargetObject()) {
            numParams--;
        }
        lArgs.add(Integer.toString(numParams));
        lArgs.add(Boolean.toString(taskParams.hasTargetObject()));
        lArgs.add(Integer.toString(numReturns));

        for (Parameter param : taskParams.getParameters()) {
            lArgs.addAll(processParameter(param));
        }

        // Conversion vector -> array
        String[] arguments = new String[lArgs.size()];
        arguments = lArgs.toArray(arguments);
        try {
            sd.setArguments(arguments);
        } catch (NullPointerException e) {
            StringBuilder sb = new StringBuilder(
                "Null argument parameter of job " + this.jobId + " " + absImpl.getMethodDefinition() + "\n");
            int i = 0;
            for (Parameter param : taskParams.getParameters()) {
                sb.append("Parameter ").append(i).append("\n");
                DataType type = param.getType();
                sb.append("\t Type: ").append(param.getType()).append("\n");
                switch (type) {
                    case FILE_T:
                    case OBJECT_T:
                    case STREAM_T:
                    case BINDING_OBJECT_T:
                        DependencyParameter dPar = (DependencyParameter) param;
                        DataAccessId dAccId = dPar.getDataAccessId();
                        sb.append("\t Target: ").append(dPar.getDataTarget()).append("\n");
                        if (type == DataType.OBJECT_T || type == DataType.STREAM_T
                            || type == DataType.BINDING_OBJECT_T) {
                            if (dAccId.isWrite()) {
                                // For the worker to know it must write the object to disk
                                sb.append("\t Direction: " + "W").append("\n");
                            } else {
                                sb.append("\t Direction: " + "R").append("\n");
                            }
                        }
                        break;
                    case STRING_T:
                    case STRING_64_T:
                        BasicTypeParameter btParS = (BasicTypeParameter) param;
                        // Check spaces
                        String value = btParS.getValue().toString();
                        int numSubStrings = value.split(" ").length;
                        sb.append("\t Num Substrings: ").append(Integer.toString(numSubStrings)).append("\n");
                        sb.append("\t Value:").append(value).append("\n");
                        break;
                    default:
                        // Basic types
                        BasicTypeParameter btParB = (BasicTypeParameter) param;
                        sb.append("\t Value: ").append(btParB.getValue().toString()).append("\n");
                        break;
                }
                i++;
            }
            LOGGER.error(sb.toString());
            this.failed(JobEndStatus.SUBMISSION_FAILED);
        }
        sd.addAttribute("jobId", jobId);
        // JEA Changed to allow execution in MN
        sd.addAttribute(SoftwareDescription.WALLTIME_MAX, absImpl.getRequirements().getWallClockLimit());
        if (absImpl.getRequirements().getHostQueues().size() > 0) {
            sd.addAttribute(SoftwareDescription.JOB_QUEUE, absImpl.getRequirements().getHostQueues().get(0));
        }
        sd.addAttribute("coreCount", absImpl.getRequirements().getTotalCPUComputingUnits());
        sd.addAttribute("gpuCount", absImpl.getRequirements().getTotalGPUComputingUnits());
        sd.addAttribute("fpgaCount", absImpl.getRequirements().getTotalFPGAComputingUnits());
        sd.addAttribute(SoftwareDescription.MEMORY_MAX, absImpl.getRequirements().getMemorySize());
        // sd.addAttribute(SoftwareDescription.SANDBOX_ROOT, "/tmp/");

        sd.addAttribute(SoftwareDescription.SANDBOX_ROOT, getResourceNode().getWorkingDir());
        sd.addAttribute(SoftwareDescription.SANDBOX_USEROOT, "true");
        sd.addAttribute(SoftwareDescription.SANDBOX_DELETE, "false");

        /*
         * sd.addAttribute(SoftwareDescription.SANDBOX_PRESTAGE_STDIN, "false");
         * sd.addAttribute(SoftwareDescription.SANDBOX_POSTSTAGE_STDOUT, "false");
         * sd.addAttribute(SoftwareDescription.SANDBOX_POSTSTAGE_STDERR, "false");
         */
        if (DEBUG) { // Set standard output file for job
            File outFile = GAT.createFile(context, ProtocolType.ANY_URI.getSchema() + File.separator + JOBS_DIR + "job"
                + jobId + "_" + this.getHistory() + ".out");
            sd.setStdout(outFile);
        }

        if (DEBUG || usingGlobus) {
            // Set standard error file for job
            File errFile = GAT.createFile(context, ProtocolType.ANY_URI.getSchema() + File.separator + JOBS_DIR + "job"
                + jobId + "_" + this.getHistory() + ".err");
            sd.setStderr(errFile);
        }

        String targetHost = getResourceNode().getHost();
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(RES_ATTR, ProtocolType.ANY_URI.getSchema() + targetUser + targetHost);
        attributes.put("Jobname", "compss_remote_job_" + jobId);
        ResourceDescription rd = new HardwareResourceDescription(attributes);

        if (DEBUG) {
            LOGGER.debug("Ready to submit job " + jobId + ":");
            LOGGER.debug("  * Host: " + targetHost);
            LOGGER.debug("  * Executable: " + sd.getExecutable());

            StringBuilder sb = new StringBuilder("  - Arguments:");
            for (String arg : sd.getArguments()) {
                sb.append(" ").append(arg);
            }
            LOGGER.debug(sb.toString());
        }

        JobDescription jd = new JobDescription(sd, rd);
        // jd.setProcessCount(method.getRequirements().getProcessorCoreCount());
        return jd;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("[[Job id: ").append(getJobId()).append("]");
        buffer.append(", ").append(taskParams.toString());
        GATWorkerNode node = getResourceNode();
        String host = node.getHost();
        String user = node.getUser();
        buffer.append(", [Target host: ").append(host).append("]");
        buffer.append(", [User: ").append(user).append("]]");
        return buffer.toString();
    }

    private LinkedList<String> processParameter(Parameter param) {
        LinkedList<String> paramDesc = new LinkedList<>();
        DataType type = param.getType();
        paramDesc.add(Integer.toString(type.ordinal()));
        paramDesc.add(Integer.toString(param.getStream().ordinal()));
        String prefix = param.getPrefix();
        if (prefix == null || prefix.isEmpty()) {
            prefix = Constants.PREFIX_EMPTY;
        }
        paramDesc.add(prefix);

        String paramName = param.getName();
        paramDesc.add((paramName == null) ? "null" : (paramName.isEmpty()) ? "null" : paramName);

        String conType = param.getContentType();
        paramDesc.add((conType == null) ? "null" : (conType.isEmpty()) ? "null" : conType);
        paramDesc.add(Double.toString(param.getWeight()));
        paramDesc.add(Boolean.toString(param.isKeepRename()));

        switch (type) {
            case FILE_T:
            case EXTERNAL_STREAM_T:
                DependencyParameter dFilePar = (DependencyParameter) param;
                String originalName = dFilePar.getOriginalName();
                paramDesc.add(originalName);
                paramDesc.add(dFilePar.getDataTarget());
                break;
            case PSCO_T:
            case EXTERNAL_PSCO_T:
                LOGGER.error("GAT Adaptor does not support PSCO Types");
                this.failed(JobEndStatus.SUBMISSION_FAILED);
                break;
            case OBJECT_T:
            case STREAM_T:
                DependencyParameter dPar = (DependencyParameter) param;
                DataAccessId dAccId = dPar.getDataAccessId();
                paramDesc.add(dPar.getDataTarget());
                if (dAccId.isWrite()) {
                    paramDesc.add("W"); // for the worker to know it must write the object to disk
                } else {
                    paramDesc.add("R");
                }
                break;
            case BINDING_OBJECT_T:
                DependencyParameter dExtObjPar = (DependencyParameter) param;
                // DataAccessId dExtObjAccId = dExtObjPar.getDataAccessId();
                BindingObject bo = BindingObject.generate(dExtObjPar.getDataTarget());
                paramDesc.add(bo.getId());
                paramDesc.add(Integer.toString(bo.getType()));
                paramDesc.add(Integer.toString(bo.getElements()));
                break;
            case STRING_T:
                BasicTypeParameter btParS = (BasicTypeParameter) param;
                // Check spaces
                String value = btParS.getValue().toString();
                int numSubStrings = value.split(" ").length;
                paramDesc.add(Integer.toString(numSubStrings));
                // todo: why isn't value parsed and added seprately?
                paramDesc.add(value);
                break;
            case STRING_64_T:
                BasicTypeParameter btp = (BasicTypeParameter) param;
                // decode the string
                byte[] decodedBytes = Base64.getDecoder().decode(btp.getValue().toString());
                String[] values = new String(decodedBytes).split(" ");
                // add total # of strings
                paramDesc.add(Integer.toString(values.length));
                paramDesc.addAll(Arrays.asList(values));
                break;
            default:
                // Basic Types
                BasicTypeParameter btParB = (BasicTypeParameter) param;
                paramDesc.add(btParB.getValue().toString());
                break;
        }
        return paramDesc;
    }

    // MetricListener interface implementation
    @Override
    public void processMetricEvent(MetricEvent value) {
        Job job = (Job) value.getSource();
        JobState newJobState = (JobState) value.getValue();
        JobDescription jd = (JobDescription) job.getJobDescription();
        SoftwareDescription sd = jd.getSoftwareDescription();
        Integer jobId = (Integer) sd.getAttributes().get("jobId");

        LOGGER.debug("Processing job ID = " + jobId);
        /*
         * Check if either the job has finished or there has been a submission error. We don't care about other state
         * transitions
         */
        if (newJobState == JobState.STOPPED) {
            if (Tracer.isActivated()) {
                releaseTracingSlot(sd);
            }

            /*
             * We must check whether the chosen adaptor is globus In that case, since globus doesn't provide the exit
             * status of a job, we must examine the standard error file
             */
            try {
                if (usingGlobus) {
                    File errFile = sd.getStderr();
                    // Error file should always be in the same host as the IT
                    File localFile = GAT.createFile(context, errFile.toGATURI());
                    if (localFile.length() > 0) {
                        gatJob = null;
                        RUNNING_JOBS.remove(this);
                        ErrorManager.warn("Error when creating file.");
                        this.failed(JobEndStatus.EXECUTION_FAILED);
                    } else {
                        if (!DEBUG) {
                            localFile.delete();
                        }
                        RUNNING_JOBS.remove(this);
                        this.completed();
                    }
                } else if (job.getExitStatus() == 0) {
                    RUNNING_JOBS.remove(this);
                    this.completed();
                } else {
                    gatJob = null;
                    RUNNING_JOBS.remove(this);
                    this.failed(JobEndStatus.EXECUTION_FAILED);
                }
            } catch (Exception e) {
                ErrorManager.fatal(CALLBACK_PROCESSING_ERR + ": " + this, e);
            }
        } else if (newJobState == JobState.SUBMISSION_ERROR) {
            if (Tracer.isActivated()) {
                releaseTracingSlot(sd);
            }

            try {
                if (usingGlobus && job.getInfo().get("resManError").equals("NO_ERROR")) {
                    RUNNING_JOBS.remove(this);
                    this.completed();
                } else {
                    gatJob = null;
                    RUNNING_JOBS.remove(this);
                    this.failed(JobEndStatus.SUBMISSION_FAILED);
                }
            } catch (GATInvocationException e) {
                ErrorManager.fatal(CALLBACK_PROCESSING_ERR + ": " + this, e);
            }
        }
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

}
