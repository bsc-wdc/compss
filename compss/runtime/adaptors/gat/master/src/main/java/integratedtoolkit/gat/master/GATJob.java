package integratedtoolkit.gat.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;

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

import integratedtoolkit.ITConstants;
import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.parameter.BasicTypeParameter;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataAccessId.RAccessId;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.types.implementations.AbstractMethodImplementation;
import integratedtoolkit.types.implementations.BinaryImplementation;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.implementations.MPIImplementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.implementations.OmpSsImplementation;
import integratedtoolkit.types.implementations.OpenCLImplementation;
import integratedtoolkit.types.job.JobListener;
import integratedtoolkit.types.job.JobListener.JobEndStatus;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Tracer;

import java.util.LinkedList;
import java.util.List;

/**
 * Representation of a Job execution for COMPSs with GAT Adaptor
 * 
 */
public class GATJob extends integratedtoolkit.types.job.Job<GATWorkerNode> implements MetricListener {
    
    // Worker script path
    private static final String WORKER_SCRIPT_PATH = File.separator + "Runtime" + File.separator + "scripts" + File.separator + "system"
            + File.separator + "adaptors" + File.separator + "gat" + File.separator;
    private static final String WORKER_SCRIPT_NAME = "worker.sh";

    // Storage Conf
    private static final boolean IS_STORAGE_ENABLED = System.getProperty(ITConstants.IT_STORAGE_CONF) != null
            && !System.getProperty(ITConstants.IT_STORAGE_CONF).equals("")
            && !System.getProperty(ITConstants.IT_STORAGE_CONF).equals("null");
    private static final String STORAGE_CONF = IS_STORAGE_ENABLED ? System.getProperty(ITConstants.IT_STORAGE_CONF) : "null";
    

    private static final String JOBS_DIR = System.getProperty(ITConstants.IT_APP_LOG_DIR) + "jobs" + java.io.File.separator;
    
    private static final String JOB_STATUS = "job.status";
    private static final String RES_ATTR = "machine.node";
    private static final String CALLBACK_PROCESSING_ERR = "Error processing callback for job";
    private static final String TERM_ERR = "Error terminating";

    private static final LinkedList<GATJob> RUNNING_JOBS = new LinkedList<>();

    // Brokers - TODO: Problem if many resources used
    private Map<String, ResourceBroker> brokers = new TreeMap<String, ResourceBroker>();
    
    private Job GATjob;
    // GAT context
    private final GATContext context;
    // GAT broker adaptor information
    private final boolean usingGlobus;
    private final boolean userNeeded;
    // Multi node information
    private final List<String> slaveWorkersNodeNames;


    /**
     * New GAT Job instance
     * 
     * @param taskId
     * @param taskParams
     * @param impl
     * @param res
     * @param listener
     * @param context
     * @param userNeeded
     * @param usingGlobus
     * @param slaveWorkersNodeNames
     */
    public GATJob(int taskId, TaskDescription taskParams, Implementation<?> impl, Resource res, JobListener listener, GATContext context,
            boolean userNeeded, boolean usingGlobus, List<String> slaveWorkersNodeNames) {
        
        super(taskId, taskParams, impl, res, listener);
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
    public void submit() throws Exception {
        // Prepare the job
        logger.info("Submit GATJob with ID " + jobId);
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
                Tracer.freeSlot(((GATWorkerNode) worker.getNode()).getHost(),
                        (Integer) jobDescr.getSoftwareDescription().getAttributes().get("slot"));
            }
            throw e;
        }

        // Update mapping
        GATjob = job;
    }

    protected static void stopAll() {
        for (GATJob job : RUNNING_JOBS) {
            try {
                job.stop();
            } catch (Exception e) {
                logger.error(TERM_ERR, e);
            }
        }
    }

    @Override
    public void stop() throws Exception {
        if (GATjob != null) {
            MetricDefinition md = GATjob.getMetricDefinitionByName(JOB_STATUS);
            Metric m = md.createMetric();
            GATjob.removeMetricListener(this, m);
            GATjob.stop();
        }
    }

    // MetricListener interface implementation
    @Override
    public void processMetricEvent(MetricEvent value) {
        Job job = (Job) value.getSource();
        JobState newJobState = (JobState) value.getValue();
        JobDescription jd = (JobDescription) job.getJobDescription();
        SoftwareDescription sd = jd.getSoftwareDescription();
        Integer jobId = (Integer) sd.getAttributes().get("jobId");

        logger.debug("Processing job ID = " + jobId);
        /*
         * Check if either the job has finished or there has been a submission error. We don't care about other state
         * transitions
         */
        if (newJobState == JobState.STOPPED) {

            if (Tracer.isActivated()) {
                Integer slot = (Integer) sd.getAttributes().get("slot");
                String host = getResourceNode().getHost();
                Tracer.freeSlot(host, slot);
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
                        GATjob = null;
                        RUNNING_JOBS.remove(this);
                        listener.jobFailed(this, JobEndStatus.EXECUTION_FAILED);
                    } else {
                        if (!debug) {
                            localFile.delete();
                        }
                        RUNNING_JOBS.remove(this);
                        listener.jobCompleted(this);
                    }
                } else {
                    if (job.getExitStatus() == 0) {
                        RUNNING_JOBS.remove(this);
                        listener.jobCompleted(this);
                    } else {
                        GATjob = null;
                        RUNNING_JOBS.remove(this);
                        listener.jobFailed(this, JobEndStatus.EXECUTION_FAILED);
                    }
                }
            } catch (Exception e) {
                ErrorManager.fatal(CALLBACK_PROCESSING_ERR + ": " + this, e);
            }
        } else if (newJobState == JobState.SUBMISSION_ERROR) {
            if (Tracer.isActivated()) {
                Integer slot = (Integer) sd.getAttributes().get("slot");
                String host = getResourceNode().getHost();
                Tracer.freeSlot(host, slot);
            }

            try {
                if (usingGlobus && job.getInfo().get("resManError").equals("NO_ERROR")) {
                    RUNNING_JOBS.remove(this);
                    listener.jobCompleted(this);
                } else {
                    GATjob = null;
                    RUNNING_JOBS.remove(this);
                    listener.jobFailed(this, JobEndStatus.SUBMISSION_FAILED);
                }
            } catch (GATInvocationException e) {
                ErrorManager.fatal(CALLBACK_PROCESSING_ERR + ": " + this, e);
            }
        }
    }

    private JobDescription prepareJob() throws Exception {
        // Get the information related to the job
        TaskDescription taskParams = this.taskParams;

        String targetPath = getResourceNode().getInstallDir();
        String targetHost = getResourceNode().getHost();
        String targetUser = getResourceNode().getUser();
        if (userNeeded && !targetUser.isEmpty()) {
            targetUser += "@";
        } else {
            targetUser = "";
        }

        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable(targetPath + WORKER_SCRIPT_PATH + WORKER_SCRIPT_NAME);
        ArrayList<String> lArgs = new ArrayList<String>();

        // Common arguments: language working_dir lib_path num_obsolete [obs1... obsN] tracing [event_type task_id slot_id]
        lArgs.add(lang);
        lArgs.add(getResourceNode().getWorkingDir());
        lArgs.add(getResourceNode().getLibPath());
        LinkedList<LogicalData> obsoleteFiles = getResource().clearObsoletes();
        if (obsoleteFiles != null) {
            lArgs.add("" + obsoleteFiles.size());
            for (LogicalData ld : obsoleteFiles) {
                String renaming = ld.getName();
                lArgs.add(renaming);
            }
        } else {
            lArgs.add("0");
        }
        lArgs.add(Boolean.toString(Tracer.isActivated()));
        lArgs.add(getHostName());
        if (debug) {
            logger.debug("hostName " + getHostName());
        }

        if (Tracer.isActivated()) {
            lArgs.add(String.valueOf(Tracer.getTaskEventsType())); // event type
            lArgs.add(String.valueOf(this.taskParams.getId() + 1)); // task id
            int slot = Tracer.getNextSlot(targetHost);
            lArgs.add(String.valueOf(slot)); // slot id
            sd.addAttribute("slot", slot);
        }

        // Language-dependent arguments: app_dir classpath pythonpath debug storage_conf
        // method_impl_type method_impl_params
        // numSlaves [slave1,..,slaveN] numCus
        // has_target num_params par_type_1 par_1 ... par_type_n par_n
        lArgs.add(getResourceNode().getAppDir());
        lArgs.add(getClasspath());
        lArgs.add(getPythonpath());
        
        lArgs.add(String.valueOf(debug));
        lArgs.add(STORAGE_CONF);
        
        AbstractMethodImplementation absImpl = (AbstractMethodImplementation) this.impl;
        lArgs.add(String.valueOf(absImpl.getMethodType()));
        switch(absImpl.getMethodType()) {
            case METHOD:
                MethodImplementation methodImpl = (MethodImplementation) absImpl;
                lArgs.add(methodImpl.getDeclaringClass());
                String methodName = methodImpl.getAlternativeMethodName();
                if (methodName == null || methodName.isEmpty()){
                    methodName = taskParams.getName();
                }
                lArgs.add(methodName);
                break;
            case MPI:
                MPIImplementation mpiImpl = (MPIImplementation) absImpl;
                lArgs.add(mpiImpl.getMpiRunner());
                lArgs.add(mpiImpl.getBinary());
                break;
            case OMPSS:
                OmpSsImplementation ompssImpl = (OmpSsImplementation) absImpl;
                lArgs.add(ompssImpl.getBinary());
                break;
            case OPENCL:
                OpenCLImplementation openclImpl = (OpenCLImplementation) absImpl;
                lArgs.add(openclImpl.getKernel());
                break;
            case BINARY:
                BinaryImplementation binaryImpl = (BinaryImplementation) absImpl;
                lArgs.add(binaryImpl.getBinary());
                break;
        }
        
        // Slave nodes and cus description
        lArgs.add(String.valueOf(slaveWorkersNodeNames.size()));
        lArgs.addAll(slaveWorkersNodeNames);
        lArgs.add(String.valueOf( ((MethodResourceDescription)this.impl.getRequirements()).getTotalCPUComputingUnits() ));
        
        // Add if has target parameter
        lArgs.add(Boolean.toString(taskParams.hasTargetObject()));
        
        // Add return type
        if (taskParams.hasReturnValue()) {
            Parameter returnParam = taskParams.getParameters()[taskParams.getParameters().length - 1];
            lArgs.add( Integer.toString(returnParam.getType().ordinal()) );
        } else {
            lArgs.add("null");
        }
        
        // Add parameters
        int numParams = taskParams.getParameters().length;
        if (taskParams.hasReturnValue()) {
            numParams--;
        }
        lArgs.add(Integer.toString(numParams));
        for (Parameter param : taskParams.getParameters()) {
            DataType type = param.getType();
            lArgs.add(Integer.toString(type.ordinal()));
            lArgs.add(Integer.toString(param.getStream().ordinal()));

            switch (type) {
                case FILE_T:
                    DependencyParameter dFilePar = (DependencyParameter) param;
                    lArgs.add(dFilePar.getDataTarget());
                    break;
                case PSCO_T:
                case EXTERNAL_PSCO_T:
                    logger.error("GAT Adaptor does not support PSCO Types");
                    listener.jobFailed(this, JobEndStatus.SUBMISSION_FAILED);
                    break;
                case OBJECT_T:
                    DependencyParameter dPar = (DependencyParameter) param;
                    DataAccessId dAccId = dPar.getDataAccessId();
                    lArgs.add(dPar.getDataTarget());
                    if (dAccId instanceof RAccessId) {
                        lArgs.add("R");
                    } else {
                        lArgs.add("W"); // for the worker to know it must write the object to disk
                    }
                    break;
                case STRING_T:
                    BasicTypeParameter btParS = (BasicTypeParameter) param;
                    // Check spaces
                    String value = btParS.getValue().toString();
                    int numSubStrings = value.split(" ").length;
                    lArgs.add(Integer.toString(numSubStrings));
                    lArgs.add(value);
                    break;
                default:
                    // Basic Types
                    BasicTypeParameter btParB = (BasicTypeParameter) param;
                    lArgs.add(btParB.getValue().toString());
                    break;
            }
        }

        // Conversion vector -> array
        String[] arguments = new String[lArgs.size()];
        arguments = lArgs.toArray(arguments);
        try {
            sd.setArguments(arguments);
        } catch (NullPointerException e) {
            StringBuilder sb = new StringBuilder("Null argument parameter of job " + this.jobId + " " + absImpl.getMethodDefinition() + "\n");
            int i = 0;
            for (Parameter param : taskParams.getParameters()) {
                sb.append("Parameter ").append(i).append("\n");
                DataType type = param.getType();
                sb.append("\t Type: ").append(param.getType()).append("\n");
                if (type == DataType.FILE_T || type == DataType.OBJECT_T) {
                    DependencyParameter dPar = (DependencyParameter) param;
                    DataAccessId dAccId = dPar.getDataAccessId();
                    sb.append("\t Target: ").append(dPar.getDataTarget()).append("\n");
                    if (type == DataType.OBJECT_T) {
                        if (dAccId instanceof RAccessId) {
                            sb.append("\t Direction: " + "R").append("\n");
                        } else {
                            // for the worker to know it must write the object to disk
                            sb.append("\t Direction: " + "W").append("\n"); 
                        }
                    }
                } else if (type == DataType.STRING_T) {
                    BasicTypeParameter btParS = (BasicTypeParameter) param;
                    // Check spaces
                    String value = btParS.getValue().toString();
                    int numSubStrings = value.split(" ").length;
                    sb.append("\t Num Substrings: " + Integer.toString(numSubStrings)).append("\n");
                    sb.append("\t Value:" + value).append("\n");
                } else { // Basic types
                    BasicTypeParameter btParB = (BasicTypeParameter) param;
                    sb.append("\t Value: " + btParB.getValue().toString()).append("\n");
                }
                i++;
            }
            logger.error(sb.toString());
            listener.jobFailed(this, JobEndStatus.SUBMISSION_FAILED);
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
        if (debug) { // Set standard output file for job
            File outFile = GAT.createFile(context, Protocol.ANY_URI.getSchema() + File.separator + JOBS_DIR 
                    + "job" + jobId + "_" + this.getHistory() + ".out");
            sd.setStdout(outFile);
        }

        if (debug || usingGlobus) {
            // Set standard error file for job
            File errFile = GAT.createFile(context, Protocol.ANY_URI.getSchema() + File.separator + JOBS_DIR 
                    + "job" + jobId + "_" + this.getHistory() + ".err");
            sd.setStderr(errFile);
        }

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(RES_ATTR, Protocol.ANY_URI.getSchema() + targetUser + targetHost);
        attributes.put("Jobname", "compss_remote_job_" + jobId);
        ResourceDescription rd = new HardwareResourceDescription(attributes);

        if (debug) {
            logger.debug("Ready to submit job " + jobId + ":");
            logger.debug("  * Host: " + targetHost);
            logger.debug("  * Executable: " + sd.getExecutable());

            StringBuilder sb = new StringBuilder("  - Arguments:");
            for (String arg : sd.getArguments()) {
                sb.append(" ").append(arg);
            }
            logger.debug(sb.toString());
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

    @Override
    public String getHostName() {
        return getResourceNode().getName();
    }

}
