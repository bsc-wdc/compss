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
import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.MethodImplementation;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.parameter.BasicTypeParameter;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataAccessId.*;
import integratedtoolkit.types.job.Job.JobListener.JobEndStatus;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Tracer;

import java.util.LinkedList;


public class GATJob extends integratedtoolkit.types.job.Job<GATWorkerNode> implements MetricListener {

    private static final String JOBS_DIR = System.getProperty(ITConstants.IT_APP_LOG_DIR) + "jobs" + java.io.File.separator;

    private static final String ANY_PROT = "any://";
    private static final String JOB_STATUS = "job.status";
    private static final String RES_ATTR = "machine.node";

    // private static final String JOB_DIR_CREATION_ERR = "Error creating job output/error directory";
    private static final String CALLBACK_PROCESSING_ERR = "Error processing callback for job";
    private static final String TERM_ERR = "Error terminating";

    private Job GATjob;
    // GAT context
    private GATContext context;
    // GAT broker adaptor information
    private boolean usingGlobus;
    private boolean userNeeded;

    // Brokers - TODO: Problem if many resources used
    private Map<String, ResourceBroker> brokers = new TreeMap<String, ResourceBroker>();

    private static final String WORKER_SCRIPT_PATH = File.separator + "Runtime" + File.separator + "scripts" + File.separator + "system"
            + File.separator + "adaptors" + File.separator + "gat" + File.separator;
    private static final String WORKER_SCRIPT_NAME = "worker.sh";

    public static LinkedList<GATJob> runningJobs = new LinkedList<GATJob>();

    public GATJob(int taskId, TaskParams taskParams, Implementation<?> impl, Resource res, JobListener listener, GATContext context, boolean userNeeded, boolean usingGlobus) {
        super(taskId, taskParams, impl, res, listener);
        this.context = context;
        this.userNeeded = userNeeded;
        this.usingGlobus = usingGlobus;
    }

    public JobKind getKind() {
        return JobKind.METHOD;
    }

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

        // Submit the job, registering for notifications of job state transitions (associatedJM is the metric listener)
        Job job = null;

        try {
            job = broker.submitJob(jobDescr, this, JOB_STATUS);
            runningJobs.add(this);
        } catch (Exception e) {
            if (tracing) {
                Tracer.freeSlot(((GATWorkerNode) worker.getNode()).getHost(), (Integer) jobDescr.getSoftwareDescription().getAttributes().get("slot"));
            }
            throw e;
        }

        // Update mapping
        GATjob = job;
    }

    protected static void stopAll() {
        for (GATJob job : runningJobs) {
            try {
                job.stop();
            } catch (Exception e) {
                logger.error(TERM_ERR, e);
            }
        }
    }

    public void stop() throws Exception {
        if (GATjob != null) {
            MetricDefinition md = GATjob.getMetricDefinitionByName(JOB_STATUS);
            Metric m = md.createMetric();
            GATjob.removeMetricListener(this, m);
            GATjob.stop();
        }
    }

    // MetricListener interface implementation
    public void processMetricEvent(MetricEvent value) {
        Job job = (Job) value.getSource();
        JobState newJobState = (JobState) value.getValue();
        JobDescription jd = (JobDescription) job.getJobDescription();
        SoftwareDescription sd = jd.getSoftwareDescription();
        Integer jobId = (Integer) sd.getAttributes().get("jobId");

        logger.debug("Processing job ID = " + jobId);
        /* Check if either the job has finished or there has been a submission error.
         * We don't care about other state transitions
         */
        if (newJobState == JobState.STOPPED) {
            if (tracing) {
                Integer slot = (Integer) sd.getAttributes().get("slot");
                String host = getResourceNode().getHost();
                Tracer.freeSlot(host, slot);
            }

            /* We must check whether the chosen adaptor is globus
             * In that case, since globus doesn't provide the exit status of a job,
             * we must examine the standard error file
             */
            try {
                if (usingGlobus) {
                    File errFile = sd.getStderr();
                    // Error file should always be in the same host as the IT
                    File localFile = GAT.createFile(context, errFile.toGATURI());
                    if (localFile.length() > 0) {
                        GATjob = null;
                        runningJobs.remove(this);
                        listener.jobFailed(this, JobEndStatus.EXECUTION_FAILED);
                    } else {
                        if (!debug) {
                            localFile.delete();
                        }
                        runningJobs.remove(this);
                        listener.jobCompleted(this);
                    }
                } else {
                    if (job.getExitStatus() == 0) {
                        runningJobs.remove(this);
                        listener.jobCompleted(this);
                    } else {
                        GATjob = null;
                        runningJobs.remove(this);
                        listener.jobFailed(this, JobEndStatus.EXECUTION_FAILED);
                    }
                }
            } catch (Exception e) {
                ErrorManager.fatal(CALLBACK_PROCESSING_ERR + ": " + this, e);
            }
        } else if (newJobState == JobState.SUBMISSION_ERROR) {
            if (tracing) {
                Integer slot = (Integer) sd.getAttributes().get("slot");
                String host = getResourceNode().getHost();
                Tracer.freeSlot(host, slot);
            }

            try {
                if (usingGlobus && job.getInfo().get("resManError").equals("NO_ERROR")) {
                    runningJobs.remove(this);
                    listener.jobCompleted(this);
                } else {
                    GATjob = null;
                    runningJobs.remove(this);
                    listener.jobFailed(this, JobEndStatus.SUBMISSION_FAILED);
                }
            } catch (GATInvocationException e) {
                ErrorManager.fatal(CALLBACK_PROCESSING_ERR + ": " + this, e);
            }
        }
    }

    private JobDescription prepareJob() throws Exception {
        // Get the information related to the job
        MethodImplementation method = (MethodImplementation) this.impl;
        TaskParams taskParams = this.taskParams;
        String methodName = taskParams.getName();

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

        // Common arguments: language working_dir lib_path num_obsolete [obs1 ... obsN] tracing [event_type task_id slot_id]
        lArgs.add(lang);
        lArgs.add(getResourceNode().getWorkingDir());
        lArgs.add(getResourceNode().getLibPath());
        LinkedList<String> obsoleteFiles = getResource().clearObsoletes();
        if (obsoleteFiles != null) {
            lArgs.add("" + obsoleteFiles.size());
            for (String renaming : obsoleteFiles) {
                lArgs.add(renaming);
            }
        } else {
            lArgs.add("0");
        }
        lArgs.add(Boolean.toString(tracing));
        lArgs.add(getHostName());
        if (debug) {
            logger.debug("hostName " + getHostName());
        }
        if (tracing) {
            lArgs.add(String.valueOf(Tracer.getTaskEventsType())); // event type
            lArgs.add(String.valueOf(this.taskParams.getId() + 1)); // task id
            int slot = Tracer.getNextSlot(targetHost);
            lArgs.add(String.valueOf(slot)); // slot id
            sd.addAttribute("slot", slot);
        }

        // Language-dependent arguments: app_dir classpath pythonpath debug method_class method_name has_target num_params par_type_1 par_1 ... par_type_n par_n
        lArgs.add(getResourceNode().getAppDir());
        lArgs.add(getClasspath());
        lArgs.add(getPythonpath());
        lArgs.add(String.valueOf(debug));
        lArgs.add(method.getDeclaringClass());
        lArgs.add(methodName);
        lArgs.add(Boolean.toString(taskParams.hasTargetObject()));
        int numParams = taskParams.getParameters().length;
        if (taskParams.hasReturnValue()) {
            numParams--;
        }
        lArgs.add(Integer.toString(numParams));
        for (Parameter param : taskParams.getParameters()) {
            DataType type = param.getType();
            lArgs.add(Integer.toString(type.ordinal()));
            if (type == DataType.FILE_T || type == DataType.OBJECT_T) {
                DependencyParameter dPar = (DependencyParameter) param;
                DataAccessId dAccId = dPar.getDataAccessId();
                lArgs.add(dPar.getDataTarget());
                if (type == DataType.OBJECT_T) {
                    if (dAccId instanceof RAccessId) {
                        lArgs.add("R");
                    } else {
                        lArgs.add("W"); // for the worker to know it must write the object to disk
                    }
                }

            } else if (type == DataType.STRING_T) {
                BasicTypeParameter btParS = (BasicTypeParameter) param;
                // Check spaces
                String value = btParS.getValue().toString();
                int numSubStrings = value.split(" ").length;
                lArgs.add(Integer.toString(numSubStrings));
                lArgs.add(value);
            } else { // Basic types
                BasicTypeParameter btParB = (BasicTypeParameter) param;
                lArgs.add(btParB.getValue().toString());
            }

        }

        // Conversion vector -> array
        String[] arguments = new String[lArgs.size()];
        arguments = lArgs.toArray(arguments);
        try {
            sd.setArguments(arguments);
        } catch (NullPointerException e) {
            StringBuilder sb = new StringBuilder("Null argument parameter of job " + this.jobId + "(" + methodName + "@" + method.getDeclaringClass() + ")\n");
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
                            sb.append("\t Direction: " + "W").append("\n"); // for the worker to know it must write the object to disk
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
        sd.addAttribute(SoftwareDescription.WALLTIME_MAX, method.getRequirements().getWallClockLimit());
        if (method.getRequirements().getHostQueues().size() > 0) {
            sd.addAttribute(SoftwareDescription.JOB_QUEUE, method.getRequirements().getHostQueues().get(0));
        }
        sd.addAttribute("coreCount", method.getRequirements().getTotalComputingUnits());
        sd.addAttribute(SoftwareDescription.MEMORY_MAX, method.getRequirements().getMemorySize());
        //sd.addAttribute(SoftwareDescription.SANDBOX_ROOT, "/tmp/");

        sd.addAttribute(SoftwareDescription.SANDBOX_ROOT, getResourceNode().getWorkingDir());
        sd.addAttribute(SoftwareDescription.SANDBOX_USEROOT, "true");
        sd.addAttribute(SoftwareDescription.SANDBOX_DELETE, "false");

        /*sd.addAttribute(SoftwareDescription.SANDBOX_PRESTAGE_STDIN, "false");
         sd.addAttribute(SoftwareDescription.SANDBOX_POSTSTAGE_STDOUT, "false");
         sd.addAttribute(SoftwareDescription.SANDBOX_POSTSTAGE_STDERR, "false");*/
        if (debug) { // Set standard output file for job
            File outFile = GAT.createFile(context, "any:///" + JOBS_DIR + "job" + jobId + "_" + this.getHistory() + ".out");
            sd.setStdout(outFile);
        }

        if (debug || usingGlobus) {
            // Set standard error file for job
            File errFile = GAT.createFile(context, "any:///" + JOBS_DIR + "job" + jobId + "_" + this.getHistory() + ".err");
            sd.setStderr(errFile);
        }

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(RES_ATTR, ANY_PROT + targetUser + targetHost);
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
        //jd.setProcessCount(method.getRequirements().getProcessorCoreCount());
        return jd;
    }

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

    public String getHostName() {
        return getResourceNode().getName();
    }

}
