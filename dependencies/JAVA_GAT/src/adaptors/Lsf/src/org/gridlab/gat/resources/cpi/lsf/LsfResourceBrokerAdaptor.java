/*
 *  A JAVAGAT adaptor for LSF+GPFS Clusters
 *  
 *  	Author: Carlos Díaz
 *      Contact: support-compss@bsc.es
 *
 *	Barcelona Supercomputing Center
 * 	www.bsc.es
 *	
 *	Grid Computing and Clusters
 *	www.bsc.es/computer-sciences/grid-computing
 *
 *	Barcelona, 2014
 */

package org.gridlab.gat.resources.cpi.lsf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.gridlab.gat.CommandNotFoundException;
import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.engine.util.ProcessBundle;
import org.gridlab.gat.engine.util.StreamForwarder;
import org.gridlab.gat.monitoring.Metric;
import org.gridlab.gat.monitoring.MetricListener;
import org.gridlab.gat.resources.AbstractJobDescription;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.resources.WrapperJobDescription;
import org.gridlab.gat.resources.cpi.ResourceBrokerCpi;
import org.gridlab.gat.resources.cpi.Sandbox;
import org.gridlab.gat.resources.cpi.WrapperJobCpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LsfResourceBrokerAdaptor extends ResourceBrokerCpi {
	
	public static String LSB_JOBID = null;
	public static String LSB_JOBINDEX = null;
	public static String LSB_JOBRES_CALLBACK = null;
	public static String LSB_MCPU_HOSTS = null;
	public static String LSF_BINDIR = null;

    public static String getDescription() {
        return "The LSF ResourceBroker Adaptor implements the ResourceBroker using the Java ProcessBuilder facility.";
    }

    public static Map<String, Boolean> getSupportedCapabilities() {
        Map<String, Boolean> capabilities = ResourceBrokerCpi
                .getSupportedCapabilities();
        capabilities.put("beginMultiJob", true);
        capabilities.put("endMultiJob", true);
        capabilities.put("submitJob", true);

        return capabilities;
    }
    
    public static String[] getSupportedSchemes() {
        return new String[] { "lsf", "fork", ""};
    }

    protected static Logger logger = LoggerFactory
            .getLogger(LsfResourceBrokerAdaptor.class);

    /**
     * This method constructs a LsfResourceBrokerAdaptor instance
     * corresponding to the passed GATContext.
     * 
     * @param gatContext
     *                A GATContext which will be used to broker resources
     */
    public LsfResourceBrokerAdaptor(GATContext gatContext, URI brokerURI)
            throws GATObjectCreationException {
        super(gatContext, brokerURI);

        // the brokerURI should point to the localhost else throw exception
        /* CDIAZ: The URI can refers to another host different from localhost 
        if (!brokerURI.refersToLocalHost()) {
            throw new GATObjectCreationException(
                    "The LsfResourceBrokerAdaptor doesn't refer to localhost, but to a remote host: "
                            + brokerURI.toString());
        }
        */
        
        String path = brokerURI.getUnresolvedPath();
        if (path != null && ! path.equals("")) {
            throw new GATObjectCreationException(
                    "The LsfResourceBrokerAdaptor does not understand the specified path: " + path);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.resources.ResourceBroker#submitJob(org.gridlab.gat.resources.JobDescription)
     */
    public Job submitJob(AbstractJobDescription abstractDescription,
            MetricListener listener, String metricDefinitionName)
            throws GATInvocationException {

        if (!(abstractDescription instanceof JobDescription)) {
            throw new GATInvocationException(
                    "can only handle JobDescriptions: "
                            + abstractDescription.getClass());
        }

        JobDescription description = (JobDescription) abstractDescription;

        SoftwareDescription sd = description.getSoftwareDescription();        

        if (sd == null) {
            throw new GATInvocationException(
                    "The job description does not contain a software description");
        }

        if (description.getProcessCount() < 1) {
            throw new GATInvocationException(
                    "Adaptor cannot handle: process count < 1: "
                            + description.getProcessCount());
        }

        if (description.getResourceCount() != 1) {
            throw new GATInvocationException(
                    "Adaptor cannot handle: resource count > 1: "
                            + description.getResourceCount());
        }

        String home = System.getProperty("user.home");
        if (home == null) {
            throw new GATInvocationException(
                    "lsf broker could not get user home dir");
        }

        Sandbox sandbox = new Sandbox(gatContext, description, "localhost",
                home, true, true, false, false);

        LsfJob lsfJob = new LsfJob(gatContext, description, sandbox);
        Job job = null;
        if (description instanceof WrapperJobDescription) {
            WrapperJobCpi tmp = new WrapperJobCpi(gatContext, lsfJob,
                    listener, metricDefinitionName);
            listener = tmp;
            job = tmp;
        } else {
            job = lsfJob;
        }
        if (listener != null && metricDefinitionName != null) {
            Metric metric = lsfJob.getMetricDefinitionByName(metricDefinitionName)
                    .createMetric(null);
            lsfJob.addMetricListener(listener, metric);
        }

        lsfJob.setState(Job.JobState.PRE_STAGING);
        lsfJob.waitForTrigger(Job.JobState.PRE_STAGING);
        sandbox.prestage();

        String exe;
        if (sandbox.getResolvedExecutable() != null) {
            exe = sandbox.getResolvedExecutable().getPath();
            // try to set the executable bit, it might be lost
            /* CDIAZ: The command "exe" can be also in a remote host
             * 		  The command must have the right permissions in the remote host
            try {
                new CommandRunner("chmod", "+x", exe);
            } catch (Throwable t) {
                // ignore
            }
            */
        } else {
            exe = getExecutable(description);
        }       
          
        String[] args = getArgumentsArray(description);        

        // Directory where the lsf command will be executed
        java.io.File f = new java.io.File(sandbox.getSandboxPath());
        if (!f.exists()) {
           	throw new GATInvocationException("Unable to find directory " + f.getAbsolutePath());
        }
        
        // Check and set the environment for a blaunch command
        Map<String, Object> env = sd.getEnvironment();
        this.prepareBLaunchEnv(env);
        
        // Encapsulate the original command into a blaunch command
        String host = brokerURI.getHost();
        String blExe = this.getBlaunchCommand();
        String[] blArgs = this.getBlaunchArgs(host, exe, args);
        
        ProcessBundle bundle = new ProcessBundle(description.getProcessCount(), blExe, blArgs, f, env);
                
        lsfJob.setSubmissionTime();
        lsfJob.setState(Job.JobState.SCHEDULED);
        try {
            lsfJob.setState(Job.JobState.RUNNING);
            lsfJob.waitForTrigger(Job.JobState.RUNNING);
            lsfJob.setStartTime();
            bundle.startBundle();
            lsfJob.setProcess(bundle);
        } catch (IOException e) {
            throw new CommandNotFoundException("LsfResourceBrokerAdaptor", e);
        }

        if (!sd.streamingStderrEnabled()) {
            // read away the stderr

            try {
                if (sd.getStderr() != null) {
                    OutputStream err = GAT.createFileOutputStream(gatContext, sd.getStderr());
                    // to file
                    StreamForwarder forwarder = new StreamForwarder(bundle.getStderr(), err, sd
                            .getExecutable()
                            + " [stderr]");
                    lsfJob.setErrorStream(forwarder);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Created stderr forwarder to file " + sd.getStderr());
                    }
                } else {
                    // or throw it away
                    new StreamForwarder(bundle.getStderr(), null, sd
                            .getExecutable()
                            + " [stderr]");
                }
            } catch (GATObjectCreationException e) {
                throw new GATInvocationException(
                        "Unable to create file output stream for stderr!", e);
            }
        }

        if (!sd.streamingStdoutEnabled()) {
            // read away the stdout
            try {
                if (sd.getStdout() != null) {
                    // to file
                    OutputStream out = GAT.createFileOutputStream(gatContext, sd.getStdout());
                    StreamForwarder forwarder = new StreamForwarder(bundle.getStdout(), out, sd
                            .getExecutable()
                            + " [stdout]");
                    lsfJob.setOutputStream(forwarder);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Created stdout forwarder to file " + sd.getStdout());
                    }
                } else {
                    // or throw it away
                    new StreamForwarder(bundle.getStdout(), null, sd
                            .getExecutable()
                            + " [stdout]");
                }
            } catch (GATObjectCreationException e) {
                throw new GATInvocationException(
                        "Unable to create file output stream for stdout!", e);
            }
        }
        
        if (!sd.streamingStdinEnabled() && sd.getStdin() != null) {
            // forward the stdin from file
            try {
                InputStream in = GAT.createFileInputStream(gatContext, sd.getStdin());
                bundle.setStdin(sd.getExecutable(), in);
            } catch (GATObjectCreationException e) {
                throw new GATInvocationException(
                        "Unable to create file input stream for stdin!", e);
            }
        }

        lsfJob.monitorState();

        return job;
    }
    
    private void prepareBLaunchEnv(Map<String, Object> env) throws GATInvocationException {
    	    	
    	// Check required LSB environment variables
    	if (env != null)
    		LSB_JOBID = (String) env.get("LSB_JOBID");
    	if (LSB_JOBID == null)
    		LSB_JOBID = System.getenv("LSB_JOBID");
    	if (LSB_JOBID == null)
    	    throw new GATInvocationException(
                    "LSB_JOBID environment varible not defined");
    	
    	if (env != null)
    		LSB_JOBINDEX = (String) env.get("LSB_JOBINDEX");
    	if (LSB_JOBINDEX == null)
    		LSB_JOBINDEX = System.getenv("LSB_JOBINDEX");
    	if (LSB_JOBINDEX == null)
    	    throw new GATInvocationException(
                    "LSB_JOBINDEX environment varible not defined");
    	
    	if (env != null)
    		LSB_JOBRES_CALLBACK = (String) env.get("LSB_JOBRES_CALLBACK");
    	if (LSB_JOBRES_CALLBACK == null)
    		LSB_JOBRES_CALLBACK = System.getenv("LSB_JOBRES_CALLBACK");
    	if (LSB_JOBRES_CALLBACK == null)
    	    throw new GATInvocationException(
                    "LSB_JOBRES_CALLBACK environment varible not defined");

    	if (env != null)
    		LSB_MCPU_HOSTS = (String) env.get("LSB_MCPU_HOSTS");
    	if (LSB_MCPU_HOSTS == null)
    		LSB_MCPU_HOSTS = System.getenv("LSB_MCPU_HOSTS");
    	if (LSB_MCPU_HOSTS == null)
    	    throw new GATInvocationException(    	    		
                    "LSB_MCPU_HOSTS environment varible not defined");
    	
    	if (env != null)
    		LSF_BINDIR = (String) env.get("LSF_BINDIR");
    	if (LSF_BINDIR == null)
    		LSF_BINDIR = System.getenv("LSF_BINDIR");
    	if (LSF_BINDIR == null)
    	    throw new GATInvocationException(
                    "LSF_BINDIR environment varible not defined");
    	
    }
    
    private String getBlaunchCommand() {
    	return LSF_BINDIR + "/blaunch"; 
    }
    
    private String[] getBlaunchArgs(String host, String exe, String[] args) {    	
    	String[] argsTmp = new String[args.length + 3];
    	
    	argsTmp[0] = host;
    	argsTmp[1] = "-use-login-shell";
    	argsTmp[2] = exe;
    	
    	for (int i=0; i<args.length; i++)
    		argsTmp[3+i] = args[i];
    	
    	return argsTmp;    	
    }
    	
}
