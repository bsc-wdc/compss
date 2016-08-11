/*
 *  A JAVAGAT adaptor for LoadLeveler+GPFS Clusters
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

package org.gridlab.gat.resources.cpi.loadleveler;

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


public class LoadLevelerResourceBrokerAdaptor extends ResourceBrokerCpi {
	
	public static String LOADL_PID = null;
	/*
	public static String LSB_JOBINDEX = null;
	public static String LSB_JOBRES_CALLBACK = null;
	public static String LSB_MCPU_HOSTS = null;
	public static String LSF_BINDIR = null;
	*/

    public static String getDescription() {
        return "The LoadLeveler ResourceBroker Adaptor implements the ResourceBroker using the Java ProcessBuilder facility.";
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
        return new String[] { "loadleveler", "fork", ""};
    }

    protected static Logger logger = LoggerFactory
            .getLogger(LoadLevelerResourceBrokerAdaptor.class);

    /**
     * This method constructs a LoadLevelerResourceBrokerAdaptor instance
     * corresponding to the passed GATContext.
     * 
     * @param gatContext
     *                A GATContext which will be used to broker resources
     */
    public LoadLevelerResourceBrokerAdaptor(GATContext gatContext, URI brokerURI)
            throws GATObjectCreationException {
        super(gatContext, brokerURI);

        // the brokerURI should point to the localhost else throw exception
        /* CDIAZ: The URI can refers to another host different from localhost 
        if (!brokerURI.refersToLocalHost()) {
            throw new GATObjectCreationException(
                    "The LoadLevelerResourceBrokerAdaptor doesn't refer to localhost, but to a remote host: "
                            + brokerURI.toString());
        }
        */
        
        String path = brokerURI.getUnresolvedPath();
        if (path != null && ! path.equals("")) {
            throw new GATObjectCreationException(
                    "The LoadLevelerResourceBrokerAdaptor does not understand the specified path: " + path);
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
                    "loadleveler broker could not get user home dir");
        }

        Sandbox sandbox = new Sandbox(gatContext, description, "localhost",
                home, true, true, false, false);

        LoadLevelerJob loadlevelerJob = new LoadLevelerJob(gatContext, description, sandbox);
        Job job = null;
        if (description instanceof WrapperJobDescription) {
            WrapperJobCpi tmp = new WrapperJobCpi(gatContext, loadlevelerJob,
                    listener, metricDefinitionName);
            listener = tmp;
            job = tmp;
        } else {
            job = loadlevelerJob;
        }
        if (listener != null && metricDefinitionName != null) {
            Metric metric = loadlevelerJob.getMetricDefinitionByName(metricDefinitionName)
                    .createMetric(null);
            loadlevelerJob.addMetricListener(listener, metric);
        }

        loadlevelerJob.setState(Job.JobState.PRE_STAGING);
        loadlevelerJob.waitForTrigger(Job.JobState.PRE_STAGING);
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

        // Directory where the loadleveler command will be executed
        java.io.File f = new java.io.File(sandbox.getSandboxPath());
        if (!f.exists()) {
           	throw new GATInvocationException("Unable to find directory " + f.getAbsolutePath());
        }
        
        // Check and set the environment for a blaunch command
        Map<String, Object> env = sd.getEnvironment();
        this.prepareBLaunchEnv(env);
        
        // Encapsulate the original command into a blaunch command
        String host = brokerURI.getHost();
        
        String newExe = this.getLoadLevelerCommand();
        String[] newArgs = this.getLoadLevelerArgs(host, exe, args);
           	
    	System.out.println("[AFTER] exe: " +  newExe);
        
    	System.out.println("[AFTER] llspawan.stdio args:");
    	for (int i=0; i<newArgs.length; i++)
    		System.out.print(" " + newArgs[i]);
    	System.out.println();
        
        ProcessBundle bundle = new ProcessBundle(description.getProcessCount(), newExe, newArgs, f, env);
                
        loadlevelerJob.setSubmissionTime();
        loadlevelerJob.setState(Job.JobState.SCHEDULED);
        try {
            loadlevelerJob.setState(Job.JobState.RUNNING);
            loadlevelerJob.waitForTrigger(Job.JobState.RUNNING);
            loadlevelerJob.setStartTime();
            bundle.startBundle();
            loadlevelerJob.setProcess(bundle);
            
            if (logger.isDebugEnabled()) {
                logger.debug("Job with PID: " + LOADL_PID + " is executed on host " + host);
            }
            
        } catch (IOException e) {
            throw new CommandNotFoundException("LoadLevelerResourceBrokerAdaptor", e);
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
                    loadlevelerJob.setErrorStream(forwarder);
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
                    loadlevelerJob.setOutputStream(forwarder);
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

        loadlevelerJob.monitorState();

        return job;
    }
    
    private void prepareBLaunchEnv(Map<String, Object> env) throws GATInvocationException {
    	    	
    	// Check required LSB environment variables
    	if (env != null)
    		LOADL_PID = (String) env.get("LOADL_PID");
    	if (LOADL_PID == null)
    		LOADL_PID = System.getenv("LOADL_PID");
    	if (LOADL_PID == null)
    	    throw new GATInvocationException(
                    "LOADL_PID environment varible not defined");
    	/*
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
    	*/
    }
    
    private String getLoadLevelerCommand() {
    	return "/usr/bin/llspawn.stdio";
    }
    
    private String[] getLoadLevelerArgs(String host, String exe, String[] args) {
    	    	
    	String[] argsTmp = new String[args.length + 2];
    	    	
    	argsTmp[0] = host;
    	argsTmp[1] = exe;    	
    	
    	for (int i=0; i<args.length; i++)
    		argsTmp[2+i] = args[i];
    	       	    	
    	System.out.println("llspawan.stdio args: ");
    	for (int i=0; i<argsTmp.length; i++)
    		System.out.print(" " + argsTmp[i]);
    	System.out.println();
    	
    	return argsTmp;
    }
    	
}
