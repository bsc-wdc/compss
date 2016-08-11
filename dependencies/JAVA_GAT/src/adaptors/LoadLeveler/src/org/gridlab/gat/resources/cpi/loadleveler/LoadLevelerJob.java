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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.engine.GATEngine;
import org.gridlab.gat.engine.util.FileWaiter;
import org.gridlab.gat.engine.util.ProcessBundle;
import org.gridlab.gat.engine.util.StreamForwarder;
import org.gridlab.gat.io.File;
import org.gridlab.gat.monitoring.Metric;
import org.gridlab.gat.monitoring.MetricDefinition;
import org.gridlab.gat.monitoring.MetricEvent;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.cpi.JobCpi;
import org.gridlab.gat.resources.cpi.Sandbox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class LoadLevelerJob extends JobCpi {

    protected static Logger logger = LoggerFactory.getLogger(LoadLevelerJob.class);

    private ProcessBundle p;

    private int exitStatus = 0;

    private MetricDefinition statusMetricDefinition;

    private Metric statusMetric;

    private int processID;
    
    private final String triggerDirectory;
    
    private final String jobName;
    
    private StreamForwarder outputStreamFile;
    
    private StreamForwarder errorStreamFile;
    
    protected LoadLevelerJob(GATContext gatContext, JobDescription description,
            Sandbox sandbox) {
        super(gatContext, description, sandbox);
        // Tell the engine that we provide job.status events
        HashMap<String, Object> returnDef = new HashMap<String, Object>();
        returnDef.put("status", JobState.class);
        statusMetricDefinition = new MetricDefinition("job.status",
                MetricDefinition.DISCRETE, "JobState", null, null, returnDef);
        statusMetric = statusMetricDefinition.createMetric(null);
        registerMetric("getJobStatus", statusMetricDefinition);
        triggerDirectory = description.getSoftwareDescription().getStringAttribute(
                "triggerDirectory", null);
        jobName = description.getSoftwareDescription().getStringAttribute(
                "job.name", null);
    }
    
    void setErrorStream(StreamForwarder err) {
        errorStreamFile = err;
    }
        
    void setOutputStream(StreamForwarder out) {
        outputStreamFile = out;
    }
    
    private FileWaiter waiter = null;
    
    // Wait for the creation of a special file (by the application).
    void waitForTrigger(JobState state) throws GATInvocationException {
        
        if (triggerDirectory == null) {
            return;
        }
        if (jobName == null) {
            return;
        }
        
        if (waiter == null) {
            try {
		waiter = FileWaiter.createFileWaiter(GAT.createFile(gatContext, triggerDirectory));
	    } catch (GATObjectCreationException e) {
		throw new GATInvocationException("Could not create", e);
	    }
        }
        
        String filename = jobName + "." + state.toString().substring(0,3);
        File file;
	try {
	    file = GAT.createFile(gatContext, triggerDirectory + "/" + filename);
	} catch (GATObjectCreationException e) {
	    throw new GATInvocationException("Could not create");
	}
	
	if (logger.isDebugEnabled()) {
	    logger.debug("Waiting for " + filename + " in directory " + triggerDirectory);
	}
	
        waiter.waitFor(filename);
        
	if (logger.isDebugEnabled()) {
	    logger.debug("Finished waiting for " + filename + " in directory " + triggerDirectory);
	}
        

        synchronized(this.getClass()) {
            if (! file.delete()) {
        	if (logger.isDebugEnabled()) {
        	    logger.debug("Could not remove " + file.toGATURI());
        	}
            }
        }
    }

    protected void setProcess(ProcessBundle bundle) {
        this.p = bundle;
        processID = bundle.getProcessID();
    }

    protected void setState(JobState state) {
        if(state == this.state) {
            //state already set to this value
            return;
        }
        
        this.state = state;
        MetricEvent v = new MetricEvent(this, state, statusMetric, System
                .currentTimeMillis());
        fireMetric(v);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.resources.Job#getInfo()
     */
    public synchronized Map<String, Object> getInfo()
            throws GATInvocationException {
        HashMap<String, Object> m = new HashMap<String, Object>();

        // update state
        getState();

        m.put("adaptor.job.id", processID);
        m.put("state", state.toString());
        if (state != JobState.RUNNING) {
            m.put("hostname", null);
        } else {
            m.put("hostname", GATEngine.getLocalHostName());
        }
        if (state == JobState.INITIAL || state == JobState.UNKNOWN) {
            m.put("submissiontime", null);
        } else {
            m.put("submissiontime", submissiontime);
        }
        if (state == JobState.INITIAL || state == JobState.UNKNOWN
                || state == JobState.SCHEDULED) {
            m.put("starttime", null);
        } else {
            m.put("starttime", starttime);
        }
        if (state != JobState.STOPPED) {
            m.put("stoptime", null);
        } else {
            m.put("stoptime", stoptime);
        }
        m.put("poststage.exception", postStageException);
        m.put("resourcebroker", "loadleveler");
        m.put("exitvalue", "" + exitStatus);
        if (deleteException != null) {
            m.put("delete.exception", deleteException);
        }
        if (wipeException != null) {
            m.put("wipe.exception", wipeException);
        }
        return m;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.resources.Job#getExitStatus()
     */
    public synchronized int getExitStatus() throws GATInvocationException {
        if (state != JobState.STOPPED) {
            throw new GATInvocationException("not in STOPPED state");
        }
        return exitStatus;
    }

    public void stop() throws GATInvocationException {
        stop(gatContext.getPreferences().containsKey("job.stop.poststage")
                && gatContext.getPreferences().get("job.stop.poststage")
                        .equals("false"), true);
    }
    
    private boolean stopping = false;

    private void stop(boolean skipPostStage, boolean kill)
            throws GATInvocationException {
	synchronized(this) {
	    if (stopping) {
		return;
	    }
	    stopping = true;
	    if (state == JobState.POST_STAGING
		    || state == JobState.STOPPED
		    || state == JobState.SUBMISSION_ERROR) {
		return;
	    }
	}

	try {
	    p.closeInput();
	} catch (Throwable e) {
	    // ignored
	}

	if (kill) {
	    p.kill();
	}

	if (outputStreamFile != null) {
	    outputStreamFile.waitUntilFinished();
	    try {
		outputStreamFile.close();
	    } catch(Throwable e) {
		// ignored
	    }
	}
	if (errorStreamFile != null) {
	    errorStreamFile.waitUntilFinished();
	    try {
		errorStreamFile.close();
	    } catch(Throwable e) {
		// ignored
	    }
	}

        if (!skipPostStage) {
            setState(JobState.POST_STAGING);
            if (! kill) {
        	waitForTrigger(JobState.POST_STAGING);
            }
            sandbox.retrieveAndCleanup(this);
        }

        setStopTime();
        setState(JobState.STOPPED);
        finished();
    }

    public OutputStream getStdin() throws GATInvocationException {
        if (jobDescription.getSoftwareDescription().streamingStdinEnabled()) {
            return p.getStdin();
        } else {
            throw new GATInvocationException("stdin streaming is not enabled!");
        }
    }

    public InputStream getStdout() throws GATInvocationException {
        if (jobDescription.getSoftwareDescription().streamingStdoutEnabled()) {
            return p.getStdout();
        } else {
            throw new GATInvocationException("stdout streaming is not enabled!");
        }
    }

    public InputStream getStderr() throws GATInvocationException {
        if (jobDescription.getSoftwareDescription().streamingStderrEnabled()) {
            return p.getStderr();
        } else {
            throw new GATInvocationException("stderr streaming is not enabled!");
        }
    }

    protected void monitorState() {
        new StateMonitor();
    }

    class StateMonitor extends Thread {

        StateMonitor() {
            setName("loadleveler state monitor: "
                    + jobDescription.getSoftwareDescription().getExecutable());
            setDaemon(true);
            start();
        }

        public void run() {
            exitStatus = p.getExitStatus();
            try {
                LoadLevelerJob.this.stop(false, false);
            } catch (GATInvocationException e) {
                e.printStackTrace();
                if (logger.isDebugEnabled()) {
                    logger.debug("unable to stop job: " + e);
                }
            }
        }
    }

}
