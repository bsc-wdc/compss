package org.gridlab.gat.resources.cpi.sshlsf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.advert.Advertisable;
import org.gridlab.gat.monitoring.MetricEvent;
import org.gridlab.gat.monitoring.MetricListener;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.resources.cpi.Sandbox;
import org.gridlab.gat.resources.cpi.SerializedSimpleJobBase;
import org.gridlab.gat.resources.cpi.SimpleJobBase;
import org.gridlab.gat.resources.cpi.sshlsf.SshLSFJob;
import org.gridlab.gat.resources.cpi.sshlsf.SshLsfResourceBrokerAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SshLSFJob extends SimpleJobBase implements MetricListener {
    
    protected static Logger logger = LoggerFactory.getLogger(SshLSFJob.class);

    private static final long serialVersionUID = 1L;

    private final ResourceBroker jobHelper;
    
    private final GATContext subContext;
    
    protected SshLSFJob(GATContext gatContext, URI brokerURI, JobDescription jobDescription, 
    		Sandbox sandbox, ResourceBroker jobHelper, String returnValueFile) {
		super(gatContext, brokerURI, jobDescription, sandbox, returnValueFile);
		this.jobHelper = jobHelper;
		subContext = SshLsfResourceBrokerAdaptor.getSubContext(gatContext);
	}

    /**
     * Constructor for unmarshalled jobs.
     */

	public SshLSFJob(GATContext gatContext, SerializedSimpleJobBase sj)
			throws GATObjectCreationException {
		super(gatContext, sj);

		subContext = SshLsfResourceBrokerAdaptor.getSubContext(gatContext);

		try {
			jobHelper = SshLsfResourceBrokerAdaptor.subResourceBroker(
					subContext, brokerURI);
		} catch (URISyntaxException e) {
			throw new GATObjectCreationException(
					"Could not create broker to get Slurm job status", e);
		}
		startListener();
	}

	protected synchronized void setJobID(String jobID) {
		super.setJobID(jobID);
		notifyAll();
	}

	public static Advertisable unmarshal(GATContext context, String s)
			throws GATObjectCreationException {
		return SimpleJobBase.unmarshal(context, s,
				SshLSFJob.class.getClassLoader());
	}

	protected void setSoft(SoftwareDescription Soft) {
		super.setSoft(Soft);
	}

	protected void setState(JobState state) {
		super.setState(state);
	}

	protected void startListener() {
		super.startListener();
	}

	boolean jobStateBusy = false;

	protected void getJobState(String jobID) throws GATInvocationException {
		synchronized (this) {
			while (jobStateBusy) {
				try {
					wait();
				} catch (InterruptedException e) {
					// ignored
				}
			}
			jobStateBusy = true;
		}

		JobState resultState;

		try {
			if (state == JobState.POST_STAGING || state == JobState.STOPPED
					|| state == JobState.SUBMISSION_ERROR) {
				return;
			}

			logger.debug("Getting task status in setState()");

			// getting the status via ssh ... squeue
			java.io.File squeueResultFile = null;
			try {
				// Create qstat job
				SoftwareDescription sd = new SoftwareDescription();
				// Use /bin/sh, so that $USER gets expanded.
				sd.setExecutable("/bin/sh");
				sd.setArguments("-c", "bjobs -noheader "+ jobID+ " | awk {' print $3 '}");
				sd.addAttribute(SoftwareDescription.SANDBOX_USEROOT, "true");
				squeueResultFile = java.io.File.createTempFile("GAT", "tmp");
				try {
					sd.setStdout(GAT.createFile(subContext,
							new URI("file:///"
									+ squeueResultFile.getAbsolutePath()
											.replace(File.separatorChar, '/'))));
				} catch (Throwable e1) {
					throw new GATInvocationException(
							"Could not create GAT object for temporary "
									+ squeueResultFile.getAbsolutePath(), e1);
				}
				JobDescription jd = new JobDescription(sd);
				Job job = jobHelper.submitJob(jd, this, "job.status");
				synchronized (job) {
					while (job.getState() != Job.JobState.STOPPED
							&& job.getState() != Job.JobState.SUBMISSION_ERROR) {
						try {
							job.wait();
						} catch (InterruptedException e) {
							// ignore
						}
					}
				}
				if (job.getState() != Job.JobState.STOPPED
						|| job.getExitStatus() != 0) {
					throw new GATInvocationException(
							"Could not submit squeue job " + sd.toString());
				}

				// submit success.
				BufferedReader in = new BufferedReader(new FileReader(
						squeueResultFile.getAbsolutePath()));
				String status = in.readLine();
				// status is now null when EOF is seen, which may happen if the
				// job is not present
				// or finished. Set to "" in this case. --Ceriel
				if (status == null) {
					status = "";
				}
				if (logger.isDebugEnabled()) {
					logger.debug("squeue line: " + status);
				}
				resultState = mapLSFStatetoGAT(status);
			} catch (IOException e) {
				logger.debug("retrieving job status sshslurmjob failed", e);
				throw new GATInvocationException(
						"Unable to retrieve the Job Status", e);
			} finally {
				squeueResultFile.delete();
			}
		} finally {
			synchronized (this) {
				jobStateBusy = false;
				notifyAll();
			}
		}

		if (resultState != JobState.STOPPED) {
			setState(resultState);
		} else {
			setState(JobState.POST_STAGING);
		}

	}

	private boolean sawJob = false;
	private int missedJob = 0;

	private JobState mapLSFStatetoGAT(String slurmState) {
		// TODO: Change to parse LSFJob state
		if (slurmState == null) {
			logger.error("Error in mapLSFStatetoGAT: no SlurmState returned");
			return JobState.UNKNOWN;
		} else {
			if (slurmState.isEmpty()) {
				if (logger.isDebugEnabled()) {
					logger.debug("no job status information for '" + this.jobID
							+ "' found.");
				}
				// if we saw it before, assume it is finished now.
				if (sawJob) {
					if (logger.isDebugEnabled()) {
						logger.debug("But is was present earlier, so we assume it finished.");
					}
					return JobState.STOPPED;
				}
				missedJob++;
				if (missedJob > 1) {
					// arbitrary threshold. Allow for a small gap between
					// successful
					// submission of the job and its appearance in squeue
					// output. But it may
					// also not appear because it is already finished ...
					if (logger.isDebugEnabled()) {
						logger.debug("But is was not present for a while, so we assume it finished.");
					}
					return JobState.STOPPED;
				}
				// Return current state.
				return state;
			} else {
				sawJob = true;
				if (logger.isDebugEnabled()) {
					logger.debug("slurmState = " + slurmState);
				}
				if (slurmState.equals("EXIT") || slurmState.equals("DONE")) {
					return JobState.STOPPED;
				}
				
				if (slurmState.equals("PEND")) {
					// pending
					return JobState.SCHEDULED;
				}
				if (slurmState.equals("RUN")) {
					// pending
					return JobState.RUNNING;
				}
				if (slurmState.equals("PSUSP")|| slurmState.equals("USUSP") || slurmState.equals("SSUSP")) {
					// suspended
					return JobState.ON_HOLD;
				}
				return JobState.UNKNOWN;
			}
		}
	}

	protected void kill(String jobID) {
		try {
			// Create qdel job
			SoftwareDescription sd = new SoftwareDescription();
			sd.setExecutable("bkill");
			sd.setArguments(jobID);
			sd.addAttribute(SoftwareDescription.SANDBOX_USEROOT, "true");
			sd.addAttribute(SoftwareDescription.SANDBOX_ROOT,
					sandbox.getSandboxPath());
			sd.addAttribute(SoftwareDescription.STOP_ON_EXIT, "false");
			JobDescription jd = new JobDescription(sd);
			Job job = jobHelper.submitJob(jd, this, "job.status");
			synchronized (job) {
				while (job.getState() != Job.JobState.STOPPED
						&& job.getState() != Job.JobState.SUBMISSION_ERROR) {
					try {
						job.wait();
					} catch (InterruptedException e) {
						// ignore
					}
				}
			}
			if (job.getState() != Job.JobState.STOPPED
					|| job.getExitStatus() != 0) {
				throw new GATInvocationException("Could not submit scancel job");
			}
		} catch (Throwable e) {
			logger.info("Failed to stop sshSlurm job: " + jobID, e);
		}
	}

	protected synchronized Integer retrieveExitStatus(String returnValueFile) {

		String marker = "retvalue = ";
		String line = null;
		int rc = -1;

		BufferedReader rExit = null;
		java.io.File fi = new java.io.File(returnValueFile);
		try {
			rExit = new BufferedReader(new FileReader(fi));

			line = rExit.readLine().toString();

			String rc_String = line.substring(marker.length());
			if (rc_String != null) {
				rc = Integer.parseInt(rc_String);
			}
			return new Integer(rc);
		} catch (Exception e) {
			logger.debug("SshLsf adaptor: error reading exit value file " + returnValueFile
					, e);
			return null;
		} finally {
			try {
				rExit.close();
			} catch (Throwable e) {
				// ignore
			}
			fi.delete();
		}
	}

	@Override
	public void processMetricEvent(MetricEvent event) {
		if (event.getValue().equals(Job.JobState.STOPPED)
				|| event.getValue().equals(Job.JobState.SUBMISSION_ERROR)) {
			synchronized (event.getSource()) {
				event.getSource().notifyAll();
			}
		}
	}
}
