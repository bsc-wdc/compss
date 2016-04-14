package org.gridlab.gat.resources.cpi.sshlsf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.gridlab.gat.AdaptorNotApplicableException;
import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.URI;
import org.gridlab.gat.engine.GATEngine;
import org.gridlab.gat.engine.util.SshHelper;
import org.gridlab.gat.monitoring.Metric;
import org.gridlab.gat.monitoring.MetricEvent;
import org.gridlab.gat.monitoring.MetricListener;
import org.gridlab.gat.resources.AbstractJobDescription;
import org.gridlab.gat.resources.HardwareResourceDescription;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.ResourceDescription;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.resources.WrapperJobDescription;
import org.gridlab.gat.resources.cpi.ResourceBrokerCpi;
import org.gridlab.gat.resources.cpi.Sandbox;
import org.gridlab.gat.resources.cpi.WrapperJobCpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SshLsfResourceBrokerAdaptor extends ResourceBrokerCpi implements
        MetricListener {

    protected static Logger logger = LoggerFactory
            .getLogger(SshLsfResourceBrokerAdaptor.class);

    public static Map<String, Boolean> getSupportedCapabilities() {
        Map<String, Boolean> capabilities = ResourceBrokerCpi
                .getSupportedCapabilities();
        capabilities.put("submitJob", true);

        return capabilities;
    }

    static final String SSHLSF_NATIVE_FLAGS = "sshlsf.native.flags";
    static final String SSHLSF_SCRIPT = "sshlsf.script";
    static final String SSHLSF_SUBMITTER_SCHEME = "sshlsf.submitter.scheme";

    public static String[] getSupportedSchemes() {
        return new String[] { "sshlsf" };
    }

    public static Preferences getSupportedPreferences() {
        Preferences p = ResourceBrokerCpi.getSupportedPreferences();
        p.put(SSHLSF_NATIVE_FLAGS, "");
        p.put(SSHLSF_SCRIPT, "");
        p.put(SSHLSF_SUBMITTER_SCHEME, "ssh");
        return p;
    }

    public static void init() {
        GATEngine.registerUnmarshaller(SshLSFJob.class);
    }

    static GATContext getSubContext(GATContext context) {
        // Create a gatContext that can be used to submit sbatch, squeue, etc
        // commands.
        Preferences p = context.getPreferences();
        Preferences prefs = new Preferences();
        if (p != null) {
            prefs.putAll(p);
        }
        prefs.remove("resourcebroker.adaptor.name");
        prefs.remove("sshtrilead.stoppable");
        GATContext subContext = (GATContext) context.clone();
        subContext.removePreferences();
        subContext.addPreferences(prefs);
        return subContext;
    }

    static ResourceBroker subResourceBroker(GATContext context, URI broker)
            throws URISyntaxException, GATObjectCreationException {
        String subScheme;
        if (context.getPreferences() != null) {
            subScheme = (String) context.getPreferences().get(
                    SSHLSF_SUBMITTER_SCHEME, "ssh");
        } else {
            subScheme = "ssh";
        }
        URI subBroker = broker.setScheme(subScheme);
        return GAT.createResourceBroker(context, subBroker);
    }

    //private final ResourceBroker subBroker;
    //private final GATContext subContext;

    public SshLsfResourceBrokerAdaptor(GATContext gatContext, URI brokerURI)
            throws GATObjectCreationException, AdaptorNotApplicableException {

        super(gatContext, brokerURI);
        /*subContext = getSubContext(gatContext);
        if (logger.isDebugEnabled()) {
			logger.debug("Subcontext: " + subContext.toString());
		}
        try {
            subBroker = subResourceBroker(subContext, brokerURI);
        } catch (Throwable e) {
            throw new GATObjectCreationException(
                    "Could not create broker to submit LSF jobs", e);
        }
		// Detect if subBroker can actually submit LSF jobs.
		// Check if squeue command exists?
		// So, execute "which bjobs" and check exit status, should be 0.
		/*SoftwareDescription sd = new SoftwareDescription();
		sd.setExecutable("which");
		sd.setArguments("bjobs");
		sd.enableStreamingStderr(true);
		sd.enableStreamingStdout(true);
		
		if (logger.isDebugEnabled()) {
			logger.debug("Submitting test job: " + sd);
		}
		JobDescription jd = new JobDescription(sd);
		Job job;
		try {
			job = subBroker.submitJob(jd, this, "job.status");
		} catch (Throwable e) {
			throw new GATObjectCreationException(
					"broker to submit Slurm jobs cannot submit test job", e);
		}
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
		try {
			if (job.getExitStatus() != 0) {
				throw new GATObjectCreationException(
						"broker to submit Slurm jobs could not find Slurm command");
			}
		} catch (GATInvocationException e) {
			throw new GATObjectCreationException(
					"broker to submit Slurm jobs could not get exit status", e);
		}*/
    }

    public Job submitJob(AbstractJobDescription abstractDescription,
            MetricListener listener, String metricDefinitionName)
            throws GATInvocationException {

        if (!(abstractDescription instanceof JobDescription)) {
            throw new GATInvocationException(
                    "can only handle JobDescriptions: "
                            + abstractDescription.getClass());
        }

	    JobDescription description = (JobDescription) ((JobDescription) abstractDescription).clone();
        SoftwareDescription sd = description.getSoftwareDescription();

        int nproc = description.getProcessCount();

        if (sd.streamingStderrEnabled() || sd.streamingStdinEnabled()
                || sd.streamingStdoutEnabled()) {
            throw new GATInvocationException(
                    "Streaming I/O not supported by SshLSF adaptor");
        }

        String authority = getAuthority();
        if (authority == null) {
            authority = "localhost";
        }
      
        
        // Create filename for return value.
        String returnValueFile = ".rc." + Math.random();
        /*java.io.File jobScriptFile = createJobScript(description);
    	java.io.File jobStarterFile = null;
    	if (nproc > 1) {
    	    jobStarterFile = createJobStarter(description, nproc, jobScriptFile);
    	}*/
        java.io.File bsubFile = createBsubScript(description,
                returnValueFile, nproc);
        if (logger.isDebugEnabled()) {
			logger.debug("**** Adding pre and post staging:\n  " + sd);
			
		}
        try {
            // Careful, you may be running on windows, so add a scheme.
            sd.addPostStagedFile(GAT.createFile(gatContext, new URI(
                    returnValueFile)));
        } catch (Throwable e) {
            throw new GATInvocationException("Error in file staging", e);
        }
        
        ResourceBroker subBroker;
		try {
            subBroker = subResourceBroker(getSubContext(gatContext), brokerURI);
        } catch (Throwable e) {
            throw new GATInvocationException(
                    "Could not create subbroker to submit LSF jobs through ssh", e);
        }
		
		Sandbox sandbox = new Sandbox(gatContext, description, authority, null,
                true, true, true, true);
        
		SshLSFJob sshLSFJob = new SshLSFJob(gatContext, brokerURI,
                description, sandbox, subBroker, returnValueFile);

        Job job = null;
        if (description instanceof WrapperJobDescription) {
            WrapperJobCpi tmp = new WrapperJobCpi(gatContext, sshLSFJob,
                    listener, metricDefinitionName);
            listener = tmp;
            job = tmp;
        } else {
            job = sshLSFJob;
        }
        if (listener != null && metricDefinitionName != null) {
            Metric metric = sshLSFJob.getMetricDefinitionByName(
                    metricDefinitionName).createMetric(null);
            sshLSFJob.addMetricListener(listener, metric);
        }
        if (logger.isDebugEnabled()) {
			logger.debug("*******We are going to perform the ssh submission: ");
			
		}
        String jobid = sshLsfSubmission(sshLSFJob, description, bsubFile, subBroker,
                sandbox);
        
        if (jobid != null) {
            sshLSFJob.setState(Job.JobState.SCHEDULED);
            sshLSFJob.setSoft(description.getSoftwareDescription());
            sshLSFJob.setJobID(jobid);
            sshLSFJob.startListener();
        } else {
            //sandbox.removeSandboxDir();
            throw new GATInvocationException("Could not submit sshLSF job");
        }
        bsubFile.delete();
        return job;
    }

	private java.io.File createBsubScript(JobDescription description,
			String returnValueFile, int nproc)
			throws GATInvocationException {

		// Adding bsub options
		String Queue = null;
		long Time = -1;
		Integer cpus = null;
		String jobname = null;
		java.io.File temp;
		LSFScriptWriter job = null;
		HashMap<String, Object> rd_HashMap = null;

		SoftwareDescription sd = description.getSoftwareDescription();
		ResourceDescription rd = description.getResourceDescription();

		// Corrected initialization of rd_HashMap: rd may be null ... --Ceriel
		if (rd != null) {
			rd_HashMap = (HashMap<String, Object>) rd.getDescription();
		}
		if (rd_HashMap == null) {
			rd_HashMap = new HashMap<String, Object>();
		}

		//try {
			temp = new java.io.File("lsf"+Math.random());
		/*} catch (IOException e) {
			throw new GATInvocationException("Cannot create temporary lsf file");
		}*/

		try {
			job = new LSFScriptWriter(new BufferedWriter(new FileWriter(temp)));
			String userScript = (String) gatContext.getPreferences().get(
					SSHLSF_SCRIPT);
			if (userScript != null) {
				// a specified job script overrides everything, except for
				// pre-staging, post-staging,
				// and exit status.
				BufferedReader f = new BufferedReader(
						new FileReader(userScript));
				for (;;) {
					String s = f.readLine();
					if (s == null) {
						break;
					}
					job.print(s + "\n");
				}
			} else {
				job.print("#!/bin/sh\n");
				job.print("# bsub script automatically generated by GAT SshLsf adaptor\n");

				// Resources: queue, walltime, memory size, et cetera.
				Queue = (String) rd_HashMap.get("machine.queue");
				if (Queue == null) {
					Queue = sd.getStringAttribute(
							SoftwareDescription.JOB_QUEUE, null);
				}
				if (Queue != null) {
					job.addOption("q", Queue);
				}

				Time = sd.getLongAttribute(SoftwareDescription.WALLTIME_MAX,
						-1L);

				cpus = (Integer) rd_HashMap
						.get(HardwareResourceDescription.CPU_COUNT);
				if (cpus == null) {
					cpus = sd.getIntAttribute("coreCount", 1);
				}

				job.addOption("n", cpus);
				// In a single node
				job.addOption("R", "\"span[ptile=" + cpus + "]\"");

				if (Time > 0) {
					// Reformat time.
					int minutes = (int) (Time % 60);
					job.addOption("W", minutes);
				} else {
					job.addOption("W", 60);
				}

				String nativeFlags = null;
				Object o = rd == null ? null : rd
						.getResourceAttribute(SSHLSF_NATIVE_FLAGS);
				if (o != null && o instanceof String) {
					nativeFlags = (String) o;
				} else {
					String s = sd == null ? null : sd.getStringAttribute(
							SSHLSF_NATIVE_FLAGS, null);
					if (s != null) {
						nativeFlags = s;
					} else {
						o = gatContext.getPreferences()
								.get(SSHLSF_NATIVE_FLAGS);
						if (o != null && o instanceof String) {
							nativeFlags = (String) o;
						}
					}
				}

				if (nativeFlags != null) {
					String[] splits = nativeFlags.split("##");
					for (String s : splits) {
						job.addString(s);
					}
				}
				String path = sd.getStringAttribute(SoftwareDescription.SANDBOX_ROOT, "");
				if (!path.isEmpty()&& !path.endsWith(File.separator)){
					path = path+File.separator;
				}
				// Set working dir.
				//job.addOption("cwd", path);

				// Name for the job.
				jobname = (String) rd_HashMap.get("Jobname");
				if (jobname == null) {
					jobname = brokerURI.getUserInfo();
					if (jobname == null || "".equals(jobname)) {
						jobname = "compss_remotejob_"+ System.getProperty("user.name");
					}
				}

				if (jobname != null)
					job.addOption("J", jobname);

				// Files for stdout and stderr.
				
				if (sd.getStdout() != null) {
					job.addOption("oo", path + sd.getStdout().getName());
				}

				if (sd.getStderr() != null) {
					job.addOption("eo", path + sd.getStderr().getName());
				}

				addScriptExecution(job, sd, rd);

			}

			job.print("echo retvalue = $? > " + returnValueFile + "\n");
			

		} catch (Throwable e) {
			throw new GATInvocationException(
					"Cannot create temporary bsub file "
							+ temp.getAbsolutePath(), e);
		} finally {
			if (job != null)
				job.close();
		}
		return temp;
	}

	private void addScriptExecution(LSFScriptWriter job,
			 SoftwareDescription sd, ResourceDescription rd) {
		     // Support DIRECTORY
					String dir = sd.getStringAttribute(SoftwareDescription.DIRECTORY, null);
					if (dir != null) {
						job.print("cd " + dir
								+ "\n");
					}

					// Support environment.
					Map<String, Object> env = sd.getEnvironment();
					if (env != null) {
						Set<String> s = env.keySet();
						Object[] keys = s.toArray();

						for (int i = 0; i < keys.length; i++) {
							String val = (String) env.get(keys[i]);
							job.print(keys[i] + "="
									+ val
									+ " && export " + keys[i] + "\n");
						}
					}

					// Construct command.
					StringBuffer cmd = new StringBuffer();

					cmd.append(sd.getExecutable()
							.toString());
					if (sd.getArguments() != null) {
						String[] args = sd.getArguments();
						for (int i = 0; i < args.length; ++i) {
							cmd.append(" ");
							cmd.append(args[i]);
						}
					}
					job.print(cmd.toString() + "\n");

	}

	private java.io.File createJobStarter(JobDescription description, int nproc,
			java.io.File jobScript) throws GATInvocationException {

		java.io.File temp;

		SoftwareDescription sd = description.getSoftwareDescription();

		try {
			temp = java.io.File.createTempFile("lsf", null);
		} catch (IOException e) {
			throw new GATInvocationException("Cannot create file", e);
		}
		PrintWriter job = null;
		try {
			job = new PrintWriter(new BufferedWriter(new FileWriter(temp)));
			job.print("#!/bin/sh\n");
			job.print("# Job starter script.\n");
			job.print("# The jobs are distributed over the available nodes in round-robin fashion.\n");
			job.print("GAT_MYDIR=`pwd`\n");
			job.print("case X$LSB_HOSTS in\n");
			job.print("X)  GAT_HOSTS=$HOSTNAME\n");
			job.print("    ;;\n");
			job.print("*)  GAT_HOSTS=`cat $LSB_HOSTS | sed 's/ .*//'`\n");
			job.print("    ;;\n");
			job.print("esac\n");
			job.print("GAT_JOBNO=1\n");
			job.print("GAT_JOBS=" + nproc + "\n");
			job.print("set $GAT_HOSTS\n");
			job.print("shift\n");
			job.print("while :\n");
			job.print("do\n");
			job.print("  for GAT_HOST in \"$@\"\n");
			job.print("  do\n");
			job.print("    echo #!/bin/sh > .gat_script.$GAT_JOBNO\n");
			job.print("    echo cd $GAT_MYDIR >> .gat_script.$GAT_JOBNO\n");
			job.print("    echo trap \\\"touch .gat_done.$GAT_JOBNO\\\" 0 1 2 3 15 >> .gat_script.$GAT_JOBNO\n");
			job.print("    cat " + jobScript.getName()
					+ " >> .gat_script.$GAT_JOBNO\n");
			job.print("    chmod +x .gat_script.$GAT_JOBNO\n");
			job.print("    ssh -o StrictHostKeyChecking=false $GAT_HOST \"$GAT_MYDIR/.gat_script.$GAT_JOBNO");
			if (sd.getStdin() != null) {
				job.print(" < $GAT_MYDIR/" + sd.getStdin().getName());
			} else {
				job.print(" < /dev/null");
			}
			job.print(" > $GAT_MYDIR/.out.$GAT_JOBNO 2>$GAT_MYDIR/.err.$GAT_JOBNO &\"\n");
			job.print("    GAT_JOBNO=`expr $GAT_JOBNO + 1`\n");
			job.print("    if expr $GAT_JOBNO \\>= $GAT_JOBS > /dev/null ; then break 2 ; fi\n");
			job.print("  done\n");
			job.print("  set $GAT_HOSTS\n");
			job.print("done\n");
		} catch (Throwable e) {
			throw new GATInvocationException(
					"Cannot create temporary job starter file "
							+ temp.getAbsolutePath(), e);
		} finally {
			if (job != null)
				job.close();
		}
		return temp;
	}

	/*private java.io.File createJobScript(JobDescription description)
			throws GATInvocationException {

		java.io.File temp;

		SoftwareDescription sd = description.getSoftwareDescription();

		try {
			temp = java.io.File.createTempFile("lsf-sub", null);
		} catch (IOException e) {
			throw new GATInvocationException("Cannot create file", e);
		}
		PrintWriter job = null;
		try {
			job = new PrintWriter(new BufferedWriter(new FileWriter(temp)));

			job.print("#!/bin/sh\n");
			job.print("# job script\n");

			// Support DIRECTORY
			String dir = sd.getStringAttribute(SoftwareDescription.DIRECTORY, null);
			if (dir != null) {
				job.print("cd " + dir + "\n");
			}

			// Support environment.
			Map<String, Object> env = sd.getEnvironment();
			if (env != null) {
				Set<String> s = env.keySet();
				Object[] keys = s.toArray();

				for (int i = 0; i < keys.length; i++) {
					String val = (String) env.get(keys[i]);
					job.print(keys[i] + "=" + val + " && export " + keys[i] + "\n");
				}
			}

			// Construct command.
			StringBuffer cmd = new StringBuffer();

			cmd.append(sd.getExecutable().toString());
			if (sd.getArguments() != null) {
				String[] args = sd.getArguments();
				for (int i = 0; i < args.length; ++i) {
					cmd.append(" ");
					cmd.append(args[i]);
				}
			}
			job.print(cmd.toString() + "\n");
			//job.print("exit $?\n");
		} catch (Throwable e) {
			throw new GATInvocationException(
					"Cannot create temporary job script file "
							+ temp.getAbsolutePath(), e);
		} finally {
			if (job != null)
				job.close();
		}
		return temp;
	}*/
    
    private String sshLsfSubmission(SshLSFJob lsfJob,
            JobDescription description, java.io.File bsubFile,
            ResourceBroker subBroker, Sandbox sandbox)
            throws GATInvocationException {


        java.io.File slurmResultFile = null;

        try {
        	if (logger.isDebugEnabled()) {
                logger.debug("***** Doing sandbox prestage " +sandbox.getSandboxPath());
            }
        	sandbox.prestage();
        	
        	if (logger.isDebugEnabled()) {
                logger.debug("***** Sandbox prestage done " +sandbox.getSandboxPath());
            }
            // Create sbatch job
            SoftwareDescription sd = new SoftwareDescription();
            sd.setExecutable("sh");
            sd.setArguments("-c","bsub < "+bsubFile.getName() +" 2>submit.err");
            	//	+ " && rm -rf " + bsubFile.getName() + " submit.err");
            sd.setAttributes(description.getSoftwareDescription().getAttributes());
            sd.addAttribute(SoftwareDescription.SANDBOX_USEROOT, "true");
            slurmResultFile = java.io.File.createTempFile("GAT", "tmp");
            try {
                sd.setStdout(GAT.createFile(
                        gatContext,
                        new URI(slurmResultFile.toURI())));
                sd.addPreStagedFile(GAT.createFile(gatContext, new URI(bsubFile.toURI())));
            } catch (Throwable e1) {
                try {
                    sandbox.removeSandboxDir();
                } catch (Throwable e) {
                    // ignore
                }
                throw new GATInvocationException(
                        "Could not create GAT object for temporary "
                                + slurmResultFile.getAbsolutePath(), e1);
            }
            //sd.addAttribute(SoftwareDescription.DIRECTORY, sd.getStringAttribute(SoftwareDescription, defaultVal)));
            JobDescription jd = new JobDescription(sd);
            
            if (logger.isDebugEnabled()) {
                logger.debug("Submitting lsf job: " + sd);
            }
            
            Job job = subBroker.submitJob(jd, this, "job.status");
            if (logger.isDebugEnabled()) {
                logger.debug("Job submitted.");
            }
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
                try {
                    sandbox.removeSandboxDir();
                } catch (Throwable e) {
                    // ignore
                }
                logger.debug("jobState = " + job.getState()
                        + ", exit status = " + job.getExitStatus());
                throw new GATInvocationException("Could not submit LSF job");
            }

            // submit success.
            BufferedReader in = new BufferedReader(new FileReader(
                    slurmResultFile.getAbsolutePath()));
            String result = in.readLine();
            if (logger.isDebugEnabled()) {
                logger.debug("bsub result line = " + result);
            }

            // Check for LSF bsub result ...
            //TODO Check if LSF return the same
            String job_prefix = "Job <";
            if (result.contains(job_prefix)) {
                int i = result.indexOf(job_prefix);
            	result = result.substring(i+job_prefix.length(), result.indexOf(">", i));
            }

            return result;
        } catch (IOException e) {
            try {
                sandbox.removeSandboxDir();
            } catch (Throwable e1) {
                // ignore
            }
            throw new GATInvocationException("Got IOException", e);
        } finally {
            slurmResultFile.delete();
            bsubFile.delete();
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
