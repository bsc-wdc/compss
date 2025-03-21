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
package es.bsc.compss.gos.master.monitoring.jobmonitor;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import es.bsc.compss.gos.master.GOSJob;
import es.bsc.compss.gos.master.exceptions.GOSWarningException;
import es.bsc.compss.gos.master.sshutils.SSHChannel;
import es.bsc.compss.gos.master.sshutils.SSHHost;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.util.ErrorManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GOSHostBatchMonitor implements GOSHostsManager {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    private static final String CHECK_COMMAND_SCRIPT = GOSJob.SCRIPT_PATH + "check.sh";

    private final String installDir;
    final ConcurrentHashMap<String, GOSSingleBatchJobManager> activeJobs = new ConcurrentHashMap<>();
    HashSet<String> failedResponse = new HashSet<>();

    String responseDir;
    SSHHost host;


    GOSHostBatchMonitor(GOSJob job) {
        addJobMonitor(job);
        this.responseDir = job.getResponseDir();
        this.host = job.getSSHHost();
        this.installDir = job.getResourceNode().getConfig().getInstallDir();
        assert host != null;
    }

    @Override
    public void monitor() {
        if (activeJobs.isEmpty()) {
            return;
        }

        GOSSingleBatchJobManager[] jobs = null;
        synchronized (activeJobs) {
            Collection<GOSSingleBatchJobManager> col = activeJobs.values();
            if (!col.isEmpty()) {
                jobs = col.toArray(new GOSSingleBatchJobManager[col.size()]);
            }
        }
        if (jobs == null) {
            return;
        }

        StringBuilder jobsId = new StringBuilder();

        for (GOSSingleBatchJobManager jm : jobs) {
            GOSJob job = jm.getGosJob();
            if (job.isBeingCancelled()) {
                job.notifyFailure(JobEndStatus.EXECUTION_FAILED);
                synchronized (activeJobs) {
                    activeJobs.remove(jm.getId());
                }
                break;
            }
            if (job.getHistory() == JobHistory.CANCELLED) {
                LOGGER.error("Ignoring notification since the job was cancelled");
                synchronized (activeJobs) {
                    activeJobs.remove(jm.getId());
                }
                break;
            }
            jobsId.append(jm.getId()).append(" ");
            // disconnect channel that launched the job
            SSHChannel channel = job.getChannel();
            if (channel != null && channel.isConnected() && channel.getExitStatus() != -1) {
                channel.disconnect();
                job.setChannel(null);
            }
        }
        if (jobsId.toString().isEmpty()) {
            return;
        }
        String command = installDir + CHECK_COMMAND_SCRIPT + " " + responseDir + " " + jobsId;
        // launch command check
        try {
            BufferedReader reader = host.executeCommand(command);
            while (reader.ready()) {
                String[] line = reader.readLine().split(" ");
                if (line.length < 3) {
                    // empty lines in reader
                    continue;
                }
                boolean finished = treatJob(line[0], line[1], line[2]);
                if (finished) {
                    synchronized (activeJobs) {
                        activeJobs.remove(line[0]);
                    }
                }
            }

        } catch (JSchException | IOException | SftpException e) {
            LOGGER.error("Error in batch job monitor " + e);
            ErrorManager.error(e);
        } catch (GOSWarningException e) {
            LOGGER.warn("Warning in batch job monitor " + e.getLocalizedMessage());
            // Non-fatal error in command
        }

    }

    private boolean treatJob(String id, String batchId, String status)
        throws JSchException, SftpException, IOException {
        GOSSingleBatchJobManager jm = activeJobs.get(id);
        if (jm == null) {
            return false;
        }
        GOSJob job = jm.getGosJob();
        boolean finished = false;
        String batchOutput = job.getBatchOutput();
        String jobID = job.getCompositeID();
        switch (status) {
            case "END":
                finished = true;
                job.setBatchId(batchId);
                job.notifySuccess();
                break;
            case "CANCEL":
            case "FAIL":
                finished = true;
                job.setBatchId(batchId);
                job.notifyFailure(JobEndStatus.EXECUTION_FAILED);
                break;
            case "SUBMIT":
            case "LAUNCH":
            case "RUN":
                // ignore launch submit and run, the job is in process
                break;
            case "NOT_EXISTS":
            default:
                // the first time that this triggers does not notify failure to runtime
                if (failedResponse.contains(id)) {
                    failedResponse.remove(id);
                    job.notifyFailure(JobEndStatus.SUBMISSION_FAILED);
                } else {
                    failedResponse.add(id);
                }
        }
        if (finished) {
            ArrayList<String> srcFiles = new ArrayList<>();
            ArrayList<String> dstFiles = new ArrayList<>();
            srcFiles.add(batchOutput + ".out");
            dstFiles.add(File.separator + GOSJob.JOBS_DIR + jobID + ".out");
            srcFiles.add(batchOutput + ".err");
            dstFiles.add(File.separator + GOSJob.JOBS_DIR + jobID + ".err");
            host.appendFiles(srcFiles, dstFiles);

            //
        }
        return finished;
    }

    @Override
    public void addJobMonitor(GOSJob job) {
        activeJobs.put(job.getCompositeID(), new GOSSingleBatchJobManager(job));
    }

    @Override
    public int countActiveJobs() {
        return activeJobs.size();
    }

    @Override
    public boolean existsRunningJobs() {
        return !activeJobs.isEmpty();
    }

    @Override
    public void shutdown() {
        for (GOSSingleBatchJobManager jm : activeJobs.values()) {
            GOSJob job = jm.getGosJob();
            try {
                job.cancelJob();
            } catch (Exception e) {
                LOGGER.warn("Error cancelling job when shutdown. Ignoring.");
            }
        }
        activeJobs.clear();
    }

    @Override
    public void removeJobMonitor(GOSJob job) {
        synchronized (activeJobs) {
            activeJobs.remove(job.getCompositeID());
        }

    }


    private class GOSSingleBatchJobManager {

        private final GOSJob gosJob;
        private final String id;


        public GOSSingleBatchJobManager(GOSJob job) {
            this.gosJob = job;
            this.id = job.getCompositeID();
        }

        public GOSJob getGosJob() {
            return gosJob;
        }

        public String getId() {
            return id;
        }
    }

}
