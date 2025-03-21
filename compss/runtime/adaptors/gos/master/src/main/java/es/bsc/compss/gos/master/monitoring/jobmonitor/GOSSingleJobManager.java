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

import es.bsc.compss.gos.master.GOSJob;
import es.bsc.compss.gos.master.sshutils.SSHChannel;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.job.JobEndStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GOSSingleJobManager {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    private final GOSJob gosJob;
    private final String id;


    public GOSSingleJobManager(GOSJob job) {
        this.gosJob = job;
        this.id = job.getCompositeID();
    }

    public String getID() {
        return id;
    }

    /**
     * Is channel ended boolean.
     *
     * @return the boolean
     */
    public boolean isChannelEnded() {
        SSHChannel ch = gosJob.getChannel();
        if (ch == null) {
            return true;
        }
        if (ch.getExitStatus() != -1) {
            ch.disconnect();
        }
        return ch.isClosed();
    }

    /**
     * Monitor boolean.
     *
     * @return the boolean
     */
    public boolean monitor() {
        if (isChannelEnded()) {
            SSHChannel ch = gosJob.getChannel();
            if (ch == null) {
                LOGGER.warn("[WARNING] Channel is null in monitoring");
                gosJob.notifyFailure(JobEndStatus.SUBMISSION_FAILED);
                return true;
            }
            int status = ch.getExitStatus();
            switch (status) {
                case 0:
                    gosJob.notifySuccess();
                    break;
                case 127:
                    gosJob.notifyFailure(JobEndStatus.SUBMISSION_FAILED);
                    break;
                default:
                    gosJob.notifyFailure(JobEndStatus.EXECUTION_FAILED);
            }
            releaseResource();
            return true;
        }
        return false;
    }

    private void releaseResource() {
        if (gosJob.getChannel() != null) {
            gosJob.getChannel().disconnect();
        }
    }

    /**
     * force end job.
     */
    public void shutdown() {
        releaseResource();
        gosJob.notifyFailure(JobEndStatus.EXCEPTION);
    }

}
