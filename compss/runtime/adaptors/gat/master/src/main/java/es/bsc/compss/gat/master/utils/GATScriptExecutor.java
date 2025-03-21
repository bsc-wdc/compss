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
package es.bsc.compss.gat.master.utils;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.gat.master.GATWorkerNode;
import es.bsc.compss.log.LoggerManager;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.util.RequestDispatcher;
import es.bsc.compss.util.RequestQueue;
import es.bsc.compss.util.ThreadPool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.Job.JobState;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;


/**
 * The cleaner class is an utility to execute the cleaning script on the remote workers.
 */
public class GATScriptExecutor {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.FTM_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String THREAD_POOL_START_ERR = "Error starting pool of threads";
    private static final String THREAD_POOL_STOP_ERR = "Error stopping pool of threads";
    private static final String CLEAN_JOB_ERR = "Error running clean job";

    // Thread pool base name
    private static final String POOL_NAME = "Cleaner";
    // Amount of threads that will execute the cleaning scripts
    private static final int POOL_SIZE = 5;

    private final GATWorkerNode node;
    private final RequestQueue<Job> jobQueue;
    private final RequestQueue<SoftwareDescription> sdQueue;
    private final ThreadPool pool;

    private int jobCount;


    /**
     * Constructs a new cleaner and starts the cleaning process. A new GATContext is created and configured. Two
     * RequestQueues are created: sdQueue and jobQueue. The former contains the tasks that still have to be executed and
     * the last keeps the notifications about their runs. With these 2 queues it constructs a new Cleaner Dispatcher
     * which is in charge of the remote executions, its threads take job descriptions from the input queue and leaves
     * the results on the jobQueue.
     *
     * @param node Node where to run the cleaning scripts.
     */
    public GATScriptExecutor(GATWorkerNode node) {
        this.node = node;
        this.jobQueue = new RequestQueue<>();
        this.sdQueue = new RequestQueue<>();
        this.pool = new ThreadPool(POOL_SIZE, POOL_NAME, new ScriptDispatcher(this.sdQueue, this.jobQueue, node));
    }

    /**
     * Executes the given list of cleaning scripts in the cleaner pool. Once the method has added all the job
     * descriptions, it waits for its response. If the task runs properly or there is an error during the submission
     * process, it counts that job has done. If it ended due to another reason, the task is resubmitted. Once all the
     * scripts have been executed, the pool of threads is destroyed. There's a timeout of 1 minute. if the task don't
     * end during this time, the thread pool is destroyed as well.
     * 
     * @param scripts List of locations where to find all the cleaning scripts that must be executed
     * @param params List of input parameters that each script will run with.
     * @param stdOutFileName Execution output.
     * @return {@literal true} if all the scripts have been executed properly, {@literal false} otherwise.
     */
    public boolean executeScript(List<URI> scripts, List<String> params, String stdOutFileName) {
        try {
            this.pool.startThreads();
        } catch (Exception e) {
            LOGGER.error(THREAD_POOL_START_ERR, e);
            return false;
        }

        synchronized (this.jobQueue) {
            this.jobCount = scripts.size();
        }

        for (int i = 0; i < scripts.size(); i++) {
            URI script = scripts.get(i);
            String cleanParam = params.get(i);
            if (script == null) {
                continue;
            }

            if (DEBUG) {
                LOGGER.debug("Clean call: " + script + " " + cleanParam);
            }

            try {
                if (!this.node.isUserNeeded() && script.getUserInfo() != null) { // Remove user from the URI
                    script.setUserInfo(null);
                }
                String user = script.getUserInfo();
                if (user == null) {
                    user = "";
                } else {
                    user += "@";
                }
                SoftwareDescription sd = new SoftwareDescription();
                sd.addAttribute("uri", ProtocolType.ANY_URI.getSchema() + user + script.getHost());
                sd.setExecutable(script.getPath());
                sd.setArguments(cleanParam.split(" "));

                sd.addAttribute("job_number", i);
                sd.addAttribute(SoftwareDescription.SANDBOX_ROOT, File.separator + "tmp" + File.separator);
                sd.addAttribute(SoftwareDescription.SANDBOX_USEROOT, "true");
                sd.addAttribute(SoftwareDescription.SANDBOX_DELETE, "false");

                if (DEBUG) {
                    try {
                        org.gridlab.gat.io.File outFile =
                            GAT.createFile(this.node.getContext(), ProtocolType.ANY_URI.getSchema() + File.separator
                                + LoggerManager.getLogDir() + File.separator + stdOutFileName + ".out");
                        sd.setStdout(outFile);
                        org.gridlab.gat.io.File errFile =
                            GAT.createFile(this.node.getContext(), ProtocolType.ANY_URI.getSchema() + File.separator
                                + LoggerManager.getLogDir() + File.separator + stdOutFileName + ".err");
                        sd.setStderr(errFile);
                    } catch (Exception e) {
                        LOGGER.error(CLEAN_JOB_ERR, e);
                    }
                }

                this.sdQueue.enqueue(sd);
            } catch (Exception e) {
                LOGGER.error(CLEAN_JOB_ERR, e);
                return false;
            }
        }

        // Poll for completion of the clean jobs
        Long timeout = System.currentTimeMillis() + 60_000L;
        while (this.jobCount > 0 && System.currentTimeMillis() < timeout) {
            Job job = this.jobQueue.dequeue();
            if (job == null) {
                synchronized (this.jobQueue) {
                    this.jobCount--;
                }
            } else if (job.getState() == JobState.STOPPED) {
                synchronized (this.jobQueue) {
                    this.jobCount--;
                }
            } else if (job.getState() == JobState.SUBMISSION_ERROR) {
                LOGGER.error(CLEAN_JOB_ERR + ": " + job);
                synchronized (this.jobQueue) {
                    this.jobCount--;
                }
            } else {
                this.jobQueue.enqueue(job);
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        try {
            this.pool.stopThreads();
        } catch (Exception e) {
            LOGGER.error(THREAD_POOL_STOP_ERR, e);
            return false;
        }

        // Move cleanX.out logs to default logger
        if (DEBUG) {
            String stdOutFilePath = LoggerManager.getLogDir() + File.separator + stdOutFileName + ".out";

            try (FileReader cleanOut = new FileReader(stdOutFilePath);
                BufferedReader br = new BufferedReader(cleanOut)) {

                String line = br.readLine();
                while (line != null) {
                    LOGGER.debug(line);
                    line = br.readLine();
                }
            } catch (IOException ioe) {
                LOGGER.error("Error moving std out file", ioe);
            }

            // Delete file
            if (!new File(stdOutFilePath).delete()) {
                LOGGER.error("Error deleting out file " + stdOutFilePath);
            }
        }

        // Move cleanX.err logs to default logger
        if (DEBUG) {
            String stdErrFilePath = LoggerManager.getLogDir() + File.separator + stdOutFileName + ".err";

            try (FileReader cleanErr = new FileReader(stdErrFilePath);
                BufferedReader br = new BufferedReader(cleanErr)) {
                String line = br.readLine();
                while (line != null) {
                    LOGGER.error(line);
                    line = br.readLine();
                }
            } catch (IOException ioe) {
                LOGGER.error("Error moving std err file", ioe);
            }

            if (!new File(stdErrFilePath).delete()) {
                LOGGER.error("Error deleting err file " + stdErrFilePath);
            }
        }
        return true;
    }


    /**
     * The CleanDispatcherClass represents a pool of threads that will run the cleaning scripts.
     */
    private class ScriptDispatcher extends RequestDispatcher<SoftwareDescription> {

        // All the GAT jobs that have already been submitted
        private RequestQueue<Job> jobQueue;
        private GATWorkerNode node;


        /**
         * Constructs a new CleanDispatcher.
         *
         * @param sdQueue List of the task to be executed.
         * @param jobQueue List where all the already executed tasks will be left.
         */
        public ScriptDispatcher(RequestQueue<SoftwareDescription> sdQueue, RequestQueue<Job> jobQueue,
            GATWorkerNode node) {

            super(sdQueue);
            this.jobQueue = jobQueue;
            this.node = node;
        }

        /**
         * Main function executed by the thread of the pool. They take a job description from the input queue and try to
         * execute it. If the tasks ends properly the job is added to the jobQueue; if not, a null job is added to
         * notify the error. The thread will stop once it dequeues a null task.
         */
        public void processRequests() {
            while (true) {
                SoftwareDescription sd = this.queue.dequeue();

                if (sd == null) {
                    break;
                }
                try {
                    URI brokerURI = new URI((String) sd.getObjectAttribute("uri"));
                    ResourceBroker broker = GAT.createResourceBroker(node.getContext(), brokerURI);
                    Job job = broker.submitJob(new JobDescription(sd));
                    this.jobQueue.enqueue(job);
                } catch (Exception e) {
                    LOGGER.error("Error submitting clean job", e);
                    this.jobQueue.enqueue((Job) null);
                }
            }
        }
    }

}
