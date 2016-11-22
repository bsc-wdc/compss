package integratedtoolkit.gat.master.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.ITConstants;
import integratedtoolkit.gat.master.GATWorkerNode;
import integratedtoolkit.util.RequestDispatcher;
import integratedtoolkit.util.RequestQueue;
import integratedtoolkit.util.ThreadPool;

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

    /**
     * Constants
     */
    private static final String THREAD_POOL_START_ERR = "Error starting pool of threads";
    private static final String THREAD_POOL_STOP_ERR = "Error stopping pool of threads";
    private static final String CLEAN_JOB_ERR = "Error running clean job";

    private static final String POOL_NAME = "Cleaner";
    /**
     * Amount of threads that will execute the cleaning scripts
     */
    private static final int POOL_SIZE = 5;
    /**
     * Amount of host to be clean
     */
    private int jobCount;

    private ThreadPool pool;

    private GATWorkerNode node;

    RequestQueue<Job> jobQueue;

    RequestQueue<SoftwareDescription> sdQueue;

    /**
     * Logger
     */
    private static final Logger logger = LogManager.getLogger(Loggers.FTM_COMP);
    private static final boolean debug = logger.isDebugEnabled();


    /**
     * Constructs a new cleaner and starts the cleaning process. A new GATContext is created and configured. Two
     * RequestQueues are created: sdQueue and jobQueue. The former contains the tasks that still have to be executed and
     * the last keeps the notifications about their runs. With this 2 queues it constructs a new Cleaner Dispatcher
     * which is in charge of the remote executions, its threads take job descriptions from the input queue and leaves
     * the results on the jobQueue. Once the method has added all the job descriptions, it waits for its response. If
     * the task runs properly or there is an error during the submission process, it counts that job has done. If it
     * ended due to another reason, the task is resubmitted. Once all the scripts have been executed, the pool of
     * threads is destroyed. There's a timeout of 1 minute. if the task don't end during this time, the thread pool is
     * destroyed as well.
     *
     *
     *
     * @param cleanScripts
     *            list of locations where to find all the cleaning scripts that must be executed
     * @param cleanParams
     *            list with the input parameters that each script will run with
     */

    public GATScriptExecutor(GATWorkerNode node) {
        sdQueue = new RequestQueue<>();
        jobQueue = new RequestQueue<>();
        pool = new ThreadPool(POOL_SIZE, POOL_NAME, new ScriptDispatcher(sdQueue, jobQueue, node));
        this.node = node;
    }

    public boolean executeScript(List<URI> scripts, List<String> params, String stdOutFileName) {
        try {
            pool.startThreads();
        } catch (Exception e) {
            logger.error(THREAD_POOL_START_ERR, e);
            return false;
        }

        synchronized (jobQueue) {
            jobCount = scripts.size();
        }

        for (int i = 0; i < scripts.size(); i++) {
            URI script = scripts.get(i);
            String cleanParam = params.get(i);
            if (script == null) {
                continue;
            }

            if (debug) {
                logger.debug("Clean call: " + script + " " + cleanParam);
            }

            try {
                if (!node.isUserNeeded() && script.getUserInfo() != null) { // Remove user from the URI
                    script.setUserInfo(null);
                }
                String user = script.getUserInfo();
                if (user == null) {
                    user = "";
                } else {
                    user += "@";
                }
                SoftwareDescription sd = new SoftwareDescription();
                sd.addAttribute("uri", Protocol.ANY_URI.getSchema() + user + script.getHost());
                sd.setExecutable(script.getPath());
                sd.setArguments(cleanParam.split(" "));

                sd.addAttribute("job_number", i);
                sd.addAttribute(SoftwareDescription.SANDBOX_ROOT, File.separator + "tmp" + File.separator);
                sd.addAttribute(SoftwareDescription.SANDBOX_USEROOT, "true");
                sd.addAttribute(SoftwareDescription.SANDBOX_DELETE, "false");

                if (debug) {
                    try {
                        org.gridlab.gat.io.File outFile = GAT.createFile(node.getContext(),
                                Protocol.ANY_URI.getSchema() + File.separator + System.getProperty(ITConstants.IT_APP_LOG_DIR) + File.separator + stdOutFileName + ".out");
                        sd.setStdout(outFile);
                        org.gridlab.gat.io.File errFile = GAT.createFile(node.getContext(),
                                Protocol.ANY_URI.getSchema() + File.separator + System.getProperty(ITConstants.IT_APP_LOG_DIR) + File.separator + stdOutFileName + ".err");
                        sd.setStderr(errFile);
                    } catch (Exception e) {
                    	logger.error(CLEAN_JOB_ERR, e);
                    }
                }

                sdQueue.enqueue(sd);
            } catch (Exception e) {
                logger.error(CLEAN_JOB_ERR, e);
                return false;
            }
        }
        Long timeout = System.currentTimeMillis() + 60_000l;
        // Poll for completion of the clean jobs
        while (jobCount > 0 && System.currentTimeMillis() < timeout) {
            Job job = jobQueue.dequeue();
            if (job == null) {
                synchronized (jobQueue) {
                    jobCount--;
                }
            } else if (job.getState() == JobState.STOPPED) {
                synchronized (jobQueue) {
                    jobCount--;
                }
            } else if (job.getState() == JobState.SUBMISSION_ERROR) {
                logger.error(CLEAN_JOB_ERR + ": " + job);
                synchronized (jobQueue) {
                    jobCount--;
                }
            } else {
                jobQueue.enqueue(job);
                try {
                    Thread.sleep(50);
                } catch (Exception e) {
                }
            }
        }

        try {
            pool.stopThreads();
        } catch (Exception e) {
            logger.error(THREAD_POOL_STOP_ERR, e);
            return false;
        }

        // Move cleanX.out logs to default logger
        if (debug) {
            String stdOutFilePath = System.getProperty(ITConstants.IT_APP_LOG_DIR) + File.separator + stdOutFileName + ".out";
            
            try (FileReader cleanOut = new FileReader(stdOutFilePath);
                    BufferedReader br = new BufferedReader(cleanOut)) {
               
                String line = br.readLine();
                while (line != null) {
                    logger.debug(line);
                    line = br.readLine();
                }
            } catch (Exception e) {
                logger.error("Error moving std out file", e);
            }
            
            // Delete file
            if (!new File(stdOutFilePath).delete()) {
                logger.error("Error deleting out file " + stdOutFilePath);
            }
        }

        // Move cleanX.err logs to default logger
        if (debug) {
            String stdErrFilePath = System.getProperty(ITConstants.IT_APP_LOG_DIR) + File.separator + stdOutFileName + ".err";
            
            try (FileReader cleanErr = new FileReader(stdErrFilePath);
                    BufferedReader br = new BufferedReader(cleanErr)) {
                
                String line = br.readLine();
                while (line != null) {
                    logger.error(line);
                    line = br.readLine();
                } 
            } catch (Exception e) {
                logger.error("Error moving std err file", e);
            }

            if (!new File(stdErrFilePath).delete()) {
                logger.error("Error deleting err file " + stdErrFilePath);
            }
        }
        return true;
    }


    /**
     * The CleanDispatcherClass represents a pool of threads that will run the cleaning scripts
     */
    class ScriptDispatcher extends RequestDispatcher<SoftwareDescription> {

        /**
         * All the GAT jobs that have already been submitted
         */
        private RequestQueue<Job> jobQueue;
        private GATWorkerNode node;


        /**
         * Constructs a new CleanDispatcher
         *
         * @param sdQueue
         *            list of the task to be executed
         * @param jobQueue
         *            list where all the already executed tasks will be left
         */
        public ScriptDispatcher(RequestQueue<SoftwareDescription> sdQueue, RequestQueue<Job> jobQueue, GATWorkerNode node) {
            super(sdQueue);
            this.jobQueue = jobQueue;
            this.node = node;

        }

        /**
         * main function executed by the thread of the pool. They take a job description from the input queue and try to
         * execute it. If the tasks ends properly the job is added to the jobqueue; if not, a null job is added to
         * notify the error. The thread will stop once it dequeues a null task.
         */
        public void processRequests() {
            while (true) {
                SoftwareDescription sd = queue.dequeue();

                if (sd == null) {
                    break;
                }
                try {
                    URI brokerURI = new URI((String) sd.getObjectAttribute("uri"));
                    ResourceBroker broker = GAT.createResourceBroker(node.getContext(), brokerURI);
                    Job job = broker.submitJob(new JobDescription(sd));
                    jobQueue.enqueue(job);
                } catch (Exception e) {
                    logger.error("Error submitting clean job", e);
                    jobQueue.enqueue((Job) null);
                }
            }
        }
    }
    
}
