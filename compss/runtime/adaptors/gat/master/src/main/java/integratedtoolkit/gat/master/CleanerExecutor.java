package integratedtoolkit.gat.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

import org.apache.log4j.Logger;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.ITConstants;
import integratedtoolkit.util.RequestDispatcher;
import integratedtoolkit.util.RequestQueue;
import integratedtoolkit.util.ThreadPool;

import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.Job.JobState;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;


/**
 * The cleaner class is an utility to execute the cleaning script on the remote
 * workers.
 */
public class CleanerExecutor {

    /**
     * Constants
     */
    private static final String ANY_PROT = "any://";
    private static final String THREAD_POOL_START_ERR 	= "Error starting pool of threads";
    private static final String THREAD_POOL_STOP_ERR 	= "Error stopping pool of threads";
    private static final String CLEAN_JOB_ERR 			= "Error running clean job";
    
    private static final String POOL_NAME = "Cleaner";
    /**
     * Amount of threads that will execute the cleaning scripts
     */
    private static final int POOL_SIZE = 5;
    /**
     * GAT context
     */
    //private GATContext context;
    
    /**
     * GAT broker adaptor information
     */
    
    /**
     * GAT is using Globus
     */
    //private boolean usingGlobus;
    
    /**
     * GAT needs a userName to connect with the resources
     */
    //private boolean userNeeded;
    
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
    private static final Logger logger = Logger.getLogger(Loggers.FTM_COMP);
    private static final boolean debug = logger.isDebugEnabled();

    /**
     * Constructs a new cleaner and starts the cleaning process. A new
     * GATContext is created and configured. Two RequestQueues are created:
     * sdQueue and jobQueue. The former contains the tasks that still have to be
     * executed and the last keeps the notifications about their runs. With this
     * 2 queues it constructs a new Cleaner Dispatcher which is in charge of the
     * remote executions, its threads take job descriptions from the input queue
     * and leaves the results on the jobQueue. Once the method has added all the
     * job descriptions, it waits for its response. If the task runs properly or
     * there is an error during the submission process, it counts that job has
     * done. If it ended due to another reason, the task is resubmitted. Once
     * all the scripts have been executed, the pool of threads is destroyed.
     * There's a timeout of 1 minute. if the task don't end during this time,
     * the thread pool is destroyed as well.
     *
     *
     *
     * @param cleanScripts list of locations where to find all the cleaning
     * scripts that must be executed
     * @param cleanParams list with the input parameters that each script will
     * run with
     */
    
    public CleanerExecutor(GATWorkerNode node){
    	sdQueue = new RequestQueue<SoftwareDescription>();
        jobQueue = new RequestQueue<Job>();
        pool = new ThreadPool(POOL_SIZE, POOL_NAME, new CleanDispatcher(sdQueue, jobQueue, node));
        this.node = node;
    }
    
    public void executeScript(List<URI> cleanScripts, List<String> cleanParams) {
        try {
            pool.startThreads();
        } catch (Exception e) {
            logger.error(THREAD_POOL_START_ERR, e);
            return;
        }

        synchronized (jobQueue) {
            jobCount = cleanScripts.size();
        }

        for (int i = 0; i < cleanScripts.size(); i++) {
            URI script = cleanScripts.get(i);
            String cleanParam = cleanParams.get(i);
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
                sd.addAttribute("uri", ANY_PROT + user + script.getHost());
                sd.setExecutable(script.getPath());
                sd.setArguments(cleanParam.split(" "));

                sd.addAttribute("job_number", i);
                sd.addAttribute(SoftwareDescription.SANDBOX_ROOT, "/tmp/");
                sd.addAttribute(SoftwareDescription.SANDBOX_USEROOT, "true");
                sd.addAttribute(SoftwareDescription.SANDBOX_DELETE, "false");

                if (debug) {
                    try {
                        org.gridlab.gat.io.File outFile = GAT.createFile(node.getContext(), "any:///" + System.getProperty(ITConstants.IT_APP_LOG_DIR) + "/cleaner.out");
                        sd.setStdout(outFile);
                        org.gridlab.gat.io.File errFile = GAT.createFile(node.getContext(), "any:///" + System.getProperty(ITConstants.IT_APP_LOG_DIR) + "/cleaner.err");
                        sd.setStderr(errFile);
                    } catch (Exception e) {
                    }
                }

                sdQueue.enqueue(sd);
            } catch (Exception e) {
                logger.error(CLEAN_JOB_ERR, e);
                return;
            }
        }
        Long timeout = System.currentTimeMillis() + 60000l;
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
            return;
        }

        //Move cleanX.out logs to default logger
        if (debug) {
            try {
                FileReader cleanOut = new FileReader(System.getProperty(ITConstants.IT_APP_LOG_DIR) + "/cleaner.out");
                BufferedReader br = new BufferedReader(cleanOut);
                String line = br.readLine();
                while (line != null) {
                    logger.debug(line);
                    line = br.readLine();
                }
                br.close();
            } catch (Exception e) {
                logger.error("Error moving cleaner.out", e);
            }
            new File(System.getProperty(ITConstants.IT_APP_LOG_DIR) + "/cleaner.out").delete();
        }

        //Move cleanX.err logs to default logger
        if (debug) {
            try {
                FileReader cleanErr = new FileReader(System.getProperty(ITConstants.IT_APP_LOG_DIR) + "/cleaner.err");
                BufferedReader br = new BufferedReader(cleanErr);
                String line = br.readLine();
                while (line != null) {
                    logger.error(line);
                    line = br.readLine();
                }
                br.close();
            } catch (Exception e) {
                logger.error("Error moving cleaner.err", e);
            }
            new File(System.getProperty(ITConstants.IT_APP_LOG_DIR) + "/cleaner.err").delete();
        }
    }

    /**
     * The CleanDispatcherClass represents a pool of threads that will run the
     * cleaning scripts
     */
    class CleanDispatcher extends RequestDispatcher<SoftwareDescription> {

        /**
         * All the GAT jobs that have already been submitted
         */
        private RequestQueue<Job> jobQueue;
        private GATWorkerNode node;
        /**
         * Constructs a new CleanDispatcher
         *
         * @param sdQueue list of the task to be executed
         * @param jobQueue list where all the already executed tasks will be
         * left
         */
        public CleanDispatcher(RequestQueue<SoftwareDescription> sdQueue,
                RequestQueue<Job> jobQueue, GATWorkerNode node) {
            super(sdQueue);
            this.jobQueue = jobQueue;
            this.node = node;
            
        }

        /**
         * main function executed by the thread of the pool. They take a job
         * description from the input queue and try to execute it. If the tasks
         * ends properly the job is added to the jobqueue; if not, a null job is
         * added to notify the error. The thread will stop once it dequeues a
         * null task.
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
