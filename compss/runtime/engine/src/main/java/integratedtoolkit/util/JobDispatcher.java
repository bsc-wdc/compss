package integratedtoolkit.util;

import integratedtoolkit.log.Loggers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobListener.JobEndStatus;


public class JobDispatcher {

    // logger
    private static final Logger logger = LogManager.getLogger(Loggers.JM_COMP);
    private static final boolean debug = logger.isDebugEnabled();

    protected static RequestQueue<Job<?>> queue;
    // Pool of worker threads and queue of requests
    private static ThreadPool pool;

    private static final String THREAD_POOL_ERR = "Error starting pool of threads";
    private static final String SUBMISSION_ERROR = "Error submitting job ";
    public static final int POOL_SIZE = 1;
    public static final String POOL_NAME = "Job Submitter";

    static {
        queue = new RequestQueue<>();
        pool = new ThreadPool(POOL_SIZE, POOL_NAME, new JobSubmitter(queue));
        try {
            pool.startThreads();
        } catch (Exception e) {
            ErrorManager.fatal(THREAD_POOL_ERR, e);
        }
    }


    public static void dispatch(Job<?> job) {
        queue.enqueue(job);
    }

    public static void stop() {
        try {
            pool.stopThreads();
        } catch (Exception e) {
            // Ignore, we are terminating
        }
    }


    static class JobSubmitter extends RequestDispatcher<Job<?>> {

        public JobSubmitter(RequestQueue<Job<?>> queue) {
            super(queue);
        }

        @Override
        public void processRequests() {
            while (true) {
                Job<?> job = queue.dequeue();
                if (job == null) {
                    break;
                }
                try {
                    job.submit();
                    if (debug) {
                        logger.debug("Job " + job.getJobId() + " submitted");
                    }
                } catch (Exception ex) {
                    logger.error(SUBMISSION_ERROR + job.getJobId(), ex);
                    job.getListener().jobFailed(job, JobEndStatus.SUBMISSION_FAILED);
                }
            }
            logger.debug("JobDispatcher finished");
        }
    }

}
