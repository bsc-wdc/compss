package integratedtoolkit.util;

import integratedtoolkit.log.Loggers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobListener.JobEndStatus;


public class JobDispatcher {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.JM_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String SUBMISSION_ERROR = "Error submitting job ";

    // Names
    public static final int POOL_SIZE = 1;
    public static final String POOL_NAME = "Job Submitter";

    // Requests queue
    protected static RequestQueue<Job<?>> queue;
    // Pool of worker threads and queue of requests
    private static ThreadPool pool;

    static {
        queue = new RequestQueue<>();
        pool = new ThreadPool(POOL_SIZE, POOL_NAME, new JobSubmitter(queue));
        pool.startThreads();
    }


    public static void dispatch(Job<?> job) {
        queue.enqueue(job);
    }

    public static void shutdown() {
        pool.stopThreads();
    }


    private static class JobSubmitter extends RequestDispatcher<Job<?>> {

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
                    if (DEBUG) {
                        LOGGER.debug("Job " + job.getJobId() + " submitted");
                    }
                } catch (Exception ex) {
                    LOGGER.error(SUBMISSION_ERROR + job.getJobId(), ex);
                    job.getListener().jobFailed(job, JobEndStatus.SUBMISSION_FAILED);
                }
            }
            LOGGER.debug("JobDispatcher finished");
        }
    }

}
