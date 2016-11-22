package integratedtoolkit.nio.worker.util;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.worker.executors.ExternalExecutor;
import integratedtoolkit.util.ErrorManager;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TaskResultReader extends Thread {

    private static final Logger logger = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    private static final String ERROR_PIPE_CLOSE = "Error closing readPipeFile ";
    private static final String ERROR_PIPE_QUIT = "Error finishing readPipeFile ";
    private static final String ERROR_PIPE_NOT_FOUND = "Pipe cannot be found";
    private static final String ERROR_PIPE_NOT_READ = "Pipe cannot be read";
    private static final String ERROR_PIPE_NOT_CLOSED = "Pipe cannot be closed";
    private static final String ERROR_PIPE_READER_NOT_CLOSED = "Pipe reader cannot be closed";

    private final String readPipeFile;

    private final HashMap<Integer, Integer> jobIdsToStatus;
    private final HashMap<Integer, Semaphore> jobIdsToWaiters;

    private boolean haveWaiters;
    private boolean mustStop;
    private Semaphore stopSem;


    public TaskResultReader(String readPipeFile) {
        this.readPipeFile = readPipeFile;

        this.mustStop = false;
        this.haveWaiters = false;
        this.jobIdsToStatus = new HashMap<>();
        this.jobIdsToWaiters = new HashMap<>();
    }

    @Override
    public void run() {
        logger.info("TaskResultReader running");
        // Process pipe while we are not asked to stop or there are waiting processes
        while (!mustStop || haveWaiters) {
            readFromPipe();
            synchronized (jobIdsToWaiters) {
                haveWaiters = !this.jobIdsToWaiters.isEmpty();
            }
        }

        logger.debug("TaskResultReader stoped with: mustStop = " + mustStop + " Waiters = " + haveWaiters);

        // When shutdown signal received, release the semaphore
        stopSem.release();
    }

    public void shutdown(Semaphore sem) {
        logger.info("Ask for shutdown");

        // Order to stop
        mustStop = true;
        stopSem = sem;

        // Send pipe message with "quit" to our pipe to unlock InputStream
        // Send quit tag to pipe
        logger.debug("Send quit tag to pipe");
        boolean done = false;
        int retries = 0;
        while (!done && retries < ExternalExecutor.MAX_RETRIES) {
            FileOutputStream output = null;
            try {
                output = new FileOutputStream(readPipeFile, true);
                String quitCMD = ExternalExecutor.QUIT_TAG + ExternalExecutor.TOKEN_NEW_LINE;
                output.write(quitCMD.getBytes());
                output.flush();
            } catch (Exception e) {
                logger.warn("Error on writing on pipe. Retrying " + retries + "/" + ExternalExecutor.MAX_RETRIES);
                ++retries;
            } finally {
                if (output != null) {
                    try {
                        output.close();
                    } catch (Exception e) {
                        ErrorManager.error(ERROR_PIPE_CLOSE + readPipeFile, e);
                    }
                }
            }
            done = true;
        }
        if (!done) {
            ErrorManager.error(ERROR_PIPE_QUIT + readPipeFile);
        }

    }

    public void askForTaskEnd(int jobId, Semaphore waiter) {
        logger.debug("Ask for task " + jobId + " end");
        // If task has already finished, release it
        synchronized (jobIdsToStatus) {
            Integer exitValue = jobIdsToStatus.get(jobId);
            if (exitValue != null) {
                waiter.release();
                return;
            }
        }

        // Otherwise, register the waiter
        synchronized (jobIdsToWaiters) {
            jobIdsToWaiters.put(jobId, waiter);
        }
    }

    // Always called after released so the value always exist
    public int getExitValue(int jobId) {
        synchronized (jobIdsToStatus) {
            int exitValue = jobIdsToStatus.get(jobId);
            jobIdsToStatus.remove(jobId);
            return exitValue;
        }
    }

    private void readFromPipe() {
        FileInputStream input = null;
        BufferedReader reader = null;
        try {
            input = new FileInputStream(readPipeFile); // WARN: This call is blocking for NamedPipes
            reader = new BufferedReader(new InputStreamReader(input));

            String line = reader.readLine();
            if (line != null) {
                String[] result = line.split(" ");

                // Skip if line is not well formed
                if (result.length < 1) {
                    logger.warn("Skipping line: " + line);
                    return;
                }
                // Process line of the form: "quit"
                // This quit is received from our proper shutdown, not from bindings
                if (result[0].equals(ExternalExecutor.QUIT_TAG)) {
                    // Quit received, end
                    logger.debug("Received quit message");
                    return;
                }

                // Process line of the form: "endTask" ID STATUS
                if (result[0].equals(ExternalExecutor.END_TASK_TAG)) {
                    if (result.length < 3) {
                        logger.warn("Skipping line: " + line);
                        return;
                    }

                    int jobId = Integer.valueOf(result[1]);
                    int exitValue = Integer.valueOf(result[2]);
                    synchronized (jobIdsToStatus) {
                        jobIdsToStatus.put(jobId, exitValue);
                    }

                    // Optimization: Check directly if waiter has already registered
                    logger.debug("Read " + jobId + " with exitValue " + exitValue);
                    synchronized (jobIdsToWaiters) {
                        Semaphore waiter = jobIdsToWaiters.get(jobId);
                        if (waiter != null) {
                            // Release waiter and clean structure
                            waiter.release();
                            jobIdsToWaiters.remove(jobId);
                        }
                    }
                }
            }
        } catch (FileNotFoundException fnfe) {
            // This exception is only handled at the beggining of the execution
            // when the pipe is not created yet. Only display on debug
            logger.debug(ERROR_PIPE_NOT_FOUND);
        } catch (IOException ioe) {
            logger.error(ERROR_PIPE_NOT_READ);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    logger.error(ERROR_PIPE_READER_NOT_CLOSED);
                }
            }
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    logger.error(ERROR_PIPE_NOT_CLOSED);
                }
            }
        }
    }

}
