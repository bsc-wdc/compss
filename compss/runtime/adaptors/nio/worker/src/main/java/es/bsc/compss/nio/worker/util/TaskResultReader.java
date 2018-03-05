package es.bsc.compss.nio.worker.util;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.worker.executors.ExternalExecutor;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.util.ErrorManager;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Task Result Reader
 *
 */
public class TaskResultReader extends Thread {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    private static final String ERROR_PIPE_QUIT = "Error finishing readPipeFile ";
    private static final String ERROR_PIPE_NOT_FOUND = "Pipe cannot be found";
    private static final String ERROR_PIPE_NOT_READ = "Pipe cannot be read";

    private final String readPipeFile;

    private final Map<Integer, ExternalTaskStatus> jobIdsToStatus;
    private final Map<Integer, Semaphore> jobIdsToWaiters;

    private boolean haveWaiters;
    private boolean mustStop;
    private Semaphore stopSem;


    /**
     * Create a new task result reader on a given pipe
     * 
     * @param readPipeFile
     */
    public TaskResultReader(String readPipeFile) {
        this.readPipeFile = readPipeFile;

        this.mustStop = false;
        this.haveWaiters = false;
        this.jobIdsToStatus = new HashMap<>();
        this.jobIdsToWaiters = new HashMap<>();
    }

    @Override
    public void run() {
        LOGGER.info("TaskResultReader running");
        // Process pipe while we are not asked to stop or there are waiting processes
        while (!this.mustStop || this.haveWaiters) {
            readFromPipe();
            synchronized (this.jobIdsToWaiters) {
                this.haveWaiters = !this.jobIdsToWaiters.isEmpty();
            }
        }

        LOGGER.debug("TaskResultReader stoped with: mustStop = " + this.mustStop + " Waiters = " + this.haveWaiters);

        // When shutdown signal received, release the semaphore
        this.stopSem.release();
    }

    /**
     * Sends the quit tag to the external executor. Blocks the semaphore @sem until the executor is stopped
     * 
     * @param sem
     */
    public void shutdown(Semaphore sem) {
        LOGGER.info("Ask for shutdown");

        // Order to stop
        this.mustStop = true;
        this.stopSem = sem;

        // Send pipe message with "quit" to our pipe to unlock InputStream
        // Send quit tag to pipe
        LOGGER.debug("Send quit tag to pipe");
        boolean done = false;
        int retries = 0;
        while (!done && retries < ExternalExecutor.MAX_RETRIES) {
            try (FileOutputStream output = new FileOutputStream(this.readPipeFile, true)) {
                String quitCMD = ExternalExecutor.QUIT_TAG + ExternalExecutor.TOKEN_NEW_LINE;
                output.write(quitCMD.getBytes());
                output.flush();

                done = true;
            } catch (IOException ioe) {
                LOGGER.warn("Error on writing on pipe. Retrying " + retries + "/" + ExternalExecutor.MAX_RETRIES);
                ++retries;
            }
        }

        if (!done) {
            ErrorManager.error(ERROR_PIPE_QUIT + this.readPipeFile);
        }

    }

    /**
     * Registers a semaphore to wait for a task completion
     * 
     * @param jobId
     * @param waiter
     */
    public void askForTaskEnd(int jobId, Semaphore waiter) {
        LOGGER.debug("Ask for task " + jobId + " end");

        // If task has already finished, release it
        synchronized (this.jobIdsToStatus) {
            ExternalTaskStatus status = this.jobIdsToStatus.get(jobId);
            if (status != null) {
                waiter.release();
                return;
            }
        }

        // Otherwise, register the waiter
        synchronized (this.jobIdsToWaiters) {
            this.jobIdsToWaiters.put(jobId, waiter);
        }
    }

    /**
     * Returns the task status. Must be always called after the release of the task end
     * 
     * @param jobId
     * @return
     */
    public ExternalTaskStatus getTaskStatus(int jobId) {
        synchronized (this.jobIdsToStatus) {
            ExternalTaskStatus taskStatus = this.jobIdsToStatus.get(jobId);
            this.jobIdsToStatus.remove(jobId);
            return taskStatus;
        }
    }

    private void readFromPipe() {
        try (FileInputStream input = new FileInputStream(readPipeFile); // WARN: This call is blocking for NamedPipes
                BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {

            String line = reader.readLine();
            if (line != null) {
                String[] result = line.split(" ");

                // Skip if line is not well formed
                if (result.length < 1) {
                    LOGGER.warn("Skipping line: " + line);
                    return;
                }

                // Process the received tag
                switch (result[0]) {
                    case ExternalExecutor.QUIT_TAG:
                        // This quit is received from our proper shutdown, not from bindings. We just end
                        LOGGER.debug("Received quit message");
                        return;
                    case ExternalExecutor.END_TASK_TAG:
                        LOGGER.debug("Received endTask message: " + line);
                        // Line of the form: "endTask" ID STATUS D paramType1 paramValue1 ... paramTypeD paramValueD
                        processEndTaskTag(result);
                        break;
                    case ExternalExecutor.ERROR_TASK_TAG:
                        LOGGER.debug("Received erroTask message: " + line);
                        // We have received a fatal error from bindings, we notify error to every waiter and end
                        processErrorTaskTag();
                        return;
                    default:
                        LOGGER.warn("Unrecognised tag: " + result[0] + ". Skipping message");
                        break;
                }
            }
        } catch (FileNotFoundException fnfe) {
            // This exception is only handled at the beginning of the execution
            // when the pipe is not created yet. Only display on debug
            LOGGER.debug(ERROR_PIPE_NOT_FOUND);
        } catch (IOException ioe) {
            LOGGER.error(ERROR_PIPE_NOT_READ);
        }
    }

    private void processEndTaskTag(String[] line) {
        if (line.length < 3) {
            LOGGER.warn("WARN: Skipping endTask line because is malformed");
            return;
        }

        // Line of the form: "endTask" ID STATUS D paramType1 paramValue1 ... paramTypeD paramValueD
        Integer jobId = Integer.parseInt(line[1]);
        Integer exitValue = Integer.parseInt(line[2]);
        ExternalTaskStatus taskStatus = new ExternalTaskStatus(exitValue);

        // Process parameters if message contains them
        if (line.length > 3) {
            int numParams = Integer.parseInt(line[3]);

            if (4 + 2 * numParams != line.length) {
                LOGGER.warn("WARN: Skipping endTask parameters because of malformation.");
            } else {
                // Process parameters
                for (int i = 0; i < numParams; ++i) {
                    int paramTypeOrdinalIndex = 0;
                    try {
                        paramTypeOrdinalIndex = Integer.parseInt(line[4 + 2 * i]);
                    } catch (NumberFormatException nfe) {
                        LOGGER.warn("WARN: Number format exception on " + line[4 + 2 * i] + ". Setting type 0", nfe);
                    }
                    DataType paramType = DataType.values()[paramTypeOrdinalIndex];

                    String paramValue = line[5 + 2 * i];
                    if (paramValue.equalsIgnoreCase("null")) {
                        paramValue = null;
                    }
                    taskStatus.addParameter(paramType, paramValue);
                }
            }
        } else {
            LOGGER.warn("WARN: endTask message does not have task result parameters");
        }

        // Add the task status to the set
        synchronized (jobIdsToStatus) {
            jobIdsToStatus.put(jobId, taskStatus);
        }

        // Optimization: Check directly if waiter has already registered
        LOGGER.debug("Read job " + jobId + " with status " + taskStatus);
        synchronized (jobIdsToWaiters) {
            Semaphore waiter = jobIdsToWaiters.get(jobId);
            if (waiter != null) {
                // Release waiter and clean structure
                waiter.release();
                jobIdsToWaiters.remove(jobId);
            }
        }
    }

    private void processErrorTaskTag() {
        synchronized (jobIdsToWaiters) {
            // Release all waiters with failed value
            for (Entry<Integer, Semaphore> entry : jobIdsToWaiters.entrySet()) {
                Integer jobId = entry.getKey();
                Semaphore waiter = entry.getValue();

                Integer exitValue = -1;
                ExternalTaskStatus taskStatus = new ExternalTaskStatus(exitValue);
                // Add the task status to the set
                synchronized (jobIdsToStatus) {
                    jobIdsToStatus.put(jobId, taskStatus);
                }
                // Relase the waiter
                waiter.release();
            }

            // Remove all waiters
            jobIdsToWaiters.clear();
        }
    }

}
