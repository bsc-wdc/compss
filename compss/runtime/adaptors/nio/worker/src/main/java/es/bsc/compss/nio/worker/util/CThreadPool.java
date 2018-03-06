/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.nio.worker.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.exceptions.InitializationException;
import es.bsc.compss.nio.worker.executors.CExecutor;
import es.bsc.compss.nio.worker.executors.ExternalExecutor;
import es.bsc.compss.util.ErrorManager;


/**
 * Representation of a C Thread Pool
 * 
 */
public class CThreadPool extends ExternalThreadPool {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);

    // C worker relative path
    private static final String C_PIPER = "c_piper.sh";
    private static final String PERSISTENT_WORKER_C = "/worker/persistent_worker_c";
    private static final String LOG_PREFIX = "[CThreadPool] ";
    public static final int MAX_RETRIES = 3;


    /**
     * Creates a new C Thread Pool associated to the given worker and with fixed size
     * 
     * @param nw
     * @param size
     * @throws IOException
     */
    public CThreadPool(NIOWorker nw, int size) {
        super(nw, size);
    }

    /**
     * Starts the threads of the pool
     * 
     */
    @Override
    public void startThreads() throws InitializationException {
        LOGGER.info(LOG_PREFIX + "Start threads for C-binding");
        int i = 0;
        for (Thread t : workerThreads) {
            CExecutor executor = new CExecutor(nw, this, queue, writePipeFiles[i], taskResultReader[i]);
            t = new Thread(executor);
            t.setName(JOB_THREADS_POOL_NAME + " pool thread # " + i);
            t.start();
            i = i + 1;
        }
        sem.acquireUninterruptibly(this.size);
        LOGGER.debug(LOG_PREFIX + "Finished C ThreadPool");
    }

    @Override
    public String getLaunchCommand() {
        // Specific launch command is of the form: binding bindingExecutor bindingArgs
        StringBuilder cmd = new StringBuilder();

        cmd.append(COMPSsConstants.Lang.C).append(ExternalExecutor.TOKEN_SEP);
        if (!NIOWorker.isPersistentCEnabled()) {
            // No persistent version
            cmd.append(installDir).append(ExternalThreadPool.PIPER_SCRIPT_RELATIVE_PATH).append(C_PIPER).append(ExternalExecutor.TOKEN_SEP);
        } else {
            // Persistent version

            if (nw.getAppDir() != null && !nw.getAppDir().isEmpty()) {
                String nx_args = "--enable-block";
                String compss_nx_args;
                if ((compss_nx_args = System.getenv("COMPSS_NX_ARGS")) != null) {
                    nx_args = nx_args.concat(" " + compss_nx_args);
                }
                if (LOGGER.isDebugEnabled()) {
                    nx_args = nx_args.concat(" --summary --verbose");
                }
                cmd.append("NX_ARGS='" + nx_args + "' ").append(nw.getAppDir()).append(PERSISTENT_WORKER_C)
                        .append(ExternalExecutor.TOKEN_SEP);
                // Adding Data pipes in the case of persistent worker
                cmd.append(writeDataPipeFile).append(ExternalExecutor.TOKEN_SEP).append(readDataPipeFile)
                        .append(ExternalExecutor.TOKEN_SEP);
            } else {
                ErrorManager.warn("Appdir is not defined. It is mandatory for c/c++ binding");
                return null;
            }
        }

        cmd.append(writePipeFiles.length).append(ExternalExecutor.TOKEN_SEP);
        for (int i = 0; i < writePipeFiles.length; ++i) {
            cmd.append(writePipeFiles[i]).append(ExternalExecutor.TOKEN_SEP);
        }

        cmd.append(readPipeFiles.length).append(ExternalExecutor.TOKEN_SEP);
        for (int i = 0; i < readPipeFiles.length; ++i) {
            cmd.append(readPipeFiles[i]).append(ExternalExecutor.TOKEN_SEP);
        }

        return cmd.toString();
    }

    @Override
    public Map<String, String> getEnvironment(NIOWorker nw) {
        return CExecutor.getEnvironment(nw);
    }

    @Override
    public void removeExternalData(String dataID) {
        /*
         * MANAGEMENT OF EXTERNAL DATA REMOVE: Send external data removals requests with data id through the data PIPE
         * to remove data in the persistent worker cache.
         */

        String cmd = ExternalExecutor.REMOVE_TAG + ExternalExecutor.TOKEN_SEP + dataID + ExternalExecutor.TOKEN_NEW_LINE;
        boolean done = false;
        int retries = 0;
        while (!done && retries < MAX_RETRIES) {
            LOGGER.debug(LOG_PREFIX + "Trying to remove data " + dataID);
            try (FileOutputStream output = new FileOutputStream(writeDataPipeFile, true);) {
                output.write(cmd.getBytes());
                output.flush();
                output.close();
                done = true;
            } catch (Exception e) {
                LOGGER.warn(LOG_PREFIX + "Error on writing on pipe " + writeDataPipeFile + ". Retrying " + retries + "/" + MAX_RETRIES);
                ++retries;
            }
        }
        if (!done) {
            LOGGER.warn("ERROR: Data " + dataID + " has not been removed because cannot write in pipe");
        }

    }

    @Override
    protected void specificStop() {
        if (NIOWorker.isPersistentCEnabled()) {
            LOGGER.debug(LOG_PREFIX + " Sending Quit to data pipe");
            try (FileOutputStream output = new FileOutputStream(writeDataPipeFile, true);) {
                String quitCMD = ExternalExecutor.QUIT_TAG + ExternalExecutor.TOKEN_NEW_LINE;
                output.write(quitCMD.getBytes());
                output.flush();
                output.close();

            } catch (Exception e) {
                ErrorManager.error(LOG_PREFIX + "Error on writing on pipe " + writeDataPipeFile, e);
            }
        }
        super.specificStop();
    }

    @Override
    public boolean serializeExternalData(String dataId, String path) {
        /*
         * MANAGEMENT OF EXTERNAL DATA SERIALIZATION Send external data serialization request through data pipes. The
         * presistent worker will if the data is in the cache and then serialize it to the file in the path. Return true
         * if serialization was done, false otherwise
         */
        LOGGER.debug(LOG_PREFIX + "Request to serialize " + dataId + " at " + path);

        String cmd = ExternalExecutor.SERIALIZE_TAG + ExternalExecutor.TOKEN_SEP + dataId + ExternalExecutor.TOKEN_SEP + path
                + ExternalExecutor.TOKEN_NEW_LINE;

        // check if the data is expected?
        boolean done = false;
        int retries = 0;
        while (!done && retries < MAX_RETRIES) {

            try (FileOutputStream output = new FileOutputStream(writeDataPipeFile, true);) {
                output.write(cmd.getBytes());
                output.flush();
                output.close();
                done = true;
            } catch (Exception e) {
                LOGGER.warn(LOG_PREFIX + "Error on writing on pipe " + writeDataPipeFile + ". Retrying " + retries + "/" + MAX_RETRIES);
                ++retries;
            }
        }

        // Wait for the result
        if (done) {
            try (FileInputStream input = new FileInputStream(readDataPipeFile);) {
                LOGGER.debug(LOG_PREFIX + "Waiting for serialization results");
                input.read();
                input.close();
                done = true;
                LOGGER.debug(LOG_PREFIX + "Data " + dataId + "serialized at " + path);
            } catch (Exception e) {
                LOGGER.warn(LOG_PREFIX + "Error on writing on pipe " + writeDataPipeFile + ". Retrying " + retries + "/" + MAX_RETRIES);
                done = false;
            }
        }

        if (!done) {
            LOGGER.warn("ERROR: Data " + dataId + " has not been serialized because cannot write in pipe");
            return false;
        }
        return true;
    }

}
