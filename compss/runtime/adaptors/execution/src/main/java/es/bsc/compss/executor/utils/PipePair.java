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
package es.bsc.compss.executor.utils;

import es.bsc.compss.invokers.commands.piped.EndTaskPipeCommand;
import es.bsc.compss.invokers.commands.piped.ErrorTaskPipeCommand;
import es.bsc.compss.invokers.commands.piped.QuitPipeCommand;
import es.bsc.compss.invokers.external.ExternalCommand;
import es.bsc.compss.invokers.external.piped.PipeCommand;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PipePair {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    // Logger messages
    private static final String ERROR_PIPE_CLOSE = "Error on closing pipe ";
    private static final String ERROR_PIPE_QUIT = "Error finishing readPipeFile ";
    private static final String ERROR_PIPE_NOT_FOUND = "Pipe cannot be found";
    private static final String ERROR_PIPE_NOT_READ = "Pipe cannot be read";

    protected static final String TOKEN_NEW_LINE = "\n";
    protected static final String TOKEN_SEP = " ";

    private static final int MAX_WRITE_PIPE_RETRIES = 3;
    private final String pipePath;

    public PipePair(String basePipePath, String id) {
        pipePath = basePipePath + id;
    }

    public boolean send(PipeCommand command) {
        boolean done = false;
        int retries = 0;
        String taskCMD = command.getAsString();
        String writePipe = pipePath + ".outbound";
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("EXECUTOR COMMAND: " + taskCMD + " @ " + writePipe);
        }
        taskCMD = taskCMD + TOKEN_NEW_LINE;
        while (!done && retries < MAX_WRITE_PIPE_RETRIES) {
            // Send to pipe : task tID command(jobOut jobErr externalCMD) \n
            OutputStream output = null;
            try {
                output = new FileOutputStream(writePipe, true);
                output.write(taskCMD.getBytes());
                output.flush();
                done = true;
            } catch (Exception e) {
                LOGGER.debug("Error on pipe write. Retry");
                ++retries;
            } finally {
                if (output != null) {
                    try {
                        output.close();
                    } catch (Exception e) {
                        ErrorManager.error(ERROR_PIPE_CLOSE + writePipe, e);
                    }
                }
            }
        }
        return done;
    }

    @Override
    public String toString() {
        return "READ pipe: " + pipePath + ".inbound  WRITE pipe:" + pipePath + ".outbound";
    }

    public PipeCommand read() {
        PipeCommand readCommand = null;
        try {
            FileInputStream input = new FileInputStream(pipePath + ".inbound");// WARN: This call is blocking for
            // NamedPipes

            String line = null;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
                line = reader.readLine();
            }
            if (line != null) {
                String[] result = line.split(" ");

                // Skip if line is not well formed
                if (result.length < 1) {
                    LOGGER.warn("Skipping line: " + line);
                    return null;
                }
                // Process the received tag
                ExternalCommand.CommandType commandType = ExternalCommand.CommandType.valueOf(result[0].toUpperCase());
                switch (commandType) {
                    case QUIT:
                        // This quit is received from our proper shutdown, not from bindings. We just end
                        LOGGER.debug("Received quit message");
                        break;
                    case END_TASK:
                        LOGGER.debug("Received endTask message: " + line);
                        if (result.length < 3) {
                            LOGGER.warn("WARN: Skipping endTask line because is malformed");
                            break;
                        }
                        // Line of the form: "endTask" ID STATUS D paramType1 paramValue1 ... paramTypeD paramValueD
                        readCommand = new EndTaskPipeCommand(result);
                        break;
                    case ERROR_TASK:
                        LOGGER.debug("Received errorTask message: " + line);
                        // We have received a fatal error from bindings, we notify error to every waiter and end
                        readCommand = new ErrorTaskPipeCommand(result);
                        break;
                    case EXECUTE_TASK:
                    case REMOVE:
                    case SERIALIZE:
                    // Should not receive any of these tags
                    default:
                        LOGGER.warn("Unrecognised tag on PipedMirror: " + result[0] + ". Skipping message");
                        break;
                }
            }
        } catch (FileNotFoundException fnfe) {
            // This exception is only handled at the beginning of the execution
            // when the pipe is not created yet. Only display on debug
            LOGGER.debug(ERROR_PIPE_NOT_FOUND + " " + pipePath + ".inbound");
        } catch (IOException ioe) {
            LOGGER.error(ERROR_PIPE_NOT_READ);
        }
        return readCommand;
    }

    public void close() {
        // Send quit tag to pipe
        String writePipe = pipePath + ".outbound";
        if (new File(writePipe).exists()) {
            boolean done = this.send(new QuitPipeCommand());
            if (!done) {
                ErrorManager.error(ERROR_PIPE_QUIT + writePipe);
            }
        }
    }
}
