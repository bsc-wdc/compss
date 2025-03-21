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
package es.bsc.compss.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import org.apache.logging.log4j.Logger;


/**
 * Support class to retrieve external processes output.
 */
public class StreamGobbler extends Thread {

    private static final int BUFFER_SIZE = 4_096;

    private final InputStream in;
    private final PrintStream out;
    private final Logger logger;

    private boolean errorStream;


    /**
     * Creates a new StreamGobbler for is {@code in} and prints information to os {@code out}.
     *
     * @param in Input stream.
     * @param out Output stream.
     * @param logger Logger.
     */
    public StreamGobbler(InputStream in, PrintStream out, Logger logger, boolean errorStream) {
        this.setName("Stream Gobbler");

        this.in = in;
        this.out = out;
        this.logger = logger;
        this.errorStream = errorStream;
    }

    @Override
    public void run() {
        logger.debug("Starting stream goobler");
        try {
            final byte[] buffer = new byte[BUFFER_SIZE];
            int nRead;
            while ((nRead = in.read(buffer, 0, buffer.length)) != -1) {
                byte[] readData = new byte[nRead];
                System.arraycopy(buffer, 0, readData, 0, nRead);
                String data = new String(readData);
                if (out != null) {
                    out.print(data);
                    out.flush();
                } else if (errorStream) {
                    logger.error(data);
                } else {
                    logger.info(data);
                }
            }
        } catch (IOException ioe) {
            logger.error("Exception during reading/writing in output Stream", ioe);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ioe) {
                    logger.warn("Exception closing IN InputStream", ioe);
                }
            }

            if (out != null) {
                out.flush();
                // out.close();
            }
        }
    }

}
