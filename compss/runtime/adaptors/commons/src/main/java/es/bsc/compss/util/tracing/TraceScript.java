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

package es.bsc.compss.util.tracing;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.StreamGobbler;
import es.bsc.compss.util.Tracer;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Wrapper for the tracer script.
 */
public class TraceScript {

    // Installation Path
    public static final String RELATIVE_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system"
        + File.separator + "trace" + File.separator + "trace.sh";

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    public static final int start(String iDir, String traceDir, String eventType, String taskId, String slot)
        throws IOException, InterruptedException {
        return invokeScript(iDir, "start", traceDir, eventType, taskId, slot);
    }

    public static final int end(String iDir, String traceDir, String eventType, String slot)
        throws IOException, InterruptedException {
        return invokeScript(iDir, "end", traceDir, slot);
    }

    public static final int init(String iDir, String traceDir, String node, String nSlot)
        throws IOException, InterruptedException {
        return invokeScript(iDir, "init", traceDir, node, nSlot);
    }

    public static final int package_extrae(String iDir, String traceDir, String packagePath, String hostId)
        throws IOException, InterruptedException {
        return invokeScript(iDir, "package", traceDir, packagePath, hostId);
    }

    public static final int gentrace_extrae(String iDir, String inDir, String outDir, String traceName,
        int numParallelMergeProcs) throws IOException, InterruptedException {
        return invokeScript(iDir, "gentrace", inDir, outDir, traceName, Integer.toString(numParallelMergeProcs));
    }

    private static int invokeScript(String iDir, String cmd, String workingDirPath, String... args)
        throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(iDir + File.separator + RELATIVE_PATH);
        List<String> command = pb.command();
        command.add(cmd);
        command.add(workingDirPath);
        for (String arg : args) {
            command.add(arg);
        }

        // Setup process environment -- Tracing entries
        Tracer.prepareEnvironment(pb.environment(), false);

        Process p;
        p = pb.start();

        if (DEBUG) {
            StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), null, LOGGER, false);
            StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), null, LOGGER, true);
            outputGobbler.start();
            errorGobbler.start();
        }

        return p.waitFor();
    }
}
