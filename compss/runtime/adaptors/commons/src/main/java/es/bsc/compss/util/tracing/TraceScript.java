/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();


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

    public static final int package_extrae(String iDir, String traceDir, String node, String hostId)
        throws IOException, InterruptedException {
        return invokeScript(iDir, "package", traceDir, node, hostId);
    }

    public static int package_scorep(String iDir, String traceDir, String node)
        throws IOException, InterruptedException {
        return invokeScript(iDir, "package-scorep", traceDir, node);
    }

    public static final int package_map(String iDir, String traceDir, String node)
        throws IOException, InterruptedException {
        return invokeScript(iDir, "package-map", traceDir, node);
    }

    public static final int gentrace_extrae(String iDir, String traceDir, String appName, String numResources)
        throws IOException, InterruptedException {
        return invokeScript(iDir, "gentrace", traceDir, appName, numResources);
    }

    public static final int gentrace_scorep(String iDir, String traceDir, String appName, String numResources)
        throws IOException, InterruptedException {
        return invokeScript(iDir, "gentrace_scorep", traceDir, appName, numResources);
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
        pb.environment().remove(Tracer.LD_PRELOAD);
        Process p;

        p = pb.start();

        if (DEBUG) {
            StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), System.out, LOGGER, false);
            StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), System.err, LOGGER, true);
            outputGobbler.start();
            errorGobbler.start();
        }

        return p.waitFor();
    }
}
