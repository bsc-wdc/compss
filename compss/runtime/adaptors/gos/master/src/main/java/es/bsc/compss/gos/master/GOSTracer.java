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
package es.bsc.compss.gos.master;

import com.jcraft.jsch.JSchException;
import es.bsc.compss.gos.master.sshutils.SSHChannel;
import es.bsc.compss.gos.master.sshutils.SSHHost;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.util.tracing.TraceScript;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;


public class GOSTracer extends Tracer {

    /**
     * Waits for the tracing job to finish.
     */
    public static void waitForTracing(GOSWorkerNode node) {
    }

    public static void startTracing(GOSWorkerNode node) {

    }

    /**
     * Generates the tracing package in a given worker node.
     *
     * @param node Worker node where to generate the tracing package.
     */
    public static void generateAnalysisFiles(GOSWorkerNode node) throws JSchException, IOException {
        String traceScript;
        final LinkedList<String> traceParams = new LinkedList<>();
        final SSHHost host = node.getSSHHost();
        final String installDir = node.getConfig().getInstallDir();
        final String workingDir = node.getConfig().getWorkingDir();

        traceScript = installDir + TraceScript.RELATIVE_PATH;
        String mode = "package";
        String pars = mode + " " + workingDir + " " + host;

        SSHChannel ch = host.openChannel("exec", "generatePackageTracing");
        ch.setCommand(traceScript + " " + pars);
        File f = new File("trace_packaging_" + host.getFullHostName());
        f.createNewFile();
        ch.setOutputStream(Files.newOutputStream(Paths.get(f.getPath())));

        ch.connect();

    }

}
