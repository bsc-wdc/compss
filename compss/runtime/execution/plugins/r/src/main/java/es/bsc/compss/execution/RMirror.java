/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.execution;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.execution.types.ExecutorContext;
import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.external.piped.ControlPipePair;
import es.bsc.compss.executor.external.piped.PipedExecutor;
import es.bsc.compss.executor.external.piped.PipedMirror;
import es.bsc.compss.invokers.types.RParams;
import es.bsc.compss.types.execution.InvocationContext;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class RMirror extends PipedMirror {

    // C worker relative path
    private static final String BINDINGS_RELATIVE_PATH =
        File.separator + "Bindings" + File.separator + "bindings-common" + File.separator + "lib";
    private static final String R_PIPER = "r_piper.sh";
    private static final String LIBRARY_PATH_ENV = "LD_LIBRARY_PATH";
    private static final String R_LIB_RELATIVE_PATH =
        File.separator + "Bindings" + File.separator + "RCOMPSs" + File.separator + "lib";
    private static final String R_LIB_DUMMY_EXTRAE_RELATIVE_PATH =
        File.separator + "Bindings" + File.separator + "RCOMPSs" + File.separator + "dummy_extrae";
    private static final String R_LIBS_PATH_ENV = "R_LIBS";

    // Piped Executor Cache
    private static final ThreadLocal<PipedExecutor> executor = new ThreadLocal<>();


    /**
     * Creates a new CMirror instance with the given context and size.
     *
     * @param platform Mirrored platform
     */
    public RMirror(ExecutorContext platform) {
        super(platform);
        InvocationContext context = platform.getContext();
        init(context);
    }

    @Override
    public String getMirrorName() {
        return "R";
    }

    @Override
    public String getPipeBuilderContext() {
        StringBuilder cmd = new StringBuilder();
        return cmd.toString();
    }

    @Override
    public String getLaunchWorkerCommand(InvocationContext context, ControlPipePair pipe) {
        // TODO: Specific launch command is of the form: binding bindingExecutor bindingArgs
        StringBuilder cmd = new StringBuilder();

        String installDir = context.getInstallDir();

        cmd.append(installDir).append(PIPER_SCRIPT_RELATIVE_PATH).append(R_PIPER).append(TOKEN_SEP);

        String executorPipes = basePipePath + "executor";
        int initialNumExecutors = this.mirroredContext.getSize();
        cmd.append(initialNumExecutors).append(TOKEN_SEP);
        for (int i = 0; i < initialNumExecutors; ++i) {
            cmd.append(executorPipes).append(i).append(".outbound").append(TOKEN_SEP);
        }

        cmd.append(initialNumExecutors).append(TOKEN_SEP);
        for (int i = 0; i < initialNumExecutors; ++i) {
            cmd.append(executorPipes).append(i).append(".inbound").append(TOKEN_SEP);
        }
        cmd.append(pipe.getOutboundPipe()).append(TOKEN_SEP);
        cmd.append(pipe.getInboundPipe());
        return cmd.toString();
    }

    @Override
    public Map<String, String> getEnvironment(InvocationContext context) {
        String ldLibraryPath = System.getenv(LIBRARY_PATH_ENV);
        String rlibs = System.getenv(R_LIBS_PATH_ENV);
        RParams rParams = (RParams) context.getLanguageParams(Lang.R);

        if (rlibs == null) {
            rlibs = rParams.getRPath();
        } else {
            rlibs = rlibs.concat(":" + rParams.getRPath());
        }

        // Add R and commons libs
        if (ldLibraryPath == null) {
            ldLibraryPath = context.getInstallDir() + R_LIB_RELATIVE_PATH;
        } else {
            ldLibraryPath = ldLibraryPath.concat(":" + context.getInstallDir() + R_LIB_RELATIVE_PATH);
        }
        ldLibraryPath = ldLibraryPath.concat(":" + context.getInstallDir() + BINDINGS_RELATIVE_PATH);
        ldLibraryPath = ldLibraryPath.concat(":" + context.getInstallDir() + R_LIB_DUMMY_EXTRAE_RELATIVE_PATH);
        Map<String, String> env = new HashMap<>();
        env.put(LIBRARY_PATH_ENV, ldLibraryPath);
        env.put(R_LIBS_PATH_ENV, rlibs);
        return env;
    }

    /**
     * Returns the PipedExecutor associated to an executor.
     *
     * @param e executor whose PipedExecutor we want to obtain
     * @return PipedExecutor corresponding to executor e
     */
    public static PipedExecutor getPipedExecutor(Executor e) {
        PipedExecutor pExec = executor.get();
        if (pExec == null || !pExec.canRun()) {
            ExecutorContext platform = e.getPlatform();
            RMirror mirror;
            synchronized (platform) {
                mirror = (RMirror) platform.getMirror(RMirror.class);
                if (mirror == null) {
                    mirror = new RMirror(platform);
                    platform.registerMirror(RMirror.class, mirror);
                }
            }
            pExec = mirror.registerExecutor(e);
            executor.set(pExec);
        }
        return pExec;
    }
}
