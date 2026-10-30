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
package es.bsc.compss.execution.platform.mirrors;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.execution.InvocationContext;
import es.bsc.compss.execution.executor.Executor;
import es.bsc.compss.execution.executor.ExecutorContext;
import es.bsc.compss.execution.executor.binding.piped.ControlPipePair;
import es.bsc.compss.execution.executor.binding.piped.PipedExecutor;
import es.bsc.compss.execution.invokers.types.CParams;
import es.bsc.compss.execution.platform.mirrors.piped.PipedMirror;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class CMirror extends PipedMirror {

    // C worker relative path
    private static final String BINDINGS_RELATIVE_PATH =
        File.separator + "Bindings" + File.separator + "bindings-common" + File.separator + "lib";
    private static final String C_PIPER = "c_piper.sh";
    private static final String LIBRARY_PATH_ENV = "LD_LIBRARY_PATH";
    private static final String C_LIB_RELATIVE_PATH =
        File.separator + "Bindings" + File.separator + "c" + File.separator + "lib";

    // Factory creating this mirror on first lazy registration
    private static final MirrorFactory<CMirror> MIRROR_FACTORY = new MirrorFactory<CMirror>() {

        @Override
        public CMirror create(ExecutorContext platform) {
            return new CMirror(platform);
        }
    };


    /**
     * Creates a new CMirror instance with the given context.
     * 
     * @param platform Mirrored platform
     */
    public CMirror(ExecutorContext platform) {
        super(platform);
        InvocationContext context = platform.getContext();
        init(context);
    }

    @Override
    public String getMirrorName() {
        return "C";
    }

    @Override
    public String getPipeBuilderContext() {
        StringBuilder cmd = new StringBuilder();
        return cmd.toString();
    }

    @Override
    public String getLaunchWorkerCommand(InvocationContext context, ControlPipePair pipe) {
        // Specific launch command is of the form: binding bindingExecutor bindingArgs
        StringBuilder cmd = new StringBuilder();

        String installDir = context.getInstallDir();

        cmd.append(installDir).append(PIPER_SCRIPT_RELATIVE_PATH).append(C_PIPER).append(TOKEN_SEP);
        int initialExecutorCount = this.mirroredContext.getSize();
        String executorPipes = basePipePath + "executor";
        cmd.append(initialExecutorCount).append(TOKEN_SEP);
        for (int i = 0; i < initialExecutorCount; ++i) {
            cmd.append(executorPipes).append(i).append(".outbound").append(TOKEN_SEP);
        }

        cmd.append(initialExecutorCount).append(TOKEN_SEP);
        for (int i = 0; i < initialExecutorCount; ++i) {
            cmd.append(executorPipes).append(i).append(".inbound").append(TOKEN_SEP);
        }
        cmd.append(pipe.getOutboundPipe()).append(TOKEN_SEP);
        cmd.append(pipe.getInboundPipe());
        return cmd.toString();
    }

    @Override
    public Map<String, String> getEnvironment(InvocationContext context) {
        String ldLibraryPath = System.getenv(LIBRARY_PATH_ENV);
        CParams cParams = (CParams) context.getLanguageParams(Lang.C);
        if (ldLibraryPath == null) {
            ldLibraryPath = cParams.getLibraryPath();
        } else {
            ldLibraryPath = ldLibraryPath.concat(":" + cParams.getLibraryPath());
        }

        // Add C and commons libs
        ldLibraryPath = ldLibraryPath.concat(":" + context.getInstallDir() + C_LIB_RELATIVE_PATH);
        ldLibraryPath = ldLibraryPath.concat(":" + context.getInstallDir() + BINDINGS_RELATIVE_PATH);
        Map<String, String> env = new HashMap<>();
        env.put(LIBRARY_PATH_ENV, ldLibraryPath);
        return env;
    }

    /**
     * Returns the PipedExecutor associated to an executor.
     *
     * @param e executor whose PipedExecutor we want to obtain
     * @return PipedExecutor corresponding to executor e
     */
    public static PipedExecutor getPipedExecutor(Executor e) {
        return e.getPlatform().getMirroredExecutor(e, CMirror.class, MIRROR_FACTORY);
    }
}
