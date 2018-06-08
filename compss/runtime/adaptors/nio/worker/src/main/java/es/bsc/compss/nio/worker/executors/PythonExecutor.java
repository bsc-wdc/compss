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
package es.bsc.compss.nio.worker.executors;

import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.util.JobsThreadPool;
import es.bsc.compss.nio.worker.util.TaskResultReader;
import es.bsc.compss.util.RequestQueue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class PythonExecutor extends ExternalExecutor {

    public static final String PYCOMPSS_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "python";

    private static final String ENV_LD_LIBRARY_PATH = "LD_LIBRARY_PATH";
    private static final String ENV_PYTHONPATH = "PYTHONPATH";


    public PythonExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue, String writePipe, TaskResultReader resultReader) {
        super(nw, pool, queue, writePipe, resultReader); //WriteDataPipe is not used until Python has data management
    }

    @Override
    protected ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs) {

        // The execution command in Python is empty (the handler adds the pre-command and the application args)
        return new ArrayList<>();
    }

    public static Map<String, String> getEnvironment(NIOWorker nw) {
        // PyCOMPSs HOME
        Map<String, String> env = new HashMap<>();
        String pycompssHome = nw.getInstallDir() + PYCOMPSS_RELATIVE_PATH + File.separator + NIOWorker.getPythonVersion();
        env.put("PYCOMPSS_HOME", pycompssHome);

        // PYTHONPATH
        String pythonPath = System.getenv(ENV_PYTHONPATH);
        if (pythonPath == null) {
            pythonPath = pycompssHome + ":" + nw.getPythonpath() + ":" + nw.getAppDir();
        } else {
            pythonPath = pycompssHome + ":" + nw.getPythonpath() + ":" + nw.getAppDir() + pythonPath;
        }

        env.put(ENV_PYTHONPATH, pythonPath);

        // LD_LIBRARY_PATH
        String ldLibraryPath = System.getenv(ENV_LD_LIBRARY_PATH);
        if (ldLibraryPath == null) {
            ldLibraryPath = nw.getLibPath();
        } else {
            ldLibraryPath = ldLibraryPath.concat(":" + nw.getLibPath());
        }
        String bindingsHome = nw.getInstallDir() + BINDINGS_RELATIVE_PATH;
        ldLibraryPath = ldLibraryPath.concat(":" + bindingsHome);
        env.put(ENV_LD_LIBRARY_PATH, ldLibraryPath);

        return env;
    }

}
