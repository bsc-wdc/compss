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
package es.bsc.compss.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;


public class PythonTraceMerger extends TraceMerger {

    private static final String PYTHON_WORKER_SUBDIR = "python";
    private static final String PYTHON_WORKER_TRACE_SUFFIX = "_python_trace" + Tracer.TRACE_PRV_FILE_EXTENTION;

    private String workingDir;


    /**
     * Initilizes class atributes for python trace merging .
     * 
     * @param workingDir Working directory
     * @throws IOException Error managing files
     */
    public PythonTraceMerger(String workingDir) throws IOException {
        final String masterDir = workingDir + File.separator + Tracer.TRACE_SUBDIR;
        final String workersDir =
            workingDir + File.separator + Tracer.TRACE_SUBDIR + File.separator + PythonTraceMerger.PYTHON_WORKER_SUBDIR;

        this.workingDir = workingDir;

        // Init master trace information
        final File masterF = new File(masterDir);
        final String traceNamePrefix = Tracer.getTraceNamePrefix();
        final File[] matchingMasterFiles = masterF.listFiles((File dir, String name) -> name.startsWith(traceNamePrefix)
            && name.endsWith(Tracer.TRACE_PRV_FILE_EXTENTION));
        if (matchingMasterFiles == null || matchingMasterFiles.length < 1) {
            throw new FileNotFoundException("Master trace " + traceNamePrefix + "*" + Tracer.TRACE_PRV_FILE_EXTENTION
                + " not found at directory " + masterDir);
        }
        if (matchingMasterFiles.length > 1) {
            LOGGER.warn("Found more than one master trace, using " + matchingMasterFiles[0] + " to merge.");
        }
        setUpMaster(matchingMasterFiles[0]);

        // set up workers

        final File workerF = new File(workersDir);

        File[] workersTraces = workerF.listFiles((File dir, String name) -> name.endsWith(PYTHON_WORKER_TRACE_SUFFIX));

        setUpWorkers(workersTraces);
        LOGGER.debug("Trace's merger initialization successful");
    }

    /**
     * Removes temporal files.
     */
    public void removeTemporalFiles() {
        if (!DEBUG) {
            String workerFolder =
                workingDir + File.separator + Tracer.TRACE_SUBDIR + File.separator + PYTHON_WORKER_SUBDIR;
            LOGGER.debug("Removing folder " + workerFolder);
            try {
                removeFolder(workerFolder);
            } catch (IOException ioe) {
                LOGGER.warn("Could not remove python temporal tracing folder" + ioe.toString());
            }
        }
    }

    /**
     * Merges the python traces with the master.
     */
    public void merge() throws Exception {
        mergePRVsWithTraceNumAndSyncEvents();
        mergePCFsHardwareCounters();
        LOGGER.debug("Merging finished.");
        removeTemporalFiles();
    }

}
