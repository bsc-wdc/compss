package es.bsc.compss.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;


public class PythonTraceMerger extends TraceMerger {

    private static final String PYTHON_WORKER_SUBDIR = "python";
    private static final String PYTHON_WORKER_TRACE_SUFFIX = "_python_trace" + TRACE_EXTENSION;
    private static final String TRACE_SUBDIR = "trace";


    /**
     * Initilizes class atributes for python trace merging .
     * 
     * @param workingDir Working directory
     * @param appName Application name
     * @throws IOException Error managing files
     */
    public PythonTraceMerger(String workingDir, String appName) throws IOException {
        String masterDir = workingDir + File.separator + TRACE_SUBDIR;
        String workersDir =
            workingDir + File.separator + TRACE_SUBDIR + File.separator + PythonTraceMerger.PYTHON_WORKER_SUBDIR;
        LOGGER.debug("______masterDir" + masterDir);
        LOGGER.debug("______workersDir" + workersDir);

        // for (File f : workersTraces) {
        // LOGGER.debug("______this.workersTraces.listFiles() " + f);
        // }

        // LOGGER.debug("______TraceMerger.workingDir" + TraceMerger.workingDir);
        // LOGGER.debug("______masterTrace" + masterTrace);
        // LOGGER.debug("______workersTraces" + workersTraces);
        // LOGGER.debug("______masterTracePath" + masterTracePath);
        // LOGGER.debug("______masterTracePcfPath" + masterTracePcfPath);
        // LOGGER.debug("______workersTracePath" + workersTracePath);
        // LOGGER.debug("______workersTracePcfPath" + workersTracePcfPath);
        // LOGGER.debug("______masterWriter" + masterWriter);
        // LOGGER.debug("______masterPcfWriter" + masterPcfWriter);

        // Init master trace information
        final File masterF = new File(masterDir);
        final String traceNamePrefix = (appName != null ? appName : "") + MASTER_TRACE_SUFFIX;
        final File[] matchingMasterFiles = masterF
            .listFiles((File dir, String name) -> name.startsWith(traceNamePrefix) && name.endsWith(TRACE_EXTENSION));
        if (matchingMasterFiles == null || matchingMasterFiles.length < 1) {
            throw new FileNotFoundException(
                "Master trace " + traceNamePrefix + "*" + TRACE_EXTENSION + " not found at directory " + masterDir);
        }
        if (matchingMasterFiles.length > 1) {
            LOGGER.warn("Found more than one master trace, using " + matchingMasterFiles[0] + " to merge.");
        }
        setUpMaster(matchingMasterFiles[0]);

        // set up workers

        final File workerF = new File(workersDir);

        for (File f : workerF.listFiles()) {
            LOGGER.debug("______this.workerF.listFiles() " + f);
        }
        LOGGER.debug("______this.workerF.listFiles()" + workerF.listFiles());
        File[] workersTraces = workerF.listFiles((File dir, String name) -> name.endsWith(PYTHON_WORKER_TRACE_SUFFIX));
        for (File f : workersTraces) {
            LOGGER.debug("______filWorkerTraces " + f);
        }
        setUpWorkers(workersTraces);
        LOGGER.debug("Trace's merger initialization successful");
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
