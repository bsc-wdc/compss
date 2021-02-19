
package es.bsc.compss.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


public class AgentTraceMerger extends TraceMerger {

    private static final String TRACE_SUBDIR = "trace";


    /**
     * Initilizes class atributes for agents trace merging .
     * 
     * @param appName Application name
     * @throws IOException Error managing files
     */
    public AgentTraceMerger(String outputDir, String[] agentsDirs, String appName) throws IOException {

        // Init workers traces information
        final String traceNamePrefix = (appName != null ? appName : "") + MASTER_TRACE_SUFFIX;
        List<File> workersFiles = new ArrayList<File>();
        for (String oneAgentDir : agentsDirs) {
            File traceDirFile = new File(oneAgentDir + File.separator + TRACE_SUBDIR);
            System.out.println("______buscando trazas en " + traceDirFile.getAbsolutePath());
            File[] oneAgentMatchingFiles = traceDirFile.listFiles(
                (File dir, String name) -> name.startsWith(traceNamePrefix) && name.endsWith(TRACE_EXTENSION));
            if (oneAgentMatchingFiles == null || oneAgentMatchingFiles.length < 1) {
                throw new FileNotFoundException("Master trace " + traceNamePrefix + "*" + TRACE_EXTENSION
                    + " not found at directory " + oneAgentDir);
            }
            for (File traceFile : oneAgentMatchingFiles) {
                workersFiles.add(traceFile);
            }
        }
        File[] workersTraces = workersFiles.toArray(new File[0]);
        setUpWorkers(workersTraces);

        // Init master trace information
        File outputFile = new File(outputDir + "/merged.prv");
        if (!outputFile.exists()) {
            createPRVFile(outputFile, workersTraces);
        }
        System.out.println("______Created prv merge file in " + outputFile.getAbsolutePath());
        setUpMaster(outputFile);

        System.out.println("Trace's merger initialization successful");
    }

    /**
     * Merges the traces .
     */
    public void merge() throws Exception {
        System.out.println("______iniciando merging desde AgentTraceMerger");
        createPRVswithGlobalCE();
        mergePRVs();
        createGlobalRow();
        System.out.println("Merging finished.");
        removeTemporalFiles();
    }

    private void createPRVFile(File prv, File[] workersTraces) throws IOException {
        prv.createNewFile();
        BufferedReader br = new BufferedReader(new FileReader(workersTraces[0]));
        PrintWriter writer = new PrintWriter(new FileWriter(prv.getAbsolutePath(), true));
        String header = br.readLine();
        System.out.println("______copiando " + header + " a " + prv.getAbsolutePath());
        writer.println(header);
        writer.close();
        br.close();
    }

    // List<File> matchingWorkerFiles = new ArrayList<File>();
    // for (String wd : workersDirs) {
    // File traceDirFile = new File(wd);
    // System.out.println("______buscando trazas en " + wd);
    // for (File traceFile : traceDirFile.listFiles((File dir, String name) -> name.endsWith(TRACE_EXTENSION))) {
    // matchingWorkerFiles.add(traceFile);
    // }
    // }
    // matchingWorkerFiles.toArray(new File[0]);

    // ---------------------------------------------------------------------------------
    // if (masterTrace != null && masterTrace.length > 0) {

    // if (masterTrace.length > 1) {
    // LOGGER.warn("Found more than one master trace, using " + this.masterTrace + " to merge.");
    // }
    // } else {
    // throw new FileNotFoundException("Master trace " + traceNamePrefix + "*" + TRACE_EXTENSION + " not found.");
    // }

    // // ______DEBUG
    // System.out.println("______masterDir " + masterDir);

    // for (File f : masterF.listFiles()) {
    // LOGGER.debug("______this.masterF.listFiles() " + f);
    // }

    // for (File f : matchingMasterFiles) {
    // LOGGER.debug("______matchingMasterFiles " + f);
    // }
    // System.out.println("______master file " + masterF.getAbsolutePath());
}