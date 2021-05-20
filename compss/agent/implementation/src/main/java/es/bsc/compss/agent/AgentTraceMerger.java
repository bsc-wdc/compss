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

package es.bsc.compss.agent;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.TraceMerger;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;


public class AgentTraceMerger extends TraceMerger {

    private static Logger LOGGER;


    private static void loggerSetUp(String path) throws IOException {

        File logFile = new File(path);
        if (!logFile.exists()) {
            logFile.mkdirs();
        }
        System.setProperty(COMPSsConstants.APP_LOG_DIR, logFile.getAbsolutePath());
        ((LoggerContext) LogManager.getContext(false)).reconfigure();
        LOGGER = LogManager.getLogger(Loggers.COMM);
    }

    /**
     * Initializes class attributes for agents trace merging .
     *
     * @param traceName Name of the resulting trace
     * @param outputDir Directory where to store the resulting trace
     * @param agentsDirs Directories that contain a trace folder to merge
     * @throws IOException Error managing files
     */
    public AgentTraceMerger(String outputDir, String[] agentsDirs, String traceName) throws IOException {
        LOGGER.debug("Initializing AgentTraceMerger");
        List<File> workersFiles = new ArrayList<>();
        LOGGER.debug("Searching for trace files");
        for (String oneAgentDir : agentsDirs) {
            File traceDirFile = new File(oneAgentDir + File.separator + Tracer.TRACE_SUBDIR);
            File tracesToMergeDirFile = new File(oneAgentDir + File.separator + Tracer.TO_MERGE_SUBDIR);
            tracesToMergeDirFile.mkdirs();
            File[] allFilesInFolder = traceDirFile.listFiles();
            if (allFilesInFolder == null) {
                throw new FileNotFoundException("Folder not found: " + oneAgentDir);
            }
            for (int i = 0; i < allFilesInFolder.length; i++) {
                String newFileName =
                    tracesToMergeDirFile.getAbsolutePath() + File.separator + allFilesInFolder[i].getName();
                File newFile = new File(newFileName);
                File oldFile = allFilesInFolder[i];
                newFile.createNewFile();
                TraceMerger.copyFile(oldFile, newFile);
                allFilesInFolder[i] = newFile;
            }
            File[] oneAgentMatchingFiles = tracesToMergeDirFile
                .listFiles((File dir, String name) -> name.endsWith(Tracer.TRACE_PRV_FILE_EXTENTION));
            if (oneAgentMatchingFiles == null || oneAgentMatchingFiles.length < 1) {
                throw new FileNotFoundException("No traces matching " + " were found at directory " + oneAgentDir);
            }
            workersFiles.addAll(Arrays.asList(oneAgentMatchingFiles));
        }
        File[] workersTraces = workersFiles.toArray(new File[0]);
        LOGGER.debug("The following traces will be merged:");
        for (File trace : workersTraces) {
            LOGGER.debug("    " + trace.getAbsolutePath());
        }
        setUpWorkers(workersTraces);

        // Init master trace information
        final File outputFile = new File(outputDir + File.separator + traceName + ".prv");
        LOGGER.debug("Merge result will be stored at " + outputFile.getAbsolutePath());
        if (!outputFile.exists()) {
            Files.createDirectories(Paths.get(outputDir));
            outputFile.createNewFile();
        }
        setUpMaster(outputFile);

        LOGGER.debug("AgentTraceMerger initialization successful");
    }

    /**
     * Merges the traces .
     */
    public void merge() throws Exception {
        LOGGER.debug("Starting merge process.");
        System.out.println("Starting merge process.");
        createPRVswithGlobalCE();
        mergePRVsIntoNewFile();
        mergeROWsIntoNewFile();
        LOGGER.debug("Merge finished.");
        System.out.println("Merge finished.");
        removeTmpAgentFiles();
    }

    /**
     * Merges the traces generated in the traceDirs directories and leaving the resulting trace in outputDir.
     * 
     * @param args String[] containing {appName, outputFolder, jobId and a list of folder paths containing the trace
     *            files to merge}
     * @throws IOException error managing files
     */
    public static void main(String[] args) throws IOException {
        final String resTraceName = args[0];
        final String outputDir = args[1];
        final String[] traceDirs = Arrays.copyOfRange(args, 2, args.length);
        loggerSetUp(outputDir);

        System.out.println("----------------------------------------");
        System.out.println("Initiating agent trace merging");
        System.out.println("----------------------------------------");
        System.out.println("Result trace name: " + resTraceName);
        System.out.println("OutputDir: " + outputDir);
        System.out.println("Merging traces in the following folders:");
        for (String path : traceDirs) {
            System.out.println("    " + path);
        }

        LOGGER.debug("----------------------------------------");
        LOGGER.debug("Initiating agent trace merging");
        LOGGER.debug("----------------------------------------");
        LOGGER.debug("Result trace name: " + resTraceName);
        LOGGER.debug("OutputDir: " + outputDir);
        LOGGER.debug("Merging traces in the following folders:");
        for (String path : traceDirs) {
            LOGGER.debug("    " + path);
        }

        try {
            AgentTraceMerger tm = new AgentTraceMerger(outputDir, traceDirs, resTraceName);
            tm.merge();
        } catch (Throwable t) {
            System.err.println("Failed to correctly merge traces: exception risen");
            LOGGER.error("Failed to correctly merge traces: exception risen", t);
        }
    }
}
