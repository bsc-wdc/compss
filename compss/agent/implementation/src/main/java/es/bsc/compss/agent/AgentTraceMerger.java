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

    private static String getExecutionName(String path, String appName) {
        File traceDir = new File(path + File.separator + Tracer.TRACE_SUBDIR);
        String[] prvFiles = traceDir.list((File dir, String name) -> name.contains(Tracer.MASTER_TRACE_SUFFIX)
            && name.endsWith(Tracer.TRACE_PRV_FILE_EXTENTION));
        String timeStamp =
            prvFiles[0].replace(Tracer.MASTER_TRACE_SUFFIX, "").replace(Tracer.TRACE_PRV_FILE_EXTENTION, "");
        return (appName != null && !appName.isEmpty() ? appName + "_" : "") + timeStamp;

    }

    /**
     * Initializes class attributes for agents trace merging .
     *
     * @param appName Application name
     * @throws IOException Error managing files
     */
    public AgentTraceMerger(String outputDir, String[] agentsDirs, String appName) throws IOException {
        LOGGER.debug("Initializing AgentTraceMerger");
        final String traceNamePrefix = Tracer.MASTER_TRACE_SUFFIX;
        List<File> workersFiles = new ArrayList<>();
        LOGGER.debug("Searching for trace files");
        for (String oneAgentDir : agentsDirs) {
            File traceDirFile = new File(oneAgentDir + File.separator + Tracer.TRACE_SUBDIR);
            File tracesToMergeDirFile = new File(oneAgentDir + File.separator + Tracer.TO_MERGE_SUBDIR);
            tracesToMergeDirFile.mkdirs();
            File[] allFilesInFolder = traceDirFile.listFiles();
            for (int i = 0; i < allFilesInFolder.length; i++) {
                String newFileName =
                    tracesToMergeDirFile.getAbsolutePath() + File.separator + allFilesInFolder[i].getName();
                File newFile = new File(newFileName);
                File oldFile = allFilesInFolder[i];
                newFile.createNewFile();
                TraceMerger.copyFile(oldFile, newFile);
                allFilesInFolder[i] = newFile;
            }
            File[] oneAgentMatchingFiles = tracesToMergeDirFile.listFiles((File dir,
                String name) -> name.startsWith(traceNamePrefix) && name.endsWith(Tracer.TRACE_PRV_FILE_EXTENTION));
            if (oneAgentMatchingFiles == null || oneAgentMatchingFiles.length < 1) {
                throw new FileNotFoundException("No traces matching " + traceNamePrefix + "*"
                    + Tracer.TRACE_PRV_FILE_EXTENTION + " were found at directory " + oneAgentDir);
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
        final File outputFile =
            new File(outputDir + File.separator + (appName != null && appName.isEmpty() ? "trace" : appName) + ".prv");
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
        LOGGER.debug("Starting merge process");
        createPRVswithGlobalCE();
        mergePRVsIntoNewFile();
        mergeROWsIntoNewFile();
        System.out.println("Merging finished.");
    }

    /**
     * Merges the traces generated in the traceDirs directories and leaving the resulting trace in outputDir.
     * 
     * @param args String[] containing {appName, outputFolder, jobId and a list of folder paths containing the trace
     *            files to merge}
     * @throws IOException error managing files
     */
    public static void main(String[] args) throws IOException {
        final String appName = args[0];
        final String outputDir = args[1];
        final String jobId = args[2];
        final String[] traceDirs = Arrays.copyOfRange(args, 3, args.length);
        final String outputSubDir;
        if (jobId != null && !jobId.isEmpty()) {
            outputSubDir =
                outputDir + File.separator + (appName != null && !appName.isEmpty() ? appName + "_" : "") + jobId;
        } else {
            outputSubDir = outputDir + File.separator + getExecutionName(traceDirs[0], appName);
        }
        loggerSetUp(outputSubDir);

        System.out.println("----------------------------------------");
        System.out.println("Initiating agent trace merging");
        System.out.println("----------------------------------------");
        System.out.println("AppName: " + appName);
        System.out.println("OutputDir: " + outputDir);
        System.out.println("Merging traces in the following folders:");
        for (String path : traceDirs) {
            System.out.println("    " + path);
        }

        LOGGER.debug("----------------------------------------");
        LOGGER.debug("Initiating agent trace merging");
        LOGGER.debug("----------------------------------------");
        LOGGER.debug("AppName: " + appName);
        LOGGER.debug("OutputDir: " + outputDir);
        LOGGER.debug("Merging traces in the following folders: " + args.length);
        for (String path : traceDirs) {
            LOGGER.debug("    " + path);
        }

        try {
            AgentTraceMerger tm = new AgentTraceMerger(outputSubDir, traceDirs, appName);
            tm.merge();
        } catch (Throwable t) {
            System.err.println("Failed to correctly merge traces: exception risen");
            LOGGER.error("Failed to correctly merge traces: exception risen", t);
        }
    }
}
