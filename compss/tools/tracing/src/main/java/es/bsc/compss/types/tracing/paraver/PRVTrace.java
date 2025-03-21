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
package es.bsc.compss.types.tracing.paraver;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.tracing.ApplicationComposition;
import es.bsc.compss.types.tracing.EventsDefinition;
import es.bsc.compss.types.tracing.MalformedException;
import es.bsc.compss.types.tracing.SynchEvent;
import es.bsc.compss.types.tracing.SystemComposition;
import es.bsc.compss.types.tracing.Trace;
import es.bsc.compss.util.tracing.TraceTransformation;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PRVTrace implements Trace {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    // private final String path;
    private final String directory;
    private final String name;
    private final String path;

    private final String date;
    private final String duration;

    private final ApplicationComposition threadOrganization;
    private final SystemComposition infrastructure;
    private final EventsDefinition definitions;


    /**
     * Constructs a new Trace file.
     *
     * @param directory directory containing the trace file
     * @param name name of the trace
     */
    public PRVTrace(String directory, String name) {
        String dirPath = new java.io.File(directory).getAbsolutePath();
        dirPath = dirPath + (dirPath.endsWith(File.separator) ? "" : File.separator);
        this.directory = dirPath;
        this.name = name;
        this.path = dirPath + name;
        this.definitions = new PcfFile(path + ".pcf");

        PRVHeader header = getHeader();
        this.date = header.getDate();
        this.duration = header.getDuration();
        this.infrastructure = header.getInfrastructure();
        this.threadOrganization = header.getThreadOrganization();
        try {
            RowFile.updateStructuresWithRowContent(this.getRowFilePath(), infrastructure, threadOrganization);
        } catch (Exception e) {
            // No row file. Labels are not updated
        }
    }

    /**
     * Constructs a new Trace file.
     *
     * @param trace Any of the files containing the trace
     */
    public PRVTrace(java.io.File trace) {
        this(parseDirectory(trace), parseTraceName(trace));
    }

    private static String parseTraceName(File trace) {
        String fileName = trace.getName();
        if (fileName.endsWith(".prv") || fileName.endsWith(".pcf") || fileName.endsWith(".row")) {
            return fileName.substring(0, fileName.length() - 4);
        }
        return fileName;
    }

    private static String parseDirectory(File trace) {
        String dirPath = trace.getParent();
        return dirPath + (dirPath.endsWith(File.separator) ? "" : File.separator);
    }

    private PRVTrace(String directory, String name, String date, String duration, SystemComposition infrastructure,
        ApplicationComposition threads, EventsDefinition definitions) {
        this.directory = directory + (directory.endsWith(File.separator) ? "" : File.separator);
        this.name = name;
        this.path = this.directory + name;
        this.date = date;
        this.duration = duration;
        this.infrastructure = infrastructure;
        this.threadOrganization = threads;
        this.definitions = definitions;
    }

    /**
     * Constructs and stores a new trace structure with no records.
     * 
     * @param directory directory containing the trace file
     * @param name name of the trace
     * @param date date of the execution
     * @param duration length of the execution
     * @param infrastructure description of the infrastructure hosting the execution
     * @param threads execution thread structure
     * @param definitions events' definition
     * @return trace representing the files
     * @throws IOException error creating one of the files
     */
    public static PRVTrace generateNew(String directory, String name, String date, String duration,
        SystemComposition infrastructure, ApplicationComposition threads, EventsDefinition definitions)
        throws IOException {
        PRVTrace trace = new PRVTrace(directory, name, date, duration, infrastructure, threads, definitions);

        String prvPath = trace.getPrvFilePath();

        File prvFile = new File(prvPath);
        boolean fileCreated = prvFile.createNewFile();
        if (!fileCreated) {
            throw new IOException("ERROR: File " + prvFile.getAbsolutePath() + " already existed");
        }
        String header = PRVHeader.generateTraceHeader(date, duration, infrastructure, threads);
        try (PrintWriter writer = new PrintWriter(new FileWriter(prvFile, false))) {
            writer.println(header);
        }
        RowFile.generateRowFile(trace.infrastructure, trace.threadOrganization, trace.getRowFilePath());
        PcfFile.generatePCFFile(definitions, trace.getPcfFilePath());

        return trace;
    }

    @Override
    public String getDirectory() {
        return directory;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns the header of the PRV file.
     *
     * @return header of the PRV file
     */
    private PRVHeader getHeader() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(getPrvFilePath()));
            String header = br.readLine();
            if (header != null) {
                return new PRVHeader(header);
            }
        } catch (Exception e) {
            // Do nothing
        }
        return null;
    }

    @Override
    public String getDate() {
        return date;
    }

    @Override
    public String getDuration() {
        return duration;
    }

    @Override
    public long getDurationInNS() {
        String[] parts = duration.split("_");
        long value = Long.parseLong(parts[0]);
        long scale;
        switch (parts[1]) {
            case "ns":
                scale = 1;
                break;
            case "us":
                scale = 1_000;
                break;
            case "ms":
                scale = 1_000_000;
                break;
            default:
                scale = 1;
        }
        return value * scale;
    }

    private String getPrvFilePath() {
        return this.path + ".prv";
    }

    private String getRowFilePath() {
        return this.path + ".row";
    }

    private String getPcfFilePath() {
        return this.path + ".pcf";
    }

    @Override
    public SystemComposition getInfrastructure() {
        return infrastructure;
    }

    @Override
    public ApplicationComposition getThreadOrganization() {
        return threadOrganization;
    }

    @Override
    public EventsDefinition getEventsDefinition() {
        return definitions;
    }

    @Override
    public Map<Integer, List<SynchEvent>> getSyncEvents(Integer workerID) throws IOException {
        String prvFile = this.getPrvFilePath();
        if (DEBUG) {
            LOGGER.debug("Getting sync events from: " + prvFile + " for worker " + workerID);
        }
        Map<Integer, List<SynchEvent>> idToSyncInfo = new HashMap<>();
        try (FileInputStream inputStream = new FileInputStream(prvFile);
            Scanner sc = new Scanner(inputStream, "UTF-8")) {

            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                SynchEvent event = PRVLine.parseSynchEvent(line);
                if (event != null) {
                    Integer wID = (workerID == -1) ? Integer.parseInt(event.getWorkerId()) : workerID;
                    List<SynchEvent> currentValue = idToSyncInfo.get(wID);
                    if (currentValue == null) {
                        currentValue = new ArrayList<>();
                        idToSyncInfo.put(wID, currentValue);
                    }
                    currentValue.add(event);
                }
            }
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        }
        // Exceptions are raised automatically, we add the try clause to automatically
        // close the streams

        return idToSyncInfo;
    }

    @Override
    public RecordScanner getRecords() throws FileNotFoundException {
        return new PRVRecordScanner(getPrvFilePath());
    }

    @Override
    public RecordAppender getRecordAppender() throws IOException {
        return new PRVRecordAppender(this.getPrvFilePath());
    }

    /**
     * Applies a set of transformation to the whole trace.
     *
     * @param transformations transformations to apply to the trace
     * @throws FileNotFoundException Some of the trace files could not be found
     * @throws IOException Could not create a temporary file nor read from the original file traces.
     * @throws MalformedException A line within the files has an unexpected format
     */
    public void applyTransformations(TraceTransformation... transformations)
        throws FileNotFoundException, MalformedException, IOException {
        // Transform structure
        for (TraceTransformation transformation : transformations) {
            transformation.apply(this.definitions);
            transformation.apply(infrastructure, threadOrganization);
        }
        // Create trace framework
        String originalPrvPath = this.getPrvFilePath();
        String tmpPrvPath = originalPrvPath + ".tmp";
        File tmpPrv = new File(tmpPrvPath);
        boolean fileCreated = tmpPrv.createNewFile();
        if (!fileCreated) {
            throw new IOException("ERROR: File " + tmpPrv.getAbsolutePath() + " already existed");
        }
        String header = PRVHeader.generateTraceHeader(this.date, this.duration, infrastructure, threadOrganization);
        try (PrintWriter writer = new PrintWriter(new FileWriter(tmpPrv, false))) {
            writer.println(header);
            RowFile.generateRowFile(infrastructure, threadOrganization, this.getRowFilePath());
            PcfFile.generatePCFFile(definitions, this.getPcfFilePath());

            // Register events
            try (RecordScanner records = this.getRecords()) {
                String record;
                // the isEmpty check should not be necessary if the .prv files are well constructed
                while ((record = records.next()) != null && !record.isEmpty()) {
                    PRVLine prvLine = PRVLine.parse(record);
                    for (TraceTransformation transformation : transformations) {
                        transformation.apply(prvLine);
                    }
                    writer.println(prvLine.toString());
                }
            }
        }
        tmpPrv.renameTo(new File(originalPrvPath));
    }

    /**
     * Returns whether the trace file already exist or not.
     * 
     * @return {@literal true} if the files exist; {@literal false} otherwise
     */
    public boolean exists() {
        return new File(this.getPrvFilePath()).exists() && new File(this.getPcfFilePath()).exists()
            && new File(this.getRowFilePath()).exists();
    }

    /**
     * Moves the files of the trace into a new location.
     *
     * @param directory location where to leave the trace
     * @param name new name of the trace
     */
    public void renameAs(String directory, String name) {
        String dirPath = new java.io.File(directory).getAbsolutePath();
        dirPath = dirPath + (dirPath.endsWith(File.separator) ? "" : File.separator);
        String path = dirPath + name;

        renameFile(this.getPrvFilePath(), path + ".prv");
        renameFile(this.getPcfFilePath(), path + ".pcf");
        renameFile(this.getRowFilePath(), path + ".row");
    }

    private static void renameFile(String original, String target) {
        File targetF = new File(target);
        if (targetF.exists()) {
            targetF.delete();
        }
        File originalF = new File(original);
        if (originalF.exists()) {
            originalF.renameTo(targetF);
        }
    }


    public static class PRVRecordScanner implements RecordScanner {

        private final BufferedReader br;


        private PRVRecordScanner(String prv) throws FileNotFoundException {
            this.br = new BufferedReader(new FileReader(prv));
            try {
                br.readLine(); // skip Header
            } catch (IOException e) {
                // Is empty and has no records. Continue...
            }

        }

        public String next() throws IOException {
            return br.readLine();
        }

        @Override
        public void close() throws IOException {
            br.close();
        }
    }

    public static class PRVRecordAppender implements RecordAppender {

        private final PrintWriter pw;


        private PRVRecordAppender(String path) throws IOException {
            this.pw = new PrintWriter(new FileWriter(path, true));
        }

        public void append(String event) throws IOException {
            pw.println(event);
        }

        @Override
        public void close() throws IOException {
            pw.close();
        }
    }
}
