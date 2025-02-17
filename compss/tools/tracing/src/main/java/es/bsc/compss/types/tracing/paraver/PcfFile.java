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

import es.bsc.compss.types.tracing.EventsDefinition;
import es.bsc.compss.types.tracing.MalformedException;
import es.bsc.compss.types.tracing.TraceEventType;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


public class PcfFile implements EventsDefinition {

    private static final String EVENT_TYPE_HEADER = "EVENT_TYPE";
    private static final String VALUES_HEADER = "VALUES";
    private static final String ID_VALUE_SEPARATOR = "      ";

    // CE identifier in .pcf
    private static final String CE_TYPE_DEFINITION = "0    " + TraceEventType.TASKS_FUNC.code + "    Task";

    // Hardware counters pattern
    private static final String HW_TYPE_DEFINITION = "7  41999999 Active hardware counter set";
    private static final String HW_COUNTER_LINE_HEADER = "7  4200";

    private final String original;
    private boolean checkedOriginal = false;

    private boolean hasCEsDefined = false;
    private boolean changedCEs = false;
    private Map<String, String> originalCEs = new HashMap<>();
    private Map<String, String> newCEs = new HashMap<>();

    private boolean hasHWCountersDefined = false;
    private List<String> originalHWCounters = new ArrayList<>();
    private List<String> newHWCounters = new LinkedList<>();


    public PcfFile(String original) {
        this.original = new File(original).getAbsolutePath();
    }

    /**
     * Returns the path of the Pcf File.
     *
     * @return path of the file
     */
    public String getPath() {
        return this.original;
    }

    private void parse() throws IOException, MalformedException {
        List<String> lines = Files.readAllLines(Paths.get(this.original), StandardCharsets.UTF_8);

        Iterator<String> linesItr = lines.iterator();
        String line = null;
        boolean hasContent = false;
        if (linesItr.hasNext()) {
            line = linesItr.next();
            hasContent = true;
        }

        while (hasContent && line != null) {
            if (line.startsWith(EVENT_TYPE_HEADER)) {
                String header = linesItr.next();
                if (HW_TYPE_DEFINITION.compareTo(header) == 0) {
                    this.hasHWCountersDefined = true;
                    parseHWCounters(linesItr);
                } else {
                    if (CE_TYPE_DEFINITION.compareTo(header) == 0) {
                        this.hasCEsDefined = true;
                        parseCEs(linesItr);
                    }
                }
            }

            if (linesItr.hasNext()) {
                line = linesItr.next();
            } else {
                hasContent = false;
            }
        }

        this.checkedOriginal = true;
    }

    private void parseCEs(Iterator<String> linesItr) throws MalformedException {
        Pattern numberPattern = Pattern.compile("[0-9]+");
        boolean endedParsing = false;
        while (linesItr.hasNext() && !endedParsing) {
            String line = linesItr.next();
            if (line.isEmpty()) {
                endedParsing = true;
            } else {
                if (line.compareTo(VALUES_HEADER) == 0) {
                    // Ignore line
                } else {
                    String[] values = line.split(ID_VALUE_SEPARATOR);
                    if (values.length != 2 || values[1].isEmpty() || !numberPattern.matcher(values[0]).matches()) {
                        throw new MalformedException("ERROR: Malformed CE in PFC " + original + "  line: " + line);
                    }
                    this.originalCEs.put(values[0], values[1]);
                }
            }
        }

    }

    private void parseHWCounters(Iterator<String> linesItr) {
        boolean endedParsing = false;
        while (linesItr.hasNext() && !endedParsing) {
            String line = linesItr.next();
            if (line.isEmpty()) {
                endedParsing = true;
            } else {
                if (line.compareTo(VALUES_HEADER) == 0) {
                    // Ignore line
                } else {
                    if (line.startsWith(HW_COUNTER_LINE_HEADER)) {
                        originalHWCounters.add(line);
                    }
                }
            }
        }
    }

    @Override
    public Map<String, String> getCEsMapping() throws IOException, MalformedException {
        if (!checkedOriginal) {
            parse();
        }
        return this.originalCEs;
    }

    @Override
    public List<String> getHWCounters() throws IOException, MalformedException {
        if (!checkedOriginal) {
            parse();
        }
        return originalHWCounters;
    }

    /**
     * Creates an empty file on the original location.
     * 
     * @throws IOException Could not create the file
     */
    public void create() throws IOException {
        File f = new File(original);
        if (!f.exists()) {
            f.createNewFile();
        }
    }

    /**
     * Redefines the CE in the Pcf.
     *
     * @param cores new Core elements to be in the Pcf
     */
    public void redefineCEs(Map<String, String> cores) {
        this.newCEs = cores;
    }

    @Override
    public void defineNewHWCounters(Collection<String> newHWCounters) {
        for (String newHWCounter : newHWCounters) {
            for (String originalHWCounter : originalHWCounters) {
                if (originalHWCounter.compareTo(newHWCounter) != 0) {
                    newHWCounters.add(newHWCounter);
                }
            }
        }
    }

    /**
     * Saves the content of the updated Pcf file in the new location.
     * 
     * @param location location where to safe the new Pcf file
     * @throws IOException error when writing the file
     */
    public void saveToFile(String location) throws IOException {
        boolean appendedHWCounters = false;
        List<String> lines = Files.readAllLines(Paths.get(this.original), StandardCharsets.UTF_8);
        String targetPath = (new File(location)).getAbsolutePath();
        try (PrintWriter pcfWriter = new PrintWriter(new FileWriter(targetPath, false))) {
            Iterator<String> linesItr = lines.iterator();
            while (linesItr.hasNext()) {
                String line = linesItr.next();
                if (line.startsWith(EVENT_TYPE_HEADER)) {
                    pcfWriter.println(line);
                    String header = linesItr.next();
                    pcfWriter.println(header);
                    if (HW_TYPE_DEFINITION.compareTo(header) == 0) {
                        copyEventValues(linesItr, pcfWriter);
                        appendNewHWCounters(pcfWriter);
                        appendedHWCounters = true;
                        pcfWriter.println();
                    } else if (CE_TYPE_DEFINITION.compareTo(header) == 0) {
                        if (!this.newCEs.isEmpty()) {
                            skipEventValues(linesItr);
                            pcfWriter.println(VALUES_HEADER);
                            appendCEs(pcfWriter);
                            pcfWriter.println();
                        }
                    }
                } else {
                    pcfWriter.println(line);
                }
            }

            if (!this.newHWCounters.isEmpty() && !appendedHWCounters) {
                if (!this.hasHWCountersDefined) {
                    // The master did not contain hardware counter labels: requires fixed
                    pcfWriter.println(EVENT_TYPE_HEADER);
                    pcfWriter.println(HW_TYPE_DEFINITION);
                    pcfWriter.println("VALUES");
                }
                appendNewHWCounters(pcfWriter);
            }
        }
        this.checkedOriginal = false;
    }

    private void skipEventValues(Iterator<String> linesItr) {
        boolean endedParsing = false;
        while (linesItr.hasNext() && !endedParsing) {
            String line = linesItr.next();
            if (line.isEmpty()) {
                endedParsing = true;
            }
        }
    }

    private void copyEventValues(Iterator<String> linesItr, PrintWriter pw) {
        while (linesItr.hasNext()) {
            String line = linesItr.next();
            if (line.isEmpty()) {
                return;
            }
            pw.println(line);
        }
    }

    private void appendCEs(PrintWriter writer) {
        for (Map.Entry<String, String> ce : newCEs.entrySet()) {
            writer.println(ce.getValue() + ID_VALUE_SEPARATOR + ce.getKey());
        }
    }

    private void appendNewHWCounters(PrintWriter writer) {
        for (String line : newHWCounters) {
            writer.println(line);
        }
    }

    public static void generatePCFFile(EventsDefinition definitions, String pcfFilePath) throws IOException {
        ((PcfFile) definitions).saveToFile(pcfFilePath);
    }

}
