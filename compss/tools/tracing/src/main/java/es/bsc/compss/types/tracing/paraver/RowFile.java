/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.types.tracing.ApplicationComposition;
import es.bsc.compss.types.tracing.ApplicationStructure;
import es.bsc.compss.types.tracing.InfrastructureElement;
import es.bsc.compss.types.tracing.Thread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;


public class RowFile {

    private static final String CPU_TITLE = "LEVEL CPU SIZE";
    private static final String NODE_TITLE = "LEVEL NODE SIZE";
    private static final String THREAD_TITLE = "LEVEL THREAD SIZE";


    public RowFile(String path) {
        // Not used
    }

    /**
     * Merges the information in another rowFile with this one.
     *
     * @throws Exception Unknown format
     */
    /*
     * public void mergeAgentRow(RowFile fileToMerge, int agentId) throws Exception { final List<String> cpus =
     * this.information[RowBlock.CPUS.ordinal()]; final List<String> threads =
     * this.information[RowBlock.THREADS.ordinal()]; final List<String> cpusToMerge =
     * fileToMerge.information[RowBlock.CPUS.ordinal()]; final List<String> threadsToMerge =
     * fileToMerge.information[RowBlock.THREADS.ordinal()];
     * 
     * int previousCpuNum = cpus.size(); for (String cp : cpusToMerge) { // replace the index of the cpu in the other
     * file with the next index of this file String newCp = Integer.toString(previousCpuNum) +
     * cp.substring(cp.indexOf(".")); cpus.add(newCp); previousCpuNum++; }
     * 
     * for (String th : threadsToMerge) { String newTh; if (th.contains("(")) { int prefixIndex = th.indexOf("(") + 1;
     * int sufixIndex = th.indexOf(")"); String oldThId = th.substring(prefixIndex, sufixIndex); String[] oldIdValues =
     * oldThId.split("\\."); // PRV_MACHINE_IDENTIFIER_POSITION-2 because it's not a line, it's only the threadId
     * oldIdValues[PRVLine.STATE_MACHINE_POS - 2] = Integer.toString(agentId); String newThId = String.join(".",
     * oldIdValues); newTh = th.substring(0, prefixIndex) + newThId + th.substring(sufixIndex, th.length()); } else if
     * (th.startsWith("THREAD ")) { String[] oldIdValues = th.split("\\."); // PRV_MACHINE_IDENTIFIER_POSITION-2 because
     * it's not a line, it's only the threadId oldIdValues[PRVLine.STATE_MACHINE_POS - 2] = "THREAD " +
     * Integer.toString(agentId); newTh = String.join(".", oldIdValues); } else { throw new Exception(
     * "Thread id in .row file with unknown format, don't know how to merge: " + th.toString()); } threads.add(newTh); }
     * }
     */
    /**
     * Updates the infrastructure and application information with the labels from a given row file.
     *
     * @param source path to the source row file
     * @param infrastructure description of the infrastructure of the trace
     * @param applications description of the application organization
     * @throws FileNotFoundException Could not find the source row file
     * @throws IOException Error reading from the file
     */
    public static void updateStructuresWithRowContent(String source, ArrayList<InfrastructureElement> infrastructure,
        ApplicationComposition applications) throws FileNotFoundException, IOException {
        List<String> nodes = new ArrayList<>();
        List<String> cpus = new ArrayList<>();
        List<String> threads = new ArrayList<>();

        File rowFile = new File(source);
        List<String> reading = null;
        try (BufferedReader br = new BufferedReader(new FileReader(rowFile))) {
            String line = "";
            while ((line = br.readLine()) != null) {
                if (!line.isEmpty()) {
                    if (line.startsWith(CPU_TITLE)) {
                        reading = cpus;
                    } else if (line.startsWith(NODE_TITLE)) {
                        reading = nodes;
                    } else if (line.startsWith(THREAD_TITLE)) {
                        reading = threads;
                    } else {
                        reading.add(line);
                    }
                }
            }
        }

        int globalCPUIdx = 0;
        for (int nodeIdx = 0; nodeIdx < infrastructure.size(); nodeIdx++) {
            InfrastructureElement node = infrastructure.get(nodeIdx);
            node.setLabel(nodes.get(nodeIdx));

            List<InfrastructureElement> nodeCpus = node.getSubComponents();
            for (int nodeCPUIdx = 0; nodeCPUIdx < nodeCpus.size(); nodeCPUIdx++) {
                nodeCpus.get(nodeCPUIdx).setLabel(cpus.get(globalCPUIdx));
                globalCPUIdx++;
            }
        }

        int globalthreadIdx = 0;
        for (ApplicationStructure systemElement : applications.getSubComponents()) {
            ApplicationComposition app = (ApplicationComposition) systemElement;
            for (ApplicationStructure appElement : app.getSubComponents()) {
                ApplicationComposition task = (ApplicationComposition) appElement;
                for (ApplicationStructure taskElement : task.getSubComponents()) {
                    Thread thread = (Thread) taskElement;
                    thread.setLabel(threads.get(globalthreadIdx++));
                }
            }
        }

    }

    /**
     * Generates a row file with the given trace framework.
     *
     * @param infrastructure description of the infrastructure of the trace
     * @param application description of the application organization
     * @param target path where to leave the row file
     * @throws IOException row file could not be created or there was an error writing it
     */
    public static void generateRowFile(ArrayList<InfrastructureElement> infrastructure,
        ApplicationComposition application, String target) throws IOException {
        File outputFile = new File(target);
        if (outputFile.exists()) {
            outputFile.delete();
            outputFile.createNewFile();
        }

        List<String> cpus = new LinkedList<>();
        List<String> nodes = new ArrayList<>(infrastructure.size());
        for (InfrastructureElement node : infrastructure) {
            nodes.add(node.getLabel());
            for (InfrastructureElement cpu : node.getSubComponents()) {
                cpus.add(cpu.getLabel());
            }
        }
        final PrintWriter rowWriter = new PrintWriter(new FileWriter(outputFile.getAbsolutePath(), false));
        rowWriter.println(CPU_TITLE + " " + cpus.size());
        for (int i = 0; i < cpus.size(); i++) {
            rowWriter.println(cpus.get(i));
        }
        rowWriter.println();

        rowWriter.println(NODE_TITLE + " " + nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            rowWriter.println(nodes.get(i));
        }
        rowWriter.println();

        List<String> threads = application.getAllLabels();
        List<String> newThreads = new ArrayList<>();
        for (String label : threads) {
            newThreads.add(label);
        }
        newThreads.sort(new RowThreadIdComparator());

        rowWriter.println(THREAD_TITLE + " " + newThreads.size());
        for (int i = 0; i < newThreads.size(); i++) {
            rowWriter.println(newThreads.get(i));
        }
        rowWriter.close();
    }


    private static class RowThreadIdComparator implements Comparator<String> {

        /**
         * Compares two threads Ids being in a string formatted like "label (X:X:X)" .
         */
        public int compare(String a, String b) {
            if (a.startsWith("THREAD ")) {
                a = a.substring(7, a.length());
            } else {
                a = a.substring(a.indexOf("(") + 1, a.indexOf(")"));
            }

            if (b.startsWith("THREAD ")) {
                b = b.substring(7, b.length());
            } else {
                b = b.substring(b.indexOf("(") + 1, b.indexOf(")"));
            }

            String[] valuesA = a.split("\\.");
            String[] valuesB = b.split("\\.");
            for (int i = 0; i < 3; i++) {
                int intA = Integer.parseInt(valuesA[i]);
                int intB = Integer.parseInt(valuesB[i]);
                if (intA > intB) {
                    return 1;
                }
                if (intA < intB) {
                    return -1;
                }
            }
            return 0;
        }
    }
}
