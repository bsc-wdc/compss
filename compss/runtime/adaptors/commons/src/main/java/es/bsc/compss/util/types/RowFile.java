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
package es.bsc.compss.util.types;

import es.bsc.compss.log.Loggers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RowFile {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);


    private static enum RowBlock {
        CPUS, NODES, THREADS;
    }


    private static String cpuSizeIdentifier = "LEVEL CPU SIZE";
    private static String nodeSizeIdentifier = "LEVEL NODE SIZE";
    private static String threadSizeIdentifier = "LEVEL THREAD SIZE";

    private List<String>[] information = new List[3];


    /**
     * Creates a rowFile with the parsed data from the .row File.
     * 
     * @throws Exception Exception parsing the file
     */
    public RowFile(File rowFile) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(rowFile));
        String line = "";
        RowBlock reading = RowBlock.CPUS;
        Arrays.setAll(information, element -> new ArrayList<>());
        while ((line = br.readLine()) != null) {
            if (!line.isEmpty()) {
                if (line.startsWith(cpuSizeIdentifier)) {
                    reading = RowBlock.CPUS;
                } else if (line.startsWith(nodeSizeIdentifier)) {
                    reading = RowBlock.NODES;
                } else if (line.startsWith(threadSizeIdentifier)) {
                    reading = RowBlock.THREADS;
                } else if (line.startsWith(cpuSizeIdentifier)) {
                    reading = RowBlock.CPUS;
                } else {
                    information[reading.ordinal()].add(line);
                }
            }
        }
        br.close();
    }

    /**
     * Merges the information in another rowFile with this one.
     * 
     * @throws Exception Unknown format
     */
    public void mergeAgentRow(RowFile fileToMerge, int agentId) throws Exception {
        final List<String> cpus = this.information[RowBlock.CPUS.ordinal()];
        final List<String> threads = this.information[RowBlock.THREADS.ordinal()];
        final List<String> cpusToMerge = fileToMerge.information[RowBlock.CPUS.ordinal()];
        final List<String> threadsToMerge = fileToMerge.information[RowBlock.THREADS.ordinal()];

        int previousCpuNum = cpus.size();
        for (String cp : cpusToMerge) {
            // replace the index of the cpu in the other file with the next index of this file
            String newCp = Integer.toString(previousCpuNum) + cp.substring(cp.indexOf("."));
            cpus.add(newCp);
            previousCpuNum++;
        }

        for (String th : threadsToMerge) {
            String newTh;
            if (th.contains("(")) {
                int prefixIndex = th.indexOf("(") + 1;
                int sufixIndex = th.indexOf(")");
                String oldThId = th.substring(prefixIndex, sufixIndex);
                String[] oldIdValues = oldThId.split("\\.");
                // PRV_MACHINE_IDENTIFIER_POSITION-2 because it's not a line, it's only the threadId
                oldIdValues[PrvLine.STATE_MACHINE_POS - 2] = Integer.toString(agentId);
                String newThId = String.join(".", oldIdValues);
                newTh = th.substring(0, prefixIndex) + newThId + th.substring(sufixIndex, th.length());
            } else if (th.startsWith("THREAD ")) {
                String[] oldIdValues = th.split("\\.");
                // PRV_MACHINE_IDENTIFIER_POSITION-2 because it's not a line, it's only the threadId
                oldIdValues[PrvLine.STATE_MACHINE_POS - 2] = "THREAD " + Integer.toString(agentId);
                newTh = String.join(".", oldIdValues);
            } else {
                throw new Exception(
                    "Thread id in .row file with unknown format, don't know how to merge: " + th.toString());
            }
            threads.add(newTh);
        }
    }

    /**
     * Change de default .row labels for custom ones.
     */
    public void updateRowLabels(List<String> labelList) {
        List<String> newThreads = new ArrayList<String>();
        for (String label : labelList) {
            newThreads.add(label);
        }
        newThreads.sort(new RowThreadIdComparator());
        information[RowBlock.THREADS.ordinal()] = newThreads;
    }

    /**
     * Prints the formated information in this RowFile in the File outputFile.
     */
    public void printInfo(File outputFile) throws IOException {
        if (outputFile.exists()) {
            outputFile.delete();
            outputFile.createNewFile();
        }
        final PrintWriter rowWriter = new PrintWriter(new FileWriter(outputFile.getAbsolutePath(), true));
        final List<String> cpus = information[RowBlock.CPUS.ordinal()];
        final List<String> nodes = information[RowBlock.NODES.ordinal()];
        final List<String> threads = information[RowBlock.THREADS.ordinal()];

        rowWriter.println(cpuSizeIdentifier + " " + cpus.size());
        for (int i = 0; i < cpus.size(); i++) {
            rowWriter.println(cpus.get(i));
        }
        rowWriter.println();

        rowWriter.println(nodeSizeIdentifier + " " + nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            rowWriter.println(nodes.get(i));
        }
        rowWriter.println();

        rowWriter.println(threadSizeIdentifier + " " + threads.size());
        for (int i = 0; i < threads.size(); i++) {
            rowWriter.println(threads.get(i));
        }
        rowWriter.println();
        rowWriter.close();
    }
}
