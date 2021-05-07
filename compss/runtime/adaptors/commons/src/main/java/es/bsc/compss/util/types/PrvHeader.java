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
import es.bsc.compss.util.Tracer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PrvHeader {

    // Splits the string on ":" not contained inside parenthesis
    private static final String SPLIT_HEADER_REGEX = ":\\s*(?![^()]*\\))";

    // Paraver trace header follows the following format
    // #Paraver (dd/mm/yy at hh:mm):traceTime:nNodes(nCpus1,nCpus2n...nCpusN):nAppl:applicationList
    // where applicationList follows the format
    // nTasks(nThreads1:node,...,...nThreadsN:node)

    private String singAndDate;
    private String duration;
    // nNodes = cpusPerNode.size();
    private List<String> cpusPerNode;
    // nAppl = applicationList.size()
    private List<String> applicationList;

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);


    /**
     * Creates a PrvHeader with the parsed data from the string.
     * 
     * @throws Exception Exception parsing the header
     */
    public PrvHeader(String header) throws Exception {
        // Is not known why the traces end with ",0" shouldn't be that way according to Paraver documentation
        if (header.endsWith(",0")) {
            header = header.substring(0, header.length() - 2);
        }
        final String[] headerParts = header.split(SPLIT_HEADER_REGEX);

        if (headerParts.length < 4) {
            throw new Exception("prv header doesn't have the expected format(wrong number of parts): " + header);
        }

        this.singAndDate = headerParts[0];
        this.duration = headerParts[1];
        this.cpusPerNode = Arrays.asList(getInsideParenthresis(headerParts[2]).split(","));
        this.applicationList = new ArrayList<String>();
        for (int i = 4; i < headerParts.length; i++) {
            this.applicationList.add(headerParts[i]);
        }
    }

    /**
     * Used to transform a trace with each worker as a node to a trace with each worker as a machine (aplication).
     * 
     * @throws Exception Exception parsing the header
     */
    public void transformNodesToAplications() throws Exception {
        if (applicationList.size() != 1) {
            throw new Exception(
                "prv header doesn't have the expected format (expected only one application): " + this.toString());
        }
        String[] appListValues = getInsideParenthresis(this.applicationList.get(0)).split(",");
        ArrayList<String> newAppList = new ArrayList<String>();
        ArrayList<String> newcpuNodesList = new ArrayList<String>();
        for (String appValue : appListValues) {
            newcpuNodesList.add(appValue.substring(0, appValue.indexOf(":")));
            newAppList.add("1(" + appValue + ")");
        }
        this.applicationList = newAppList;
        this.cpusPerNode = newcpuNodesList;
    }

    private String getInsideParenthresis(String block) {
        Matcher numThreadMatcher = Tracer.INSIDE_PARENTHESIS_PATTERN.matcher(block);
        numThreadMatcher.find();
        String inside = numThreadMatcher.group(0);
        inside = inside.substring(1, inside.length() - 1);
        return inside;
    }

    /**
     * Adds to this header the threads and threadRanges from the header headerToMerge.
     */
    public void addAsAplication(PrvHeader headerToMerge) {
        this.cpusPerNode = new ArrayList<String>(this.cpusPerNode);
        this.cpusPerNode.addAll(headerToMerge.cpusPerNode);
        this.applicationList = new ArrayList<String>(this.applicationList);
        this.applicationList.addAll(headerToMerge.applicationList);
    }

    /**
     * Returns an aplication for the header based on oldApp but with the threads clasified in runtime and executors.
     */
    private String splitRuntimeExecutorsApplication(int numberOfRuntimeThreads, String oldApp) {
        int oldNumThreads = Integer.parseInt(getInsideParenthresis(oldApp).split(":")[0]);
        String runtimeThreads = Integer.toString(numberOfRuntimeThreads);
        int nonRuntimeThreads = oldNumThreads - numberOfRuntimeThreads;
        if (nonRuntimeThreads > 0) {
            return "2(" + runtimeThreads + ":1," + Integer.toString(nonRuntimeThreads) + ":2)";
        }
        return "1(" + runtimeThreads + ":1)";
    }

    /**
     * Classifies the threads of the apps on the header in runtime and executors.
     * 
     * @throws Exception unexpected header range
     */
    public void splitRuntimeExecutors(int[] numRuntimeThreadsPerApp) throws Exception {
        for (int i = 0; i < this.applicationList.size(); i++) {
            String newApp = splitRuntimeExecutorsApplication(numRuntimeThreadsPerApp[i], this.applicationList.get(i));
            this.applicationList.set(i, newApp);
        }
    }

    @Override
    public String toString() {
        final String nodesString = cpusPerNode.size() + "(" + String.join(",", cpusPerNode) + ")";
        final String applicationsString = applicationList.size() + ":" + String.join(":", applicationList);
        return singAndDate + ":" + duration + ":" + nodesString + ":" + applicationsString;

    }
}
