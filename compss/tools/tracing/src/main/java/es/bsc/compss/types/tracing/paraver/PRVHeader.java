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
import es.bsc.compss.types.tracing.ApplicationStructure;
import es.bsc.compss.types.tracing.CPU;
import es.bsc.compss.types.tracing.MalformedException;
import es.bsc.compss.types.tracing.SystemComposition;
import es.bsc.compss.types.tracing.SystemStructure;
import es.bsc.compss.types.tracing.Thread;
import es.bsc.compss.types.tracing.ThreadIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PRVHeader {

    private static final String MALFORMED_HEADER = "Prv header doesn't have the expected format";
    private static final String MALFORMED_HEADER_PARTS = MALFORMED_HEADER + " (wrong number of parts)";

    // Splits the string on ":" not contained inside parenthesis
    private static final String SPLIT_HEADER_REGEX = ":\\s*(?![^()]*\\))";
    public static final Pattern INSIDE_PARENTHESIS_PATTERN = Pattern.compile("\\(.+\\)");

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);

    // Paraver trace header follows the following format
    // #Paraver (dd/mm/yy at hh:mm):traceTime:nNodes(nCpus1,nCpus2n...nCpusN):nAppl:applicationList
    // where applicationList follows the format
    // nTasks(nThreads1:node,...,...nThreadsN:node)
    private String date;
    private String duration;

    private SystemComposition<PRVNode> infrastructureOrganization;
    private ApplicationComposition<PRVApplication> threadOrganization;


    /**
     * Creates a PrvHeader with the parsed data from the string.
     *
     * @param header line containing the PRV header
     * @throws MalformedException Exception parsing the header
     */
    public PRVHeader(String header) throws MalformedException {
        // Is not known why the traces end with ",0" shouldn't be that way according to Paraver documentation
        if (header.endsWith(",0")) {
            header = header.substring(0, header.length() - 2);
        }
        final String[] headerParts = header.split(SPLIT_HEADER_REGEX);

        if (headerParts.length < 4) {
            throw new MalformedException(MALFORMED_HEADER_PARTS + ": " + header);
        }
        this.date = getInsideParenthesis(headerParts[0]);
        this.duration = headerParts[1];

        String[] cpusPerNode = getInsideParenthesis(headerParts[2]).split(",");
        int numNodes = cpusPerNode.length;
        this.infrastructureOrganization = new SystemComposition<>("");
        for (int nodeIdx = 0; nodeIdx < numNodes; nodeIdx++) {
            PRVNode node = new PRVNode(nodeIdx, "NODE" + nodeIdx);
            infrastructureOrganization.appendComponent(node);
            for (int cpuIdx = 0; cpuIdx < Integer.parseInt(cpusPerNode[nodeIdx]); cpuIdx++) {
                CPU cpu = new CPU("cpu" + nodeIdx + "." + cpuIdx);
                node.appendComponent(cpu);
            }
        }

        this.threadOrganization = new ApplicationComposition();
        String systemDescription = headerParts[4];
        Matcher m = Pattern.compile("\\((.*?)\\)").matcher(systemDescription);
        int appId = 1;
        while (m.find()) {
            PRVApplication app = new PRVApplication();
            String appDescription = m.group(1);
            String[] taskDescriptions = appDescription.split(",");
            int taskId = 1;
            for (String taskDescription : taskDescriptions) {
                PRVTask task = new PRVTask();
                String[] threads = taskDescription.split(":");
                int numThreads = Integer.parseInt(threads[0]);
                int numNode = Integer.parseInt(threads[1]);
                for (int threadId = 0; threadId < numThreads; threadId++) {
                    ThreadIdentifier threadIdentifier = new PRVThreadIdentifier(appId, taskId, threadId + 1);
                    Thread thread = new Thread(threadIdentifier, "");
                    task.appendComponent(thread);
                }
                PRVNode node = infrastructureOrganization.getSubComponents().get(numNode - 1);
                task.setNode(node);
                app.appendComponent(task);
                taskId++;
            }
            threadOrganization.appendComponent(app);
            appId++;
        }
    }

    /**
     * Timestamp of the trace.
     *
     * @return trace timestamp
     */
    public String getDate() {
        return this.date;
    }

    /**
     * Returns the length of the trace.
     *
     * @return duration of the trace
     */
    public String getDuration() {
        return this.duration;
    }

    /**
     * Returns number of cpus per node.
     *
     * @return number of cpus per node
     */
    public SystemComposition getInfrastructure() {
        return this.infrastructureOrganization;
    }

    /**
     * Returns the thread organization of the trace.
     *
     * @return thread organization
     */
    public ApplicationComposition getThreadOrganization() {
        return this.threadOrganization;
    }

    private String getInsideParenthesis(String block) {
        Matcher numThreadMatcher = INSIDE_PARENTHESIS_PATTERN.matcher(block);
        numThreadMatcher.find();
        String inside = numThreadMatcher.group(0);
        inside = inside.substring(1, inside.length() - 1);
        return inside;
    }

    @Override
    public String toString() {
        return generateTraceHeader(this.date, this.duration, this.infrastructureOrganization, this.threadOrganization);
    }

    /**
     * Updates the structure of the trace.
     *
     * @param cpusPerNode number of cpus for each node
     * @param system System description
     */
    public void setStructure(SystemComposition cpusPerNode, ApplicationComposition system) {
        this.infrastructureOrganization = cpusPerNode;
        this.threadOrganization = system;
    }

    /**
     * Generates the header to create a trace with that information.
     *
     * @param date time when the trace was taken
     * @param duration length of the trace
     * @param infrastructureOrganization description of the infrastructure shown in the trace
     * @param threadOrganization thread organization of the trace
     * @return PRV file header for a trace given the information
     */
    public static String generateTraceHeader(String date, String duration,
        SystemComposition<PRVNode> infrastructureOrganization,
        ApplicationComposition<PRVApplication> threadOrganization) {
        List<String> cpusPerNode = new ArrayList();
        for (SystemStructure node : infrastructureOrganization.getSubComponents()) {
            cpusPerNode.add(Integer.toString(node.getNumberOfDirectSubcomponents()));
        }

        List<String> tasksPerApp = new ArrayList();
        for (PRVApplication app : threadOrganization.getSubComponents()) {
            List<String> threadsPerTask = new ArrayList();
            for (PRVTask task : app.getSubComponents()) {
                int numThreadsPerTask = task.getNumberOfDirectSubcomponents();
                PRVNode node = task.getNode();
                int numNode = node.getNodeId() + 1;
                threadsPerTask.add(numThreadsPerTask + ":" + numNode);
            }
            int numTasks = app.getNumberOfDirectSubcomponents();
            String taskDescription = numTasks + "(" + String.join(",", threadsPerTask) + ")";
            tasksPerApp.add(taskDescription);
        }

        final String nodesString = cpusPerNode.size() + "(" + String.join(",", cpusPerNode) + ")";
        final String applicationsString = tasksPerApp.size() + ":" + String.join(",", tasksPerApp);
        return "#Paraver (" + date + "):" + duration + ":" + nodesString + ":" + applicationsString;
    }
}
