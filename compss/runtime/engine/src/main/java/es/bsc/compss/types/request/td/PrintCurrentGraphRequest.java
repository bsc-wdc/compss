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
package es.bsc.compss.types.request.td;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.allocatableactions.ExecutionAction;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ResourceManager;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Semaphore;


/**
 * The DeleteIntermediateFilesRequest represents a request to delete the intermediate files of the execution from all
 * the worker nodes of the resource pool.
 */
public class PrintCurrentGraphRequest extends TDRequest {

    private static final String ERROR_PRINT_CURRENT_GRAPH = "ERROR: Cannot print current graph state";

    /**
     * Semaphore to synchronize until the representation is constructed.
     */
    private final Semaphore sem;
    /**
     * BufferedWriter describing the graph file where to write the information.
     */
    private final BufferedWriter graph;


    /**
     * Constructs a GetCurrentScheduleRequest.
     *
     * @param sem Semaphore to synchronize until the representation is constructed.
     * @param graph BufferedWriter to print the graph.
     */
    public PrintCurrentGraphRequest(Semaphore sem, BufferedWriter graph) {
        this.sem = sem;
        this.graph = graph;
    }

    /**
     * Returns the semaphore to synchronize until the representation is constructed.
     *
     * @result Semaphore to synchronize until the representation is constructed.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        try {
            PriorityQueue<AbstractTask> pending = new PriorityQueue<>();

            Set<AbstractTask> tasks = new HashSet<>();
            String prefix = "  ";

            // Header options
            this.graph.write(prefix + "outputorder=\"edgesfirst\";");
            this.graph.newLine();
            this.graph.write(prefix + "compound=true;");
            this.graph.newLine();

            /* Subgraph for blocked and scheduled tasks ******************** */
            this.graph.write(prefix + "subgraph cluster1 {");
            this.graph.newLine();
            this.graph.write(prefix + prefix + "color=white");
            this.graph.newLine();

            // Room for Blocked tasks
            int roomIndex = 1;
            this.graph.write(prefix + prefix + "subgraph cluster_room" + roomIndex + " {");
            ++roomIndex;
            this.graph.newLine();
            this.graph.write(prefix + prefix + prefix + "ranksep=0.20;");
            this.graph.newLine();
            this.graph.write(prefix + prefix + prefix + "node[height=0.75];");
            this.graph.newLine();
            this.graph.write(prefix + prefix + prefix + "label = \"Blocked\"");
            this.graph.newLine();
            this.graph.write(prefix + prefix + prefix + "color=red");
            this.graph.newLine();
            List<AllocatableAction> blockedActions = ts.getBlockedActions();
            for (AllocatableAction action : blockedActions) {
                if (action instanceof ExecutionAction) {
                    ExecutionAction se = (ExecutionAction) action;
                    Task t = se.getTask();
                    this.graph.write(prefix + prefix + prefix + t.getDotDescription());
                    this.graph.newLine();
                    LinkedList<AbstractTask> tmpList = new LinkedList<>();
                    boolean done = false;
                    while (!done) {
                        try {
                            synchronized (t) {
                                tmpList.addAll(t.getSuccessors());
                            }
                            done = true;
                        } catch (ConcurrentModificationException cme) {
                            tmpList.clear();
                        }
                    }
                    pending.addAll(tmpList);
                    tasks.add(t);
                }
            }
            this.graph.write(prefix + prefix + "}");
            this.graph.newLine();

            // Room for unassigned tasks
            this.graph.write(prefix + prefix + "subgraph cluster_room" + roomIndex + " {");
            ++roomIndex;
            this.graph.newLine();
            this.graph.write(prefix + prefix + prefix + "ranksep=0.20;");
            this.graph.newLine();
            this.graph.write(prefix + prefix + prefix + "node[height=0.75];");
            this.graph.newLine();
            this.graph.write(prefix + prefix + prefix + "label = \"No Assigned\"");
            this.graph.newLine();
            this.graph.write(prefix + prefix + prefix + "color=orange");
            this.graph.newLine();
            Collection<AllocatableAction> unassignedActions = ts.getUnassignedActions();
            for (AllocatableAction action : unassignedActions) {
                if (action instanceof ExecutionAction) {
                    ExecutionAction se = (ExecutionAction) action;
                    Task t = se.getTask();
                    this.graph.write(prefix + prefix + prefix + t.getDotDescription());
                    this.graph.newLine();

                    LinkedList<AbstractTask> tmpList = new LinkedList<>();
                    boolean done = false;
                    while (!done) {
                        try {
                            synchronized (t) {
                                tmpList.addAll(t.getSuccessors());
                            }
                            done = true;
                        } catch (ConcurrentModificationException cme) {
                            tmpList.clear();
                        }
                    }
                    pending.addAll(tmpList);
                    tasks.add(t);
                }
            }
            this.graph.write(prefix + prefix + "}");
            this.graph.newLine();

            // Add another room for each worker
            for (Worker<? extends WorkerResourceDescription> worker : ResourceManager.getAllWorkers()) {
                this.graph.write(prefix + prefix + "subgraph cluster_room" + roomIndex + " {");
                this.graph.newLine();
                this.graph.write(prefix + prefix + prefix + "label = \"" + worker.getName() + "\"");
                this.graph.newLine();
                this.graph.write(prefix + prefix + prefix + "color=black");
                this.graph.newLine();

                // Create a box for running tasks
                this.graph.write(prefix + prefix + prefix + "subgraph cluster_box" + roomIndex + "1 {");
                this.graph.newLine();
                this.graph.write(prefix + prefix + prefix + prefix + "label = \"Running\"");
                this.graph.newLine();
                this.graph.write(prefix + prefix + prefix + prefix + "ranksep=0.20;");
                this.graph.newLine();
                this.graph.write(prefix + prefix + prefix + prefix + "node[height=0.75];");
                this.graph.newLine();
                this.graph.write(prefix + prefix + prefix + prefix + "color=green");
                this.graph.newLine();
                AllocatableAction[] hostedActions = ts.getHostedActions(worker);
                for (AllocatableAction action : hostedActions) {
                    if (action instanceof ExecutionAction) {
                        ExecutionAction se = (ExecutionAction) action;
                        Task t = se.getTask();
                        this.graph.write(prefix + prefix + prefix + prefix + t.getDotDescription());
                        this.graph.newLine();

                        LinkedList<AbstractTask> tmpList = new LinkedList<>();
                        boolean done = false;
                        while (!done) {
                            try {
                                synchronized (t) {
                                    tmpList.addAll(t.getSuccessors());
                                }
                                done = true;
                            } catch (ConcurrentModificationException cme) {
                                tmpList.clear();
                            }
                        }
                        pending.addAll(tmpList);
                        tasks.add(t);
                    }
                }
                this.graph.write(prefix + prefix + prefix + "}");
                this.graph.newLine();

                this.graph.write(prefix + prefix + prefix + "subgraph cluster_box" + roomIndex + "2 {");
                this.graph.newLine();
                this.graph.write(prefix + prefix + prefix + prefix + "label = \"Resource Blocked\"");
                this.graph.newLine();
                this.graph.write(prefix + prefix + prefix + prefix + "ranksep=0.20;");
                this.graph.newLine();
                this.graph.write(prefix + prefix + prefix + prefix + "node[height=0.75];");
                this.graph.newLine();
                this.graph.write(prefix + prefix + prefix + prefix + "color=red");
                this.graph.newLine();
                PriorityQueue<AllocatableAction> blockedActionsOnResource = ts.getBlockedActionsOnResource(worker);
                for (AllocatableAction action : blockedActionsOnResource) {
                    if (action instanceof ExecutionAction) {
                        ExecutionAction se = (ExecutionAction) action;
                        Task t = se.getTask();
                        this.graph.write(prefix + prefix + prefix + prefix + t.getDotDescription());
                        this.graph.newLine();

                        LinkedList<AbstractTask> tmpList = new LinkedList<>();
                        boolean done = false;
                        while (!done) {
                            try {
                                synchronized (t) {
                                    tmpList.addAll(t.getSuccessors());
                                }
                                done = true;
                            } catch (ConcurrentModificationException cme) {
                                tmpList.clear();
                            }
                        }
                        pending.addAll(tmpList);
                        tasks.add(t);
                    }
                }
                // Close box
                this.graph.write(prefix + prefix + prefix + "}");
                this.graph.newLine();

                // Close room
                this.graph.write(prefix + prefix + "}");
                this.graph.newLine();
                ++roomIndex;
            }

            // Close cluster
            this.graph.write(prefix + "}");
            this.graph.newLine();

            /* Subthis.graph for pending tasks ********************************** */
            this.graph.write(prefix + "subgraph cluster2 {");
            this.graph.newLine();
            this.graph.write(prefix + prefix + "label = \"Pending\"");
            this.graph.newLine();
            this.graph.write(prefix + prefix + "ranksep=0.20;");
            this.graph.newLine();
            this.graph.write(prefix + prefix + "node[height=0.75];");
            this.graph.newLine();
            this.graph.write(prefix + prefix + "color=blue");
            this.graph.newLine();

            while (!pending.isEmpty()) {
                AbstractTask t = pending.poll();
                if (!tasks.contains(t)) {
                    this.graph.write(prefix + prefix + t.getDotDescription());
                    this.graph.newLine();
                    tasks.add(t);
                    LinkedList<AbstractTask> tmpList = new LinkedList<>();
                    boolean done = false;
                    while (!done) {
                        try {
                            synchronized (t) {
                                tmpList.addAll(t.getSuccessors());
                            }
                            done = true;
                        } catch (ConcurrentModificationException cme) {
                            tmpList.clear();
                        }
                    }
                    pending.addAll(tmpList);
                }
            }

            // Close cluster
            this.graph.write(prefix + "}");
            this.graph.newLine();

            /* Write edges *************************************************** */
            for (AbstractTask t : tasks) {
                Set<AbstractTask> successors = new HashSet<>();
                boolean done = false;
                while (!done) {
                    try {
                        synchronized (t) {
                            successors.addAll(t.getSuccessors());
                        }
                        done = true;
                    } catch (ConcurrentModificationException cme) {
                        successors.clear();
                    }
                }
                for (AbstractTask t2 : successors) {
                    this.graph.write(prefix + t.getId() + " -> " + t2.getId() + ";");
                    this.graph.newLine();
                }
            }

            /* Force flush before end ***************************************** */
            this.graph.flush();
        } catch (IOException e) {
            LOGGER.error(ERROR_PRINT_CURRENT_GRAPH);
        }

        this.sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.PRINT_CURRENT_GRAPH;
    }

}
