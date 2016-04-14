package integratedtoolkit.components.scheduler.impl;

import org.apache.log4j.Logger;

import integratedtoolkit.api.ITExecution;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.components.scheduler.SchedulerPolicies;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.Worker;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;


public class DefaultSchedulerPolicies extends SchedulerPolicies {

    protected static final Logger logger = Logger.getLogger(Loggers.TS_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    public PriorityQueue<ObjectValue<Task>> sortTasksForResource(Worker<?> host, List<Task> tasks, TaskScheduler.ExecutionProfile[][] profiles) {
        PriorityQueue<ObjectValue<Task>> pq = new PriorityQueue<ObjectValue<Task>>();
        for (Task t : tasks) {
            if (host.canRun(t.getTaskParams().getId())) {
                int score = 0;
                Parameter[] params = t.getTaskParams().getParameters();
                for (Parameter p : params) {
                    if (p instanceof DependencyParameter) {
                        DependencyParameter fp = (DependencyParameter) p;
                        DataInstanceId dId = null;
                        switch (fp.getDirection()) {
                            case IN:
                                DataAccessId.RAccessId raId = (DataAccessId.RAccessId) fp.getDataAccessId();
                                dId = raId.getReadDataInstance();
                                break;
                            case INOUT:
                                DataAccessId.RWAccessId rwaId = (DataAccessId.RWAccessId) fp.getDataAccessId();
                                dId = rwaId.getReadDataInstance();
                                break;
                            case OUT:
                                break;
                        }
                        if (dId != null) {
                            HashSet<Resource> hosts = Comm.getData(dId.getRenaming()).getAllHosts();
                            for (Resource h : hosts) {
                                if (h != null && h.compareTo(host) == 0) {
                                    score++;
                                }
                            }
                        }

                    }
                }
                logger.info("Available Resource: " + host.getName() + ". Task: " + t.getId() + ", score: " + score);
                pq.add(new ObjectValue<Task>(t, score));
            }
        }

        return pq;
    }

    public PriorityQueue<ObjectValue<Worker<?>>> sortResourcesForTask(Task t, Set<Worker<?>> resources, TaskScheduler.ExecutionProfile[][] profiles) {
        PriorityQueue<ObjectValue<Worker<?>>> pq = new PriorityQueue<ObjectValue<Worker<?>>>();

        Parameter[] params = t.getTaskParams().getParameters();
        HashMap<Resource, Integer> hostToScore = new HashMap<Resource, Integer>(params.length * 2);

        // Obtain the scores for each host: number of task parameters that are located in the host
        for (Parameter p : params) {
            if (p instanceof DependencyParameter && p.getDirection() != ITExecution.ParamDirection.OUT) {
                DependencyParameter dp = (DependencyParameter) p;
                DataInstanceId dId = null;
                switch (dp.getDirection()) {
                    case IN:
                        DataAccessId.RAccessId raId = (DataAccessId.RAccessId) dp.getDataAccessId();
                        dId = raId.getReadDataInstance();
                        break;
                    case INOUT:
                        DataAccessId.RWAccessId rwaId = (DataAccessId.RWAccessId) dp.getDataAccessId();
                        dId = rwaId.getReadDataInstance();
                        break;
                    case OUT:
                        break;
                }

                if (dId != null) {
                    HashSet<Resource> hosts = Comm.getData(dId.getRenaming()).getAllHosts();
                    for (Resource host : hosts) {
                        if (host == null) {
                            continue;
                        }
                        Integer score;
                        if ((score = hostToScore.get(host)) == null) {
                            score = new Integer(0);
                            hostToScore.put(host, score);
                        }
                        hostToScore.put(host, score + 1);
                    }
                }
            }
        }

        // Random sort of the valid resources to generate some kind of RoundRobin
        LinkedList<Worker<?>> shuffled_resources = new LinkedList<Worker<?>> ();
        shuffled_resources.addAll(resources);
        Collections.shuffle(shuffled_resources);
        for (Worker<?> resource : shuffled_resources) {
            Integer score = hostToScore.get(resource);
            if (score == null) {
                pq.offer(new ObjectValue<Worker<?>>(resource, 0));
                logger.info("Resource: " + resource.getName() + ", score: 0");
            } else {
                pq.offer(new ObjectValue<Worker<?>>(resource, score));
                logger.info("Resource: " + resource.getName() + ", score: " + score);
            }
        }
        return pq;
    }

    @Override
    public OwnerTask[] stealTasks(Worker<?> destResource, HashMap<String, LinkedList<Task>> pendingTasks, int numberOfTasks, TaskScheduler.ExecutionProfile[][] profiles) {

        OwnerTask[] stolenTasks = new OwnerTask[numberOfTasks];
        PriorityQueue<ObjectValue<OwnerTask>> pq = new PriorityQueue<ObjectValue<OwnerTask>>();
        for (java.util.Map.Entry<String, LinkedList<Task>> e : pendingTasks.entrySet()) {
            String ownerName = e.getKey();
            LinkedList<Task> candidates = e.getValue();
            for (Task t : candidates) {
                int score = 0;
                if (t.isSchedulingStrongForced()) {
                    continue;
                } else if (!t.isSchedulingForced()) {
                    score = 10000;
                }
                if (destResource.canRun(t.getTaskParams().getId())) {
                    Parameter[] params = t.getTaskParams().getParameters();
                    for (Parameter p : params) {
                        if (p instanceof DependencyParameter) {
                            DependencyParameter dp = (DependencyParameter) p;
                            DataInstanceId dId = null;
                            switch (dp.getDirection()) {
                                case IN:
                                    DataAccessId.RAccessId raId = (DataAccessId.RAccessId) dp.getDataAccessId();
                                    dId = raId.getReadDataInstance();
                                    break;
                                case INOUT:
                                    DataAccessId.RWAccessId rwaId = (DataAccessId.RWAccessId) dp.getDataAccessId();
                                    dId = rwaId.getReadDataInstance();
                                    break;
                                case OUT:
                                    break;
                            }

                            if (dId != null) {
                                HashSet<Resource> hosts = Comm.getData(dId.getRenaming()).getAllHosts();
                                for (Resource host : hosts) {
                                    if (host == null) {
                                        continue;
                                    }

                                    if (host.equals(ownerName)) {
                                        score--;
                                        break;
                                    }
                                    if (host.equals(destResource)) {
                                        score += 2;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                pq.offer(new ObjectValue<OwnerTask>(new OwnerTask(ownerName, t), score));
            }
        }

        int i = 0;
        while (pq.iterator().hasNext() && i < numberOfTasks) {
            stolenTasks[i] = pq.iterator().next().o;
            i++;
        }
        return stolenTasks;
    }

    @Override
    public LinkedList<Implementation<?>> sortImplementationsForResource(LinkedList<Implementation<?>> runnable, Worker<?> resource, TaskScheduler.ExecutionProfile[][] profiles) {
        LinkedList<Implementation<?>> sorted = new LinkedList<Implementation<?>>();
        sorted.addAll(runnable);
        return sorted;
    }
}
