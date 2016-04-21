package integratedtoolkit.components.scheduler;

import integratedtoolkit.components.impl.JobManager;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.loader.PSCOId;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.resources.Worker;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;


public abstract class SchedulerPolicies {

    public JobManager JM;

    public abstract PriorityQueue<ObjectValue<Task>> sortTasksForResource(Worker<?> hostName, List<Task> tasksToReschedule, TaskScheduler.ExecutionProfile[][] profiles);

    public abstract PriorityQueue<ObjectValue<Worker<?>>> sortResourcesForTask(Task t, Set<Worker<?>> resources, TaskScheduler.ExecutionProfile[][] profiles);

    public abstract OwnerTask[] stealTasks(Worker<?> destResource, HashMap<String, LinkedList<Task>> pendingTasks, int numberOfTasks, TaskScheduler.ExecutionProfile[][] profiles);

    public abstract LinkedList<Implementation<?>> sortImplementationsForResource(LinkedList<Implementation<?>> get, Worker<?> chosenResource, TaskScheduler.ExecutionProfile[][] profiles);
    
	public abstract HashMap<Integer, PSCOId> getIdToPscoId();

    public class OwnerTask {

        public String owner;
        public Task t;

        public OwnerTask(String owner, Task t) {
            this.owner = owner;
            this.t = t;
        }
    }

    public class ObjectValue<T> implements Comparable<ObjectValue<T>> {

        public T o;
        public int value;

        public ObjectValue(T o, int value) {
            this.o = o;
            this.value = value;
        }

        public int compareTo(ObjectValue<T> o) {
            return o.value - this.value;
        }
    }

}
