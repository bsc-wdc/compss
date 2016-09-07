package integratedtoolkit.types.request.ap;

import integratedtoolkit.components.impl.AccessProcessor;
import integratedtoolkit.components.impl.DataInfoProvider;
import integratedtoolkit.components.impl.TaskAnalyser;
import integratedtoolkit.components.impl.TaskDispatcher;
import integratedtoolkit.types.Task;


public class TaskEndNotification extends APRequest {

    private Task task;


    public TaskEndNotification(Task task) {
        this.task = task;
    }

    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher<?, ?> td) {
        ta.endTask(task);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.UPDATE_GRAPH;
    }

}
