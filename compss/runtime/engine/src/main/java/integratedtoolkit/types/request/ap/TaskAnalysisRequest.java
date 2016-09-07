package integratedtoolkit.types.request.ap;

import integratedtoolkit.components.impl.AccessProcessor;
import integratedtoolkit.components.impl.DataInfoProvider;
import integratedtoolkit.components.impl.TaskAnalyser;
import integratedtoolkit.components.impl.TaskDispatcher;
import integratedtoolkit.types.Task;


public class TaskAnalysisRequest extends APRequest {

    private Task task;


    public TaskAnalysisRequest(Task task) {
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
        ta.processTask(task);
        td.executeTask(ap, task);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.ANALYSE_TASK;
    }

}
