package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.Task;

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
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        ta.processTask(task);
        td.executeTask(ap, task);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.ANALYSE_TASK;
    }

}
