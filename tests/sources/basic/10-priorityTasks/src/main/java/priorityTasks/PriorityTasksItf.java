package priorityTasks;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface PriorityTasksItf {

	@Method(declaringClass = "priorityTasks.PriorityTasksImpl", priority = Constants.IS_NOT_PRIORITARY_TASK)
    void normalTask (
    	@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName
    ); 
	
	@Method(declaringClass = "priorityTasks.PriorityTasksImpl", priority = Constants.IS_PRIORITARY_TASK)
    void priorityTask (
    	@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName
    ); 
	
}
