package priorityTasks;

import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


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
