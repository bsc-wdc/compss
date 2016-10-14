package priorityTasks;

import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


public interface PriorityTasksItf {

	@Method(declaringClass = "priorityTasks.PriorityTasksImpl", priority = !Constants.PRIORITY)
    void normalTask (
    	@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName
    ); 
	
	@Method(declaringClass = "priorityTasks.PriorityTasksImpl", priority = Constants.PRIORITY)
    void priorityTask (
    	@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName
    ); 
	
}
