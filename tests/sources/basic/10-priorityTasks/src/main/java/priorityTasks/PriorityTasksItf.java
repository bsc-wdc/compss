package priorityTasks;

import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;


public interface PriorityTasksItf {

	@Method(declaringClass = "priorityTasks.PriorityTasksImpl", priority = false)
    void normalTask (
    	@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName
    ); 
	
	@Method(declaringClass = "priorityTasks.PriorityTasksImpl", priority = true)
    void priorityTask (
    	@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName
    ); 
	
}
