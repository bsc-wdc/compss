package cancelRunningTasks;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface CancelRunningTasksItf {

    @Method(declaringClass = "cancelRunningTasks.CancelRunningTasksImpl")
    void throwException(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Method(declaringClass = "cancelRunningTasks.CancelRunningTasksImpl")
    void longTask(@Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Method(declaringClass = "cancelRunningTasks.CancelRunningTasksImpl")
    void executedTask(@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Method(declaringClass = "cancelRunningTasks.CancelRunningTasksImpl")
    void cancelledTask(@Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Method(declaringClass = "cancelRunningTasks.CancelRunningTasksImpl")
    void writeTwo(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);
}
