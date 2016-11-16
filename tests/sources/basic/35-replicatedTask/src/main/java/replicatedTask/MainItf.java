package replicatedTask;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.SchedulerHints;
import integratedtoolkit.types.annotations.task.Method;
import types.Pair;


public interface MainItf {
    
    @Method(declaringClass = "replicatedTask.MainImpl")
    void initInitialP(
        @Parameter(type = Type.OBJECT, direction = Direction.INOUT) Pair p
    );

    @Method(declaringClass = "replicatedTask.MainImpl")
    @SchedulerHints(isReplicated = true)
    Pair globalTask(
        @Parameter(type = Type.OBJECT, direction = Direction.IN) Pair p,
        @Parameter(type = Type.INT, direction = Direction.IN) int newX
    );
    
    @Method(declaringClass = "replicatedTask.MainImpl")
    void normalTask(
        @Parameter(type = Type.OBJECT, direction = Direction.IN) Pair p
    );

}
