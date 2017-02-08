package distributedTask;

import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.SchedulerHints;
import integratedtoolkit.types.annotations.task.Method;


public interface MainItf {

    @Method(declaringClass = "distributedTask.MainImpl")
    @Constraints(computingUnits = "1")
    void normalTask(
        @Parameter(type = Type.OBJECT, direction = Direction.IN) String msg
    );
    
    @Method(declaringClass = "distributedTask.MainImpl")
    @Constraints(computingUnits = "1")
    @SchedulerHints(isDistributed = Constants.IS_DISTRIBUTED_TASK)
    void distributedTask(
        @Parameter(type = Type.OBJECT, direction = Direction.IN) String msg
    );

}
