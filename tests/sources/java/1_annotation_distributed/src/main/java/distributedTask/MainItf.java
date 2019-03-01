package distributedTask;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.SchedulerHints;
import es.bsc.compss.types.annotations.task.Method;


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
