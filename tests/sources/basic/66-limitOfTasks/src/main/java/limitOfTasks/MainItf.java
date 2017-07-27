package limitOfTasks;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface MainItf {

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "limitOfTasks.MainImpl")
    void increment(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String file
    );

}
