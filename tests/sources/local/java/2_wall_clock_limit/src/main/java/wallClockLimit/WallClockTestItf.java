package wallClockLimit;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface WallClockTestItf {

    @Method(declaringClass = "wallClockLimit.WallClockTestImpl")
    void inoutLongTask(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Method(declaringClass = "wallClockLimit.WallClockTestImpl")
    void longTask(@Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Method(declaringClass = "wallClockLimit.WallClockTestImpl")
    void executedTask(@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Method(declaringClass = "wallClockLimit.WallClockTestImpl")
    void cancelledTask(@Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

}
