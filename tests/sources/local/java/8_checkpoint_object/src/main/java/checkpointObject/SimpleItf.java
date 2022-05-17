package checkpointObject;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;
import checkpointObject.SimpObj;


public interface SimpleItf {

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "checkpointObject.SimpleImpl")
    SimpObj increment(@Parameter(direction = Direction.IN) SimpObj counter);
}
