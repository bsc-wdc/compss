package testNodeInitFailure;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.task.Method;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;


public interface TestNodeInitFailureItf {

    @Constraints(computingUnits = "1", memorySize = "0.3")
    @Method(declaringClass = "testNodeInitFailure.TestNodeInitFailureImpl")
    void increment(@Parameter(type = Type.FILE, direction = Direction.IN) String file,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String file_out);
}
