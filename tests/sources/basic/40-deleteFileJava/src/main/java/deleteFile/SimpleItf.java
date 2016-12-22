package deleteFile;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface SimpleItf {

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "simple.SimpleImpl")
    void increment(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String file
    );
    
    @Constraints(computingUnits = "1")
    @Method(declaringClass = "simple.SimpleImpl")
    void increment2(
            @Parameter(type = Type.FILE, direction = Direction.IN) String file_in,
            @Parameter(type = Type.FILE, direction = Direction.OUT) String file_out
        );

}
