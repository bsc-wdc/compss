package remoteFile;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;


public interface SimpleItf {

    @Constraints(computingUnits = 1)
    @Method(declaringClass = "remoteFile.SimpleImpl")
    void increment(
        @Parameter(type = Type.FILE, direction = Direction.IN) String fileIN,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String fileOUT
    );

}
