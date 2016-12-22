package remoteFile;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface SimpleItf {

    @Method(declaringClass = "remoteFile.SimpleImpl")
    void increment(
        @Parameter(type = Type.FILE, direction = Direction.IN) String fileIN,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String fileOUT
    );

}
