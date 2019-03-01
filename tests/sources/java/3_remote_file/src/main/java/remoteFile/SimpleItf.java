package remoteFile;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface SimpleItf {

    @Method(declaringClass = "remoteFile.SimpleImpl")
    void increment(
        @Parameter(type = Type.FILE, direction = Direction.IN) String fileIN,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String fileOUT
    );

}
