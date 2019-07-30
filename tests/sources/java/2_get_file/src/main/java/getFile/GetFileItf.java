package getFile;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface GetFileItf {

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "getFile.GetFileImpl")
    void writeInFile(@Parameter(type = Type.FILE, direction = Direction.INOUT) String file,
        @Parameter(direction = Direction.IN) int i);

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "getFile.GetFileImpl")
    void readInFile(@Parameter(type = Type.FILE, direction = Direction.IN) String file);

}
