package deleteFileSharedDisk;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface DeleteFileSharedDiskItf {

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "deleteFileSharedDisk.DeleteFileSharedDiskImpl")
    void readFromFile(@Parameter(type = Type.FILE, direction = Direction.IN) String file);

}
