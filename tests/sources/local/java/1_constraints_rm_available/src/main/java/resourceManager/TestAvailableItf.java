package resourceManager;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface TestAvailableItf {

    @Method(declaringClass = "resourceManager.Implementation1")
    @Constraints(computingUnits = "2", storageBW = "100")
    void coreElement1(@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Method(declaringClass = "resourceManager.Implementation1")
    @Constraints(memorySize = "2.0")
    void coreElement2(@Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Method(declaringClass = "resourceManager.Implementation1")
    @Constraints(storageBW = "100")
    void coreElement3(@Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

}
