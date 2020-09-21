package files;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface MainItf {

    @Method(declaringClass = "files.MainImpl")
    void taskIn(@Parameter(direction = Direction.IN, type = Type.FILE) String fileName);

    @Method(declaringClass = "files.MainImpl")
    void taskInNested(@Parameter(direction = Direction.IN, type = Type.FILE) String fileName);

    @Method(declaringClass = "files.MainImpl")
    void taskOut(@Parameter(direction = Direction.OUT, type = Type.FILE) String fileName);

    @Method(declaringClass = "files.MainImpl")
    void taskOutNested(@Parameter(direction = Direction.OUT, type = Type.FILE) String fileName);

    @Method(declaringClass = "files.MainImpl")
    void taskInout(@Parameter(direction = Direction.INOUT, type = Type.FILE) String fileName);

    @Method(declaringClass = "files.MainImpl")
    void taskInoutNested(@Parameter(direction = Direction.INOUT, type = Type.FILE) String fileName);

}
