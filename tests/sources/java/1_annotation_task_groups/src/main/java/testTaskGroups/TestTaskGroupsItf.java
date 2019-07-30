package testTaskGroups;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface TestTaskGroupsItf {

    @Method(declaringClass = "testTaskGroups.TestTaskGroupsImpl")
    void writeTwo(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);
}
