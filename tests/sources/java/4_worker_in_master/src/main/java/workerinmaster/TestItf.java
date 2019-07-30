
package workerinmaster;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface TestItf {

    @Method(declaringClass = "workerinmaster.Tasks")
    void testBasicTypes(@Parameter(type = Type.FILE, direction = Direction.OUT) String file,
        @Parameter(type = Type.BOOLEAN) boolean b, @Parameter(type = Type.CHAR) char c,
        @Parameter(type = Type.STRING) String s, @Parameter(type = Type.BYTE) byte by,
        @Parameter(type = Type.SHORT) short sh, @Parameter(type = Type.INT) int i, @Parameter(type = Type.LONG) long l,
        @Parameter(type = Type.FLOAT) float f, @Parameter(type = Type.DOUBLE) double d);

    @Method(declaringClass = "workerinmaster.Tasks")
    void createFileWithContent(@Parameter(type = Type.STRING) String content,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Method(declaringClass = "workerinmaster.Tasks")
    void checkFileWithContent(@Parameter(type = Type.STRING) String content,
        @Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Method(declaringClass = "workerinmaster.Tasks")
    void checkAndUpdateFileWithContent(@Parameter(type = Type.STRING) String content,
        @Parameter(type = Type.STRING) String newContent,
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Method(declaringClass = "workerinmaster.Tasks")
    StringWrapper createObjectWithContent(@Parameter(type = Type.STRING) String content);

    @Method(declaringClass = "workerinmaster.Tasks")
    void checkObjectWithContent(@Parameter(type = Type.STRING) String content,
        @Parameter(type = Type.OBJECT) StringWrapper sw);

    @Method(declaringClass = "workerinmaster.Tasks")
    void checkAndUpdateObjectWithContent(@Parameter(type = Type.STRING) String content,
        @Parameter(type = Type.STRING) String newContent,
        @Parameter(type = Type.OBJECT, direction = Direction.INOUT) StringWrapper sw);

    @Method(declaringClass = "workerinmaster.Tasks")
    Report sleepTask();

}
