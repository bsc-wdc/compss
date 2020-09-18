package fileStreamTest;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;
import es.bsc.distrostreamlib.api.files.FileDistroStream;


public interface MainItf {

    @Constraints(processorArchitecture = "Intel")
    @Method(declaringClass = "fileStreamTest.Tasks")
    void writeFiles(@Parameter(type = Type.STREAM, direction = Direction.OUT) FileDistroStream fds,
        @Parameter() int sleepTime);

    @Constraints(processorArchitecture = "AMD")
    @Method(declaringClass = "fileStreamTest.Tasks")
    Integer readFiles(@Parameter(type = Type.STREAM, direction = Direction.IN) FileDistroStream fds,
        @Parameter() int sleepTime);

    @Method(declaringClass = "fileStreamTest.Tasks")
    Integer processFile(@Parameter(type = Type.FILE, direction = Direction.IN) String fileName);

    @Constraints(processorArchitecture = "Intel")
    @Method(declaringClass = "fileStreamTest.Tasks")
    void writeFilesAlias(@Parameter(type = Type.STREAM, direction = Direction.OUT) FileDistroStream fds,
        @Parameter() int sleepTime);

}