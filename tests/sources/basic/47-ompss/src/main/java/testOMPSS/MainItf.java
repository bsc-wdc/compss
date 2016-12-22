package testOMPSS;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Stream;
import integratedtoolkit.types.annotations.task.OmpSs;


public interface MainItf {
    
    @OmpSs(binary = "${HELLO_WORLD_OMPSS}")
    int ompssTask(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String stdOut
    );

}
