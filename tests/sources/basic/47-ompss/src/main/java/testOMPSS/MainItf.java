package testOMPSS;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.annotations.task.OmpSs;


public interface MainItf {
    
    @OmpSs(binary = "${HELLO_WORLD_OMPSS}")
    int ompssTask(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String stdOut
    );

}
