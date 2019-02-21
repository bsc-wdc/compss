package streams;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;
import streams.components.COMPSsStream;
import streams.types.Result;


public interface MainItf {

    @Constraints(computingUnits = "1")
    @Method(declaringClass = "streams.components.Producer")
    Integer sendMessages(
        @Parameter(type = Type.OBJECT, direction = Direction.IN) COMPSsStream stream, // TODO: FIX direction and type
        @Parameter() int numMessages
    );
    
    @Constraints(computingUnits = "1")
    @Method(declaringClass = "streams.components.Consumer")
    Result receiveMessages(
        @Parameter(type = Type.OBJECT, direction = Direction.IN) COMPSsStream stream
    );
    
}
