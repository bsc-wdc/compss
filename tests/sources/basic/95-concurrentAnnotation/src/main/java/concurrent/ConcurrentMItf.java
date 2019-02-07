package concurrent;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;

public interface ConcurrentMItf {

	@Constraints(computingUnits = "1")
	@Method(declaringClass = "concurrent.ConcurrentMImpl")
	void write_one(
		@Parameter(type = Type.FILE, direction = Direction.CONCURRENT) String fileName
	);
	
	@Constraints(computingUnits = "1")
    @Method(declaringClass = "concurrent.ConcurrentMImpl")
    void write_two(
        @Parameter(type = Type.FILE, direction = Direction.IN) String fileName
    );
}
