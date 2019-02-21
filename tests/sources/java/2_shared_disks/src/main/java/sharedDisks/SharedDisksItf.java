package sharedDisks;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface SharedDisksItf {
	
	@Constraints(processorArchitecture = "architecture1")
    @Method(declaringClass = "sharedDisks.SharedDisksImpl")
	int inputTask (
		@Parameter(type = Type.FILE, direction = Direction.IN) String fileName,
		@Parameter(type = Type.STRING, direction = Direction.IN) String name
	); 

	@Constraints(processorArchitecture = "architecture2")
    @Method(declaringClass = "sharedDisks.SharedDisksImpl")
	void outputTaskWriter (
		@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName
	);

	@Constraints(processorArchitecture = "architecture3")
    @Method(declaringClass = "sharedDisks.SharedDisksImpl")
	void outputTaskReader (
		@Parameter(type = Type.FILE, direction = Direction.IN) String fileName
	);
	
}
