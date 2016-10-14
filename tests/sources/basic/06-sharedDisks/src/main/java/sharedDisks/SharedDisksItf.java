package sharedDisks;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


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
