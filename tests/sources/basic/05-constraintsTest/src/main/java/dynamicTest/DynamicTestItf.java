package dynamicTest;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.task.Method;


public interface DynamicTestItf {
	
	@Method(declaringClass = "dynamicTest.DynamicTestImpl")
	@Constraints(computingUnits = "3")
	void coreElementDynamic1();
	
	@Method(declaringClass = "dynamicTest.DynamicTestImpl")
	@Constraints(computingUnits = "1")
	void coreElementDynamic2();
}
