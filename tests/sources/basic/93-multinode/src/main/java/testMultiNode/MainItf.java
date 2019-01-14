package testMultiNode;

import es.bsc.compss.types.annotations.task.MultiNode;
import es.bsc.compss.types.annotations.Constraints;


public interface MainItf {

    @MultiNode(declaringClass = "testMultiNode.MainImpl", computingNodes = "2")
    @Constraints(computingUnits = "2")
    int multiNodeTask();

}
