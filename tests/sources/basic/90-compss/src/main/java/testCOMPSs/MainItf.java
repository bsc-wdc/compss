package testCOMPSs;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.task.COMPSs;


public interface MainItf {

    @COMPSs(appName = "simple.Simple", computingNodes = "1")
    @Constraints(computingUnits = "2")
    int taskSingleNode(
        @Parameter() int counter
    );

    @COMPSs(runcompss = "$RUNCOMPSS", flags = "-d", appName = "simple.Simple", workingDir = "$TEST_WD", computingNodes = "1")
    @Constraints(computingUnits = "2")
    int taskSingleNodeComplete(
        @Parameter() int counter
    );

    @COMPSs(runcompss = "", appName = "simple.Simple", computingNodes = "2")
    @Constraints(computingUnits = "2")
    int taskMultiNode(
        @Parameter() int counter
    );

    @COMPSs(appName = "simple.Simple", computingNodes = "2")
    @Constraints(computingUnits = "2")
    Integer taskConcurrentMultiNode(
        @Parameter() int counter
    );

}
