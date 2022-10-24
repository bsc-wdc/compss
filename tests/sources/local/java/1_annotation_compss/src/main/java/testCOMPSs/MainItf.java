package testCOMPSs;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.task.COMPSs;


public interface MainItf {

    @COMPSs(runcompss = "$RUNCOMPSS", appName = "compssSimple.Simple", computingNodes = "1")
    @Constraints(computingUnits = "2")
    int taskSingleNode(@Parameter() int counter);

    @COMPSs(runcompss = "$RUNCOMPSS", appName = "compssSimple.Simple", workingDir = "$TEST_WORKING_DIR", computingNodes = "1")
    @Constraints(computingUnits = "4")
    int taskSingleNodeComplete(@Parameter() int counter);

    @COMPSs(runcompss = "$RUNCOMPSS", flags = "-d", appName = "compssSimple.Simple", computingNodes = "2")
    @Constraints(computingUnits = "2")
    int taskMultiNodeFit(@Parameter() int counter);

    @COMPSs(runcompss = "$RUNCOMPSS", workerInMaster = Constants.WORKER_IN_MASTER, appName = "compssSimple.Simple", computingNodes = "2")
    @Constraints(computingUnits = "4")
    int taskMultiNodeNoFit(@Parameter() int counter);

    @COMPSs(runcompss = "$RUNCOMPSS", appName = "compssSimple.Simple", computingNodes = "2")
    @Constraints(computingUnits = "2")
    Integer taskConcurrentMultiNode(@Parameter() int counter);

    @COMPSs(runcompss = "$RUNCOMPSS", appName = "compssSimple.Simple", workerInMaster = Constants.WORKER_NOT_IN_MASTER, computingNodes = "2")
    @Constraints(computingUnits = "2")
    Integer taskNoWorkerInMasterFit(@Parameter() int counter);

    @COMPSs(runcompss = "$RUNCOMPSS", appName = "compssSimple.Simple", workerInMaster = Constants.WORKER_NOT_IN_MASTER, computingNodes = "2")
    @Constraints(computingUnits = "4")
    Integer taskNoWorkerInMasterNoFit(@Parameter() int counter);

    @COMPSs(runcompss = "$RUNCOMPSS", flags = "--log_dir=${TEST_LOG_DIR}", appName = "compssSimple.Simple", computingNodes = "1")
    @Constraints(computingUnits = "2")
    Integer taskBaseLogDir(@Parameter() int counter);

}
