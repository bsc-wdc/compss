package fileTransfer;

import es.bsc.compss.types.annotations.task.Method;


public interface FaultToleranceItf {

    @Method(declaringClass = "fileTransfer.FaultToleranceImpl")
    void taskLocalhost();

    @Method(declaringClass = "fileTransfer.FaultToleranceImpl")
    void taskHostDown();
}
