package workerEnvironment;

import es.bsc.compss.types.annotations.task.Method;


public interface MainItf {

    @Method(declaringClass = "workerEnvironment.MainImpl")
    boolean checkWorkerEnv();

}
