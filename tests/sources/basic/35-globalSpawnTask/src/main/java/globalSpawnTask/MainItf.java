package globalSpawnTask;

import integratedtoolkit.types.annotations.Method;


public interface MainItf {

    @Method(declaringClass = "globalSpawnTask.MainImpl", computingNodes = "all")
    void globalTask();

}
