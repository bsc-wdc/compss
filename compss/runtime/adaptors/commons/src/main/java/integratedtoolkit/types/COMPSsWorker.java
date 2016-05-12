package integratedtoolkit.types;

import integratedtoolkit.types.resources.configuration.Configuration;


public abstract class COMPSsWorker extends COMPSsNode {

    public COMPSsWorker(String name, Configuration config) {
        super();
    }

    public abstract String getUser();
    
    public abstract String getClasspath();
    
    public abstract String getPythonpath();

    public abstract void updateTaskCount(int processorCoreCount);

    public abstract void announceDestruction() throws Exception;

    public abstract void announceCreation() throws Exception;

}
