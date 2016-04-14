package integratedtoolkit.types;

import java.util.HashMap;


public abstract class COMPSsWorker extends COMPSsNode {

    public COMPSsWorker(String name, HashMap<String, String> properties) {
        super();
    }

    public abstract String getUser();

    //public abstract boolean isTracingReady();
    //public abstract void waitForTracingReady();
    public abstract void updateTaskCount(int processorCoreCount);

    public abstract void announceDestruction() throws Exception;

    public abstract void announceCreation() throws Exception;

}
