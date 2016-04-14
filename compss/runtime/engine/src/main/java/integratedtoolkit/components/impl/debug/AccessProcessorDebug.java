package integratedtoolkit.components.impl.debug;

import integratedtoolkit.components.impl.AccessProcessor;


public class AccessProcessorDebug extends AccessProcessor {

    TaskDispatcherDebug taskDispatcher;

    public AccessProcessorDebug() {
        super();
    }

    public void setTD(TaskDispatcherDebug td) {
        this.taskDispatcher = td;
        taskAnalyser.setCoWorkers(dataInfoProvider, td);
    }

}
