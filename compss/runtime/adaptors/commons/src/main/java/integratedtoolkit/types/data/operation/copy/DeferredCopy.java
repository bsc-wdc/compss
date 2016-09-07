package integratedtoolkit.types.data.operation.copy;

import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;


public class DeferredCopy extends Copy {

    public DeferredCopy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        super(srcData, prefSrc, prefTgt, tgtData, reason, listener);
    }

}