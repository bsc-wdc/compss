package es.bsc.compss.types.data.operation.copy;

import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;


public class DeferredCopy extends Copy {

    public DeferredCopy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        super(srcData, prefSrc, prefTgt, tgtData, reason, listener);
    }

}
