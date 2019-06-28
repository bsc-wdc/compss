package es.bsc.compss.loader.total;

import es.bsc.compss.api.COMPSsGroup;
import es.bsc.compss.api.COMPSsRuntime;

public class COMPSsGroupLoader extends COMPSsGroup {
    
    private final COMPSsRuntime api;
    private long appId;
    
    public COMPSsGroupLoader(COMPSsRuntime api, Long appId, String groupName, boolean implicitBarrier) {
        super(groupName, implicitBarrier);
        this.api = api;
        this.api.openTaskGroup(this.groupName, implicitBarrier);
        this.appId = appId;        
    }

    @Override
    public void close() throws Exception {
        this.api.closeTaskGroup(this.groupName);
        if (this.barrier == true) {
            this.api.barrierGroup(appId, groupName);
        }
    }

   
}
