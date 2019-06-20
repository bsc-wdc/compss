package es.bsc.compss.loader.total;

import es.bsc.compss.api.COMPSsGroup;
import es.bsc.compss.api.COMPSsRuntime;

public class COMPSsGroupLoader extends COMPSsGroup {
    
    private final COMPSsRuntime api;
    
    public COMPSsGroupLoader(COMPSsRuntime api, String groupName) {
        super(groupName);
        this.api = api;
        this.api.openTaskGroup(this.groupName);
        
    }

    @Override
    public void close() throws Exception {
        this.api.closeTaskGroup(this.groupName);
    }

   
}
