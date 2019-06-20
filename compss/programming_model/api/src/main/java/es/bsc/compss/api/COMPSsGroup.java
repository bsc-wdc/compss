package es.bsc.compss.api;

public class COMPSsGroup implements AutoCloseable{
    public String groupName;
    
    public COMPSsGroup(String groupName) {
        this.groupName = groupName;
        
    }

    @Override
    public void close() throws Exception {
       System.out.println("Group " + groupName + " closed");
        
    }
}
