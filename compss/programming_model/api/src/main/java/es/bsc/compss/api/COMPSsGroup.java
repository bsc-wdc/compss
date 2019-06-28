package es.bsc.compss.api;

public class COMPSsGroup implements AutoCloseable{
    public String groupName;
    public Boolean barrier;
    
    public COMPSsGroup(String groupName, boolean implicitBarrier) {
        this.groupName = groupName;
        this.barrier = implicitBarrier;
    }

    @Override
    public void close() throws Exception {
       System.out.println("Group " + groupName + " closed");
        
    }
}
