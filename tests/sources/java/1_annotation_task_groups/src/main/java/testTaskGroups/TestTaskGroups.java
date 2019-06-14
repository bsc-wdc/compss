package testTaskGroups;


public class TestTaskGroups {
    
    public static final int N = 3;
    public static final int M = 4;
    public static final int MAX_AVAILABLE = 1;


    public static void main(String[] args) throws Exception {
        
        System.out.println("[LOG] Test task time out");
        testTaskTimeOut();
             
    }
    
    private static void testTaskTimeOut() throws Exception {
        TestTaskGroupsImpl.timeOutTaskFast();
        TestTaskGroupsImpl.timeOutTaskSlow();


    }

}
