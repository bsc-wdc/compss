package es.bsc.compss.worker;

import java.util.HashMap;

public class COMPSsWorker {
    public static final String COMPSS_TASK_ID = "COMPSS_TASK_ID";
    private static HashMap<Integer, Boolean> tasksToCancel;
    
    
    public static void cancellationPoint() throws Exception {
        String taskIdStr = System.getProperty(COMPSS_TASK_ID);
        if (taskIdStr != null && tasksToCancel !=null) {
            Boolean toCancel = tasksToCancel.get(Integer.parseInt(taskIdStr));
           System.out.println("MARTA: ToCancel value is " + toCancel);
            if (toCancel!=null && toCancel) {
                throw new Exception("Task " + taskIdStr + " has been cancelled.");
            }
        }
    }
    
    protected final static void setCancelled(int taskId) {
        if(tasksToCancel == null) {
            tasksToCancel = new HashMap<>();
        }
        tasksToCancel.put(taskId, true);
        System.out.println("MARTA: Task to cancel true");
    }
}