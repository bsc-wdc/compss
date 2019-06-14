package testTaskGroups;

import java.lang.Thread;

import es.bsc.compss.worker.COMPSsWorker;

public class TestTaskGroupsImpl {


    public static void timeOutTaskFast() throws Exception {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        System.out.println("Before cancellation point");
        COMPSsWorker.cancellationPoint();
        System.out.println("After the cancellation point");
    }
    
    public static void timeOutTaskSlow() throws Exception {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        System.out.println("Before cancellation point");
        COMPSsWorker.cancellationPoint();
        System.out.println("After the cancellation point");
    }
    
}