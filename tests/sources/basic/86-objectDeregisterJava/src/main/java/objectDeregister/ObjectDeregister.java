package objectDeregister;

import es.bsc.compss.api.COMPSs;

public class ObjectDeregister {

    public static void main(String[] args) {
    
    	/* This test provides a Dummy object used to provoke a situation where
    	 * useless objects remain in memory until the end of execution, this is
    	 * creating a loop using the same object all over. */
    	
    	final int ITERATIONS = 2;
    	
    	System.out.println("[LOG] Starting to execute the tasks");
    	for (int i = 0; i < ITERATIONS; ++i) {
        	Dummy d = new Dummy(0);

        	ObjectDeregisterImpl.task1(i, d); 
    		//ObjectDeregisterImpl.task2(i+1, d); 
    		ObjectDeregisterImpl.task3(d); 
    		//Allows garbage collector to delete the object from memory
    		COMPSs.deregisterObject((Object) d);
    	}
    	System.out.println("[LOG] Finished executing tasks");
    	
    	/* task1 & task2 write into the object so a copy 
    	 * of the object will be created, the task3
    	 * will just read the object task2 used */
    	
    }

}
