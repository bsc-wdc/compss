package objectDeregister;

public class ObjectDeregisterImpl {

	
    public static void task1 (int n, Dummy d1) { //Writes
    	d1.setDummyNumber(n);
    }

    public static void task2 (int n, Dummy d2) { //Writes
    	d2.setDummyNumber(n);
    }
    
    public static void task3 (Dummy d3) { //Reads
    	d3.getDummyNumber();
    }
    
}
