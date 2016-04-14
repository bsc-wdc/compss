package testReschedulingFail;


//N workers available, task fails in all of them
public class Main {
	private static int NUM_TASKS = 2;
	
	public static void main(String[] args) {
		Dummy[] dummies = new Dummy[NUM_TASKS];
		for(int i = 0; i < NUM_TASKS; ++i) {
			dummies[i] = errorTask(i, new Dummy());
		}
		
		for(int i = 0; i < NUM_TASKS; ++i) {
			System.out.println("Finished task " + (i + 1) + " (" + dummies[i] + ")");
		}
	}
	
	public static Dummy errorTask(int x, Dummy din) {
		Dummy d = null; 
		try { 
			Thread.sleep(100); 
		} catch(Exception e) {
			// No need to catch such exception
		}
		
		// Execute a null access
		d.foo();
		
		return d;
	}
}
