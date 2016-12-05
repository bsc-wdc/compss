package simple;

import integratedtoolkit.util.ResourceManager;

import java.io.FileInputStream;
import java.io.FileOutputStream;


public class Simple {
	private static final String counterName = "counter";
	
	public static void main(String[] args) {
		// Check parameters parameters
		if (args.length != 5) {
			System.out.println("[ERROR] Incorrect number of parameters");
			System.out.println("    Usage simple <initVal> <increment> <minVM> <maxVM> <creationTime>");
			System.exit(-1);
		}
		
		FileOutputStream fos;
		FileInputStream fis;
		try{
			//Get parameters
			int initialValue = Integer.parseInt(args[0]);	
			int increment = Integer.parseInt(args[1]);
			int minVM = Integer.parseInt(args[2]);
			int maxVM = Integer.parseInt(args[3]);
			int creationTime = Integer.parseInt(args[4]); 
			System.out.println("[LOG] Initial counter value is " + initialValue);
			System.out.println("[LOG] Minimal VM is " + minVM);
			System.out.println("[LOG] Maximal VM is " + maxVM);
			System.out.println("[LOG] Creating VM time is " + creationTime);
			
			//Check initial values
			int currentRes = ResourceManager.getAllWorkers().size();
			System.out.println("[LOG] Initial number of resources is " + currentRes);
			if (currentRes != 0){
				System.out.println("FAIL: Initial Resources incorrect " + currentRes);
				System.exit(-1);
			} else {
				System.out.println("** Initial Resource detection  OK **");
			}
			
			//Wait to have initial VMs loaded
			System.out.println("[LOG] Creating minimal number of VM's. Waiting...");
			for (int i = 0; i < minVM; i++){
				try {
					Thread.sleep(creationTime*1000);
				} catch (InterruptedException e) {
					System.out.println("FAIL: Cannot sleep current thread");
					e.printStackTrace();
					System.exit(-1);
				}
			}
			
			//Check number of initial VMs
			System.out.println("[LOG] Checking initial number of VMs " + minVM);
			currentRes = ResourceManager.getAllWorkers().size();
			if (currentRes != minVM){
				System.out.println("FAIL: Initial VMs incorrect " + currentRes + " (" + minVM + ")");
				System.exit(-1);
			} else {
				System.out.println("** Initial VM creation  OK **");
			}
			
			// Execute increment
			System.out.println("[LOG] Sending increment executions");
			for (int i = 0; i < increment; i++){
				fos = new FileOutputStream(counterName + i);
				fos.write(initialValue);
				fos.close();
				SimpleImpl.increment(counterName + i);
			}

			//Open the file and print final counter value (should be 2)
			for (int i = 0; i < increment; i++){
				fis = new FileInputStream(counterName + i);
				int finalValue = fis.read();
				int expected = initialValue + 1;
				System.out.println("[LOG] Final counter" + i + " value is " + finalValue + " (expected: " + expected + ")");
				fis.close();
				if (finalValue != expected){
					System.out.println("FAIL: Incorrect final value at counter" + i);
					System.exit(-1);
				}
			}
			System.out.println("[ATTENTION] Elasticity on VM's must be checked on result script");
			System.out.println("** Application values OK **");
			long sleepTime = creationTime*2*1000;
			System.out.println("Waiting " + sleepTime + " ms for the elastic VMs to be removed");
			Thread.sleep(sleepTime);
		} catch (Exception ioe) {
			System.out.println("[ERROR] Exception found");
			ioe.printStackTrace();
			System.exit(-1);
		}
	}

}
