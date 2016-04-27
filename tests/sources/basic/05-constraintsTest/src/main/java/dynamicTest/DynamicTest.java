package dynamicTest;


public class DynamicTest {

    public static void main(String[] args) {
    	try{
    		Thread.sleep(7000);
    	} catch(Exception e){
    	}
    	
    	//Run Dynamic Test
    	System.out.println("[LOG] Running Dynamic Test");
    	//Launch 2 tasks on Core constraints Core = 3
    	System.out.println("[LOG] Creating tasks Core = 3");
    	for (int i = 0; i < 2; ++i) {
    		DynamicTestImpl.coreElementDynamic1();
    	}
    	
    	//Launch 2 tasks on Core constraints Core = 1
    	System.out.println("[LOG] Creating tasks Core = 1");
    	for (int i = 0; i < 2; ++i) {
    		DynamicTestImpl.coreElementDynamic2();
    	}
    	
    	//Result is checked on runtime.log
    	System.out.println("[LOG] Main program finished. No more tasks.");
    }

}
