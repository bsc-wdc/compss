package constraintManager;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class Implementation1 {

    public void coreElement0() {
        System.out.println("Running " + this.getClass().getCanonicalName() + ".coreElement1");
    }

    public void coreElement1() {
        System.out.println("Running " + this.getClass().getCanonicalName() + ".coreElement2");
    }

    public void coreElement2() {
        System.out.println("Running " + this.getClass().getCanonicalName() + ".coreElement3");
    }

    public void coreElement3() {
        System.out.println("Running " + this.getClass().getCanonicalName() + ".coreElement4");
    }
    
    public void coreElement4() {
        System.out.println("Running " + this.getClass().getCanonicalName() + ".coreElement4");
    }
    
    public static void coreElementAR1 (String fileName) {
    	System.out.println("Running coreElementDynamic1.");
    	try {
			FileOutputStream fos = new FileOutputStream(fileName, true);
			fos.write(1);
			fos.close();
    	} catch(IOException ioe) {
			ioe.printStackTrace();
			System.exit(-1);
    	}
    }
    
    public static void coreElementAR2 (String fileName) {
    	System.out.println("Running coreElementDynamic2.");
    	try {
			FileInputStream fis = new FileInputStream(fileName);
			System.out.println("--Value: " + String.valueOf(fis.read()));
			fis.close();
    	} catch(IOException ioe) {
			ioe.printStackTrace();
			System.exit(-1);
    	}
    }
}
