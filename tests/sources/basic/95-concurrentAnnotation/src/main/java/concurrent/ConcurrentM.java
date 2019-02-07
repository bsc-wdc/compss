package concurrent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ConcurrentM {

	public static int MAX_AVAILABLE = 1;
	public static String fileName = "/tmp/sharedDisk/file.txt";

	public static void main(String[] args) throws InterruptedException, IOException {

		System.out.println("START");
		
		File file = new File(fileName);
		file.mkdirs();
		if (file.exists()) {
		    file.delete();
		}
		
		file.createNewFile();
		
		new File(fileName);
		for (int i = 0; i < 3; i++) {
			ConcurrentMImpl.write_one(fileName);
		}
		
		for (int i=0; i < 3; i++) {
		    ConcurrentMImpl.write_two(fileName);
		}//COMPSs.barrierConcurrent();
        
		FileInputStream fis = new FileInputStream(fileName);
		int lines = 0;
		while (fis.read()>0) lines++;
        System.out.println("Final counter value is " + lines);
        fis.close();

	 
	}
}
