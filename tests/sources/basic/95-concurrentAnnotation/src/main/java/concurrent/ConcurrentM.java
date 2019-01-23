package concurrent;

import java.io.File;
import java.io.IOException;
import es.bsc.compss.api.COMPSs;

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
		for (int i = 0; i < 15; i++) {
			ConcurrentMImpl.write_one(fileName);
		}

		COMPSs.barrierConcurrent();
	}
}
