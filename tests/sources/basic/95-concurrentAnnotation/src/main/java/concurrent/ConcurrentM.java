package concurrent;

import java.io.FileOutputStream;
import java.io.IOException;
import es.bsc.compss.api.COMPSs;

public class ConcurrentM {

	public static int MAX_AVAILABLE = 1;
	public static String fileName = "text.txt";

	public static void main(String[] args) throws InterruptedException, IOException {
		int number;

		System.out.println("START");
		
		File file = new File(fileName);
		file.mkdirs();
		if (file.exists()) {
		    file.delete();
		}
		
		file.createNewFile();
		
		new File(fileName);
		for (int i = 0; i < 15; i++) {
			ConcurrentMImpl.plusone(fileName);
		}

		COMPSs.barrierConcurrent();
	}
}
