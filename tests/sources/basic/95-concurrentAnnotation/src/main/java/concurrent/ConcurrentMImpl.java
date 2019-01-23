package concurrent;

//import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class ConcurrentMImpl {
	public static void write_one(String fileName) {
		try {
			FileOutputStream fos = new FileOutputStream(fileName, true);
			fos.write(1);
			fos.close();
			Thread.sleep(2000);
		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
			System.exit(-1);
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(-1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
