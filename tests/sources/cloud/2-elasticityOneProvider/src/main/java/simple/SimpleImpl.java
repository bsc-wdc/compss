package simple;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;


public class SimpleImpl {

	public static void increment(String counterFile) {
		try	{
			FileInputStream fis = new FileInputStream(counterFile);
			int count = fis.read();
			fis.close();
			try {
				Thread.sleep(30_000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			FileOutputStream fos = new FileOutputStream(counterFile);
            		fos.write(++count);			
			fos.close();
		}
		catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
}
