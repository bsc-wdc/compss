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
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void write_two(String fileName) {
        try {
            FileOutputStream fos = new FileOutputStream(fileName, true);
            fos.write(2);
            fos.close();
            Thread.sleep(2000);
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
