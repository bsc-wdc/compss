package testNonExistingInFile;


public class Main 
{
	public static void main(String[] args)
	{
		Dummy dummy = errorTask("./fakeFile.txt");
		System.out.println("Finished task (" + dummy + ")");
	}
	
	public static Dummy errorTask(String file)
	{
		try { Thread.sleep(2000); } catch(Exception e) {}
		return new Dummy();
	}
}

