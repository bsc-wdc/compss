package cbm1;

import java.util.Random;

public class Cbm1Impl 
{
	public static void computeSleep()
	{
		double x = new Random().nextDouble();
		//for(int j = 0; j < 100; ++j)
		{	
			for(int i = 0; i < 100/*00000*/; ++i) {
				x = Math.atan(Math.sqrt(Math.pow(x, 10)));
			}
		}
	}
	
	public static String runTaskI(int a)
	{
		computeSleep();
		return new String("Wololo");
	}
}
