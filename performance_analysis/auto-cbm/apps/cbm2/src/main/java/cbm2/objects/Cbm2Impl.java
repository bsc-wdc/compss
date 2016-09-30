package cbm2.objects;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

public class Cbm2Impl
{
	public static void computeSleep(int time)
	{
		long t = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId());
		while( (ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId()) - t) 
				/ 1000000 < time)
		{
			double x = new Random().nextDouble();
			for(int i = 0; i < 1000; ++i) {
				x = Math.atan(Math.sqrt(Math.pow(x, 10)));
			}
		}
	}
	
	public static void runTaskInOut(int sleepTime, DummyPayload dummyInOut)
	{
		computeSleep(sleepTime);
		//dummyInOut.regen(dummyInOut.size);
	}
	
	public static DummyPayload runTaskIn(int sleepTime, DummyPayload dummyIn)
	{
		computeSleep(sleepTime);
		return new DummyPayload(dummyIn.size);
	}
}