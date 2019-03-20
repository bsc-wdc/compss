/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package cbm3.objects;

import java.lang.management.ManagementFactory;
import java.util.Random;

public class Cbm3 
{
	//Compss BenchMark to test mergesort-like apps performance.
	private static void usage()
	{
		System.out.println(":::: Usage: runcompss cbm3.objects.Cbm3 (deepness) (task_Sleep_Time) (txSizeInBytes) (INOUT | IN)::::");
		System.out.println("Exiting cbm3...!");
	}
	
    public static void main( String[] args )
    {
    	//Get args ////////////////////////////////////////
    	if(args.length < 4 || (!args[3].equals("INOUT") && !args[3].equals("IN"))) { usage(); return; }

    	int deepness = Integer.parseInt(args[0]);
    	int taskSleepTime = Integer.parseInt(args[1]);
    	int txSizeInBytes = Integer.parseInt(args[2]); //Size of the transference
    	boolean inout = args[3].equals("INOUT"); //INOUT or IN ???
    	
    	
    	System.out.println(":::::::::::");
    	System.out.println("Dependency graph deepness: {{" + deepness + "}}");
    	System.out.println("Tasks sleep time: {{" + taskSleepTime + "}}");
    	System.out.println("Transference size in bytes: {{" + txSizeInBytes + "}}");
    	System.out.println("Execution type (INOUT || IN): {{" + (inout ? "INOUT" : "IN") + "}}");
    	System.out.println("Execution type (FILES || OBJECTS): {{OBJECTS}}");
    	System.out.println(":::::::::::");
    	System.out.println("");
    	System.out.println(":::::::::::");
    	System.out.println("Starting cbm3 with objects...");
    	/////////////////////////////////////////////////////

    	deepness += 1; 
    	
    	//Create all the objects.
		System.out.println("Creating pool of objects...");
		
	    	int numTasks = (int) Math.pow(2, deepness-1);
	    	DummyPayload[] dummyPool = new DummyPayload[numTasks];
	    	for(int i = 0; i < numTasks; ++i) dummyPool[i] = new DummyPayload(txSizeInBytes);
	    	
    	System.out.println("Pool of objects created (" + dummyPool.length + " created)");
    	//////////////////////////////////////////////////////////
    	
    	System.out.println("From now on we start to measure time...");
    	System.out.println("Creating tasks...");
    	double compssTime = System.nanoTime(); //Start measuring time
    	//TASKS CREATION //////////////////////////////////////////////
    		int step = 1;
	    	for(int d = 0; d <= deepness; ++d)
	    	{
	    		for(int i = 0; i+step < numTasks; i+=step*2)
		    	{
	    			DummyPayload obj = dummyPool[i];
	    			if(inout) runTaskInOut(taskSleepTime, dummyPool[i], dummyPool[i + step]);
	    			else
    				{
	    				obj = runTaskIn(taskSleepTime, obj, dummyPool[i + step]);
		    			dummyPool[i] = obj;
    				}
		    	}
	    		step *= 2;
	    	}
    	/////////////////////////////////////////////////////
    	System.out.println("Tasks created in " + ((System.nanoTime() - compssTime) / 1000000) + " ms");
    	System.out.println("Waiting to sync...");
    	System.out.println("Sync: " + dummyPool[0]);
    	
    	System.out.println(":::::::::::");
    	System.out.println("cbm3 Finished!");
    	System.out.println("Time: {{" + ((System.nanoTime() - compssTime) / 1000000) + "}}");
    	System.out.println(":::::::::::");
    }

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
	
    public static DummyPayload runTaskIn(int sleepTime, DummyPayload objinLeft, DummyPayload objinRight)
    {
    	computeSleep(sleepTime);
		objinRight.regen(objinRight.size); 
		return objinLeft;
    }
    
    public static void runTaskInOut(int sleepTime, DummyPayload objinoutLeft, DummyPayload objinRight)
    {
    	computeSleep(sleepTime);
		objinoutLeft.regen(objinoutLeft.size); 
    }
}
