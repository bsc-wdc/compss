/*
 *  Copyright 2002-2015 Barcelona Supercomputing Center (www.bsc.es)
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
 */
package kmeans_frag;

import java.util.LinkedList;
import java.util.Random;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class KMeans_frag {
	
	private static void usage() {
		System.out.println("    Usage: kmeans_frag.KMeans_frag");
	}
    

	public static void main(String[] args) {
    	// Default values
        int K = 4;					// k
        double epsilon = 1e-4;		// criterio de convergencia
        int iterations = 50;		// maxIterations
        int nPoints = 2000;			// numV
        int nDimensions = 2;
        int nFrags = 2;				// numFrag
        int argIndex = 0;
        //String datasetPath = "";	// p

        // Get and parse arguments
        while (argIndex < args.length) {
            String arg = args[argIndex++];
            if (arg.equals("-c")) {
                K = Integer.parseInt(args[argIndex++]);   
            } else if (arg.equals("-i")) {
                iterations = Integer.parseInt(args[argIndex++]);
            } else if (arg.equals("-n")) {
            	nPoints = Integer.parseInt(args[argIndex++]);
            } else if (arg.equals("-d")) {
            	nDimensions = Integer.parseInt(args[argIndex++]);
            } else if (arg.equals("-f")) {
            	nFrags = Integer.parseInt(args[argIndex++]);
            //} else if (arg.equals("-p")) {
            // datasetPath = args[argIndex++];
            } else {
            	// WARN: Disabled
            	System.err.print("ERROR: Bad parameter");
            	usage();
            	System.exit(1);
            }
        }

        System.out.println("-----------------------------------");
        System.out.println("KMeans with random generated points");
        System.out.println("-----------------------------------");
        System.out.println("Running with the following parameters:");
        System.out.println("- Clusters: " + K);
        System.out.println("- Iterations: " + iterations);
        System.out.println("- Points: " + nPoints);
        System.out.println("- Dimensions: " + nDimensions);
        System.out.println("- Fragments: " + nFrags);
        //System.out.println("- Dataset path: " + datasetPath);

        /*
        // Load data
        File path = new File(datasetPath);
        File [] files = path.listFiles();
        String[] fragments_files = new String[files.length];
        for (int i = 0; i < files.length; i++){
            if (files[i].isFile()){ //this line weeds out other directories/folders
                //System.out.println(files[i]);
            	fragments_files[i] = files[i].toString();
            }
        }
        
        // Check num files equal to given fragments
        assert(fragments_files.length == nFrags);
        */
        
        // KMeans execution
        long startTime = System.currentTimeMillis();
        computeKMeans(nPoints, K, epsilon, iterations, nFrags, "random", nDimensions);
        long estimatedTime = System.currentTimeMillis() - startTime;
        
        // END
        System.out.println("-- END --");
        System.out.println("Elapsed time: " + estimatedTime);
    }
	
	
    private static void computeKMeans(int numV, int k, double epsilon, int maxIterations, 
    	int numFrag, String initMode, int nDimensions) {
    	
    	// Read 1 file of data per task
    	Fragment[] dataSet = new Fragment[numFrag];
    	int sizeFrag = numV / numFrag;
    	for(int i = 0; i < numFrag; i++) {
    		System.out.println("Fragment: " + i);
    		//dataSet[i] = readDatasetFromFile(fragments_files[i], sizeFrag, nDimensions);        // task
    		dataSet[i] = generateFragment(sizeFrag, nDimensions);                                 // task
    	}
    	
		// First centers
		//Random rand = new Random(5);
		//int ind = rand.nextInt(numFrag - 1);
		//Fragment mu = init_random(dataSet[ind], k, nDimensions);                // primeros centros desde un fragmento aleatorio
    	Fragment mu = generateFragment(k, nDimensions);                           // primeros centros aleatorios

    	// Converge
    	Fragment oldmu = null;
    	int n = 0;
    	while (!has_converged(mu, oldmu, epsilon, n, maxIterations)) {
    		oldmu = mu;
    		Clusters[] clusters = new Clusters[numFrag];       	// key: indiceCluster - value: lista de posiciones de los puntos 
    		SumPoints[] partialResult = new SumPoints[numFrag]; // key: indiceCluster - value: tupla(numPuntosCluster, sumaDeTodosLosPuntosCluster) 
    		for (int f = 0; f < numFrag; f++){
    			clusters[f] = clusters_points_partial(dataSet[f], mu, k, sizeFrag*f);
    			partialResult[f] = partial_sum(dataSet[f], clusters[f], k, sizeFrag*f);
    		}
    		
    		// MERGE-REDUCE
    		LinkedList<Integer> q = new LinkedList<Integer>();
        	for (int i = 0; i < numFrag; i++){
        		q.add(i);
        	}
        	
        	int x = 0;
        	while (!q.isEmpty()){
        		x = q.poll();
        		int y;
        		if (!q.isEmpty()) {
        			y = q.poll();
        			partialResult[x] = reduceCentersTask(partialResult[x], partialResult[y]);
        			q.add(x);
        		}
        	}
    		
        	// NORMALIZE
        	mu = partialResult[0].normalize();
    	
    		++n;
    	}    	
    }
    
    /*
    // @task
    public static Fragment readDatasetFromFile(String path, int numV, int nDimensions) {
    	System.out.println("* Task Parameters:");
    	System.out.println("  - PATH: " + path);
    	System.out.println("  - numV: " + numV);
    	System.out.println("  - nDimensions: " + nDimensions);
    	
    	float[][] points = new float[numV][nDimensions];
    	
    	FileReader fr = null;
    	BufferedReader br = null;
    	try {
    		fr = new FileReader(path);
    		br = new BufferedReader(fr);
    		
    	    int v = 0;
    	    while (v < numV) {
    	        String line = br.readLine();
    	        String[] values = line.split(" ");
    	        for (int i = 0; i < values.length; i++){
    	        	points[v][i] = Float.valueOf(values[i]);
    	        }
    	        v = v + 1;
    	    }
    	} catch (IOException e){
    		e.printStackTrace();
    	} finally {
    		if (fr != null) {
    			try {
    				fr.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    		if (br != null) {
    			try {
    				br.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}
    	
    	return new Fragment(points);
    }
    */
    
    /*
	private static Fragment init_random(String file, int k, int nDimensions){
		System.out.println("* Retrieving mu :");
    	System.out.println("  - PATH: " + file);
    	System.out.println("  - k: " + k);
    	System.out.println("  - nDimensions: " + nDimensions);
        float[][] mu = new float[k][nDimensions];
    	
    	FileReader fr = null;
    	BufferedReader br = null;
    	try {
    		fr = new FileReader(file);
    		br = new BufferedReader(fr);
    		
    	    int v = 0;
    	    while (v < k) {
    	        String line = br.readLine();
    	        String[] values = line.split(" ");
    	        for (int i = 0; i < values.length; i++){
    	        	mu[v][i] = Float.valueOf(values[i]);
    	        }
    	        v = v + 1;
    	    }
    	} catch (IOException e){
    		e.printStackTrace();
    	} finally {
    		if (fr != null) {
    			try {
    				fr.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    		if (br != null) {
    			try {
    				br.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}
//    	for(int i=0;i<k;i++){
//    		for(int j=0;j<nDimensions;j++){
//    			System.out.print(mu[i][j]);
//    		}
//    		System.out.println();
//    	}
    	return new Fragment(mu);
//    	Fragment mu = new Fragment (k, nDimensions);
//    	int v = 0;
//    	while (v < k){
//    		for (int i = 0; i < nDimensions; i++){
//	        	mu.setPoint(points.getPoint(v, i), v, i);
//	        }
//	        v = v + 1;
//    	}
//    	return mu;
    }
	*/
    
    // @task
    public static Fragment generateFragment(int numV, int nDimensions) {
    	System.out.println("* Task Parameters:");
    	System.out.println("  - numV: " + numV);
    	System.out.println("  - nDimensions: " + nDimensions);
    	
    	float[][] points = new float[numV][nDimensions];
    	Random random = new Random();
    	
    	for(int i=0;i<numV;i++){
    		for(int j=0;j<nDimensions;j++){
    			// Random between [-1,1)
    			points[i][j] = random.nextFloat() * (1 - (-1)) - 1;
    		}
    	}
    	return new Fragment(points);
    }
    
    private static Fragment init_random(Fragment points, int k, int nDimensions){
		System.out.println("* Retrieving mu :");
    	System.out.println("  - k: " + k);
    	System.out.println("  - nDimensions: " + nDimensions);
        float[][] mu = new float[k][nDimensions];
    	
  	    int v = 0;
   	    while (v < k) {
   	        for (int i = 0; i < nDimensions; i++){
   	        	mu[v][i] = points.getPoint(v, i);
   	        }
   	        v = v + 1;
   	    }
    	return new Fragment(mu);
    }

	
    private static boolean has_converged (Fragment mu, Fragment oldmu, double epsilon, int n, int maxIterations){
	   	System.out.println("iter: " + n);
	   	System.out.println("maxIterations: " + maxIterations);
	   	
	   	if (oldmu == null) {
	   		return false;
	   	} else if (n >= maxIterations) {
	   		return true;
	   	} else {
			float aux = 0;
			for (int k = 0; k < mu.getVectors(); k++) {              // recorrer cada centro
				float dist = 0;
				for (int dim = 0; dim < mu.getDimensions(); dim++) { 	// recorrer cada dimension de un centro
					float tmp = oldmu.getPoint(k, dim) - mu.getPoint(k, dim) ;
					dist += tmp*tmp;
				}
				aux += dist;
			}
			if (aux < epsilon*epsilon) {
				System.out.println("Distancia_T: " + aux);
				return true;
			} else {
				System.out.println("Distancia_F: " + aux);
				return false;
			}
	   	}
   }
    
	// @task
	public static Clusters clusters_points_partial(Fragment points, Fragment mu, int k, int ind) {
		Clusters clustersOfFrag = new Clusters(k);
		
		int numDimensions = points.getDimensions();
		for (int p = 0; p < points.getVectors(); p++){
			int closest = -1;
            float closestDist = Float.MAX_VALUE;
			for(int m = 0; m < mu.getVectors(); m++){
				float dist = 0;
                for (int dim = 0; dim < numDimensions; dim++) {
                    float tmp = points.getPoint(p, dim) - mu.getPoint(m, dim);
                    dist += tmp*tmp;
                }
                if (dist < closestDist) {
                    closestDist = dist;
                    closest = m; // cluster al que pertenece
                }
			}
			int value = ind + p;
			clustersOfFrag.addIndex(closest, value);
		}
		
		return clustersOfFrag;
	}
	
	// @task
	public static SumPoints partial_sum (Fragment points, Clusters cluster, int k, int ind) {
		SumPoints pSum = new SumPoints(k, points.getDimensions());
		for (int c = 0; c < cluster.getSize(); c++) {  // en realidad cluster.getSize = k
			int[] positionValues = cluster.getIndexes(c); 
			for (int i = 0; i < cluster.getIndexesSize(c); i++) {
				int value = positionValues[i];
				float[] v = points.getVector(value-ind);
				pSum.sumValue(c, v, 1);
			}
		}
		return pSum;
	}
	
	/*
	private static SumPoints merge_reduce (SumPoints[] data){
    	LinkedList<SumPoints> q = new LinkedList<SumPoints>();
    	for (int i = 0; i < data.length; i++){
    		q.add(data[i]);
    	}
    	
    	while (!q.isEmpty()){
    		SumPoints x = q.poll();
    		SumPoints y = null;
    		if (!q.isEmpty()) {
    			y = q.poll();
    			q.add(reduceCentersTask(x, y));
    		} else {
    			return x;
    		}
    	}
    	
    	return null;
    }
	*/
	
	// @task
	//public static SumPoints reduceCentersTask(SumPoints a, SumPoints b){
	public static SumPoints reduceCentersTask(SumPoints a, SumPoints b){
		for (int i = 0; i < b.getSize(); i++){
			a.sumValue(i, b.getValue(i), b.getNumPoints(i));
		}
		
		return a;
	}
	

}
