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

import java.io.Serializable;


/**
 * A class to encapsulate an input set of 
 * Points for use in the KMeans program and the 
 * reading/writing of a set of points to data files.
 */
public class Clusters implements Serializable {
	private Pair[] cluster;
    
	/* Default constructor */
	public Clusters() {
	}
	
	public Pair[] getCluster() {
		return this.cluster;
	}
	
	public void setCluster(Pair[] cluster) {
		this.cluster = cluster;
	}
	
    public Clusters(int k) {
    	// K = number of centers
    	this.cluster = new Pair[k];
    	// Initialize each Pair with its id.
    	for(int i=0; i < k; i++){
    		this.cluster[i] = new Pair(i);
    	}
    }
    
    public void addIndex(int id, int index){
    	this.cluster[id].addValue(index);
    }
    
    public int[] getIndexes(int id){
    	return this.cluster[id].getIndexes();
    }
    
    public int getSize(){
    	return this.cluster.length;
    }
    
    public int getIndexesSize(int id){
    	return this.cluster[id].getSize();
    }
    
}
