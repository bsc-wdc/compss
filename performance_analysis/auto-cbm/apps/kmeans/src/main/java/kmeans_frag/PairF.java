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
public class PairF implements Serializable {
	private int id;
	private int numPoints;  // amount of points that have been sum
	private float[] sum;
    
	/* Default constructor */
	public PairF(){
		this.id = 0;
		this.numPoints = 0;
		this.sum = null;
	}
	
    public PairF(int id, int dim) {
        this.id = id;
        this.numPoints = 0;
        this.sum = new float[dim];
        for(int i=0; i<dim; i++){
        	this.sum[i] = 0;
        }
    }
       
    public int getId () {
    	return this.id;
    }
    
    public int getNumPoints(){
    	return this.numPoints;
    }

    public float[] getSum () {
    	return this.sum;
    }
    
    public float getValue (int dim) {
    	assert (dim >= 0 && dim < this.sum.length);
    	
    	return this.sum[dim];
    }

    public void setId (int id) {
    	this.id = id;
    }
    
    public void setNumPoints(int numPoints){
    	this.numPoints = numPoints;
    }
    
    public void setSum (float[] sum) {
    	this.sum = sum;
    }
        
    public void sumValue (float[] value, int numPoints) {
    	assert (value.length == this.sum.length);
    	for (int i=0; i<this.sum.length; i++){
    		this.sum[i] += value[i];
    	}
    	this.numPoints += numPoints;
    }
    
}
