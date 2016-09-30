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
public class SumPoints implements Serializable {
	private int k;
	private int dim;
	private PairF[] pairs;
    
	/* Default constructor */
	public SumPoints(){
		this.k = 0;
		this.dim = 0;
		this.pairs = null;
	}
	
    public SumPoints(int k, int dim) {
    	this.k = k;
    	this.dim = dim;
    	this.pairs = new PairF[k];
    	for (int i=0; i<k; i++){
    		this.pairs[i] = new PairF(i, dim);
    	}
    }
    
    public SumPoints(PairF[] pairs){
    	this.pairs = pairs;
    }
    
    public void sumValue(int id, float[] value, int numPoints){
    	this.pairs[id].sumValue(value, numPoints);
    }
    
    public int getK() {
    	return this.k;
    }
    
    public int getDim() {
    	return this.dim;
    }
    
    public PairF[] getPairs() {
    	return this.pairs;
    }
    
    public float[] getValue(int id){
    	return this.pairs[id].getSum();
    }
    
    public int getNumPoints(int id){
    	return this.pairs[id].getNumPoints();
    }
    
    public int getSize(){
    	return this.pairs.length;
    }
    
    public void setK (int k) {
    	this.k = k;
    }
    
    public void setDim (int dim) {
    	this.dim = dim;
    }
    
    public void setPairs (PairF[] pairs) {
    	this.pairs = pairs;
    }
    
    public Fragment normalize(){
    	float[][] f = new float[k][dim];
    	for (int i=0; i< this.pairs.length; i++){
    		int numPoints = this.pairs[i].getNumPoints();
    		float[] values = this.pairs[i].getSum();
    		for(int j=0; j < values.length; j++){
    			f[i][j] = values[j] / numPoints;
    		}
    	}
    	Fragment norm = new Fragment(f);
    	return norm;
    }
      
}
