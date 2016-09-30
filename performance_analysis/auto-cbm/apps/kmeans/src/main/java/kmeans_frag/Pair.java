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
import java.util.LinkedList;


/**
 * A class to encapsulate an input set of 
 * Points for use in the KMeans program and the 
 * reading/writing of a set of points to data files.
 */
public class Pair implements Serializable {
	private int id;
	private LinkedList<Integer> indexes;
    
	/* Default constructor */
	public Pair(){
		this.id = 0;
		this.indexes = new LinkedList<Integer>();
	}
	
    public Pair(int id) {
        this.id = id;
        this.indexes = new LinkedList<Integer>();
    }
    
    public int getId () {
    	return this.id;
    }
    
    public int getSize() {
    	return this.indexes.size();
    }

    public int[] getIndexes () {
    	int[] ret = new int[this.indexes.size()];
        int i = 0;
        for (Integer e : this.indexes)  
            ret[i++] = e.intValue();
        return ret;
    }
    
    public void setId (int id) {
    	this.id = id;
    }
    
    public void addValue (int index) {
    	this.indexes.add(index);
    }
    
    public void setIndexes (LinkedList<Integer> indexes) {
    	this.indexes = indexes;
    }

}
