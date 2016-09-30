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
package matmul.randomGen.objects;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Random;


@SuppressWarnings("serial")
public class Block implements Serializable {
	private int M;
	private double[][] data;
	
	
	public Block() {	
	}
	
	public Block(int bSize) {
		M = bSize;
		data = new double[M][M];
		
		for (int i = 0 ; i < M; i++)
			for (int j = 0; j < M; j++)
				data[i][j] = 0.0;
	}
	
	public int getM() {
		return M;
	}
	
	public void setM(int i) {
		M = i;
	}
	
	public double[][] getData() {
		return data;
	}
	
	public void setData(double[][] d) {
		data = d;
	}
	
	public void printBlock() {
		for (int i = 0 ; i < M; i++) {
			for (int j = 0; j < M; j++) {
				System.out.print(data[i][j] + " ");
			}
		}
    	System.out.println("");
    }
	
	public void blockToDisk(int i, int j, String name) {
		FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(name + "." + i + "." + j);
            for (int k1 = 0; k1 < M; k1++) {
                for (int k2 = 0; k2 < M; k2++) {
                	String str = data[k1][k2] + " ";
                	fos.write(str.getBytes());
                }
                fos.write( "\n".getBytes() );
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	if (fos != null) {
        		try {
        			fos.close();
        		} catch (IOException e) {
        			e.printStackTrace();
        		}
        	}
        }
    }
	
	public void multiplyAccumulative(Block a, Block b) {
		for (int i = 0; i < M; i++) {
			for (int j = 0; j < M; j++) {
				for (int k = 0; k < M; k++) {
					data[i][j] += a.data[i][k]*b.data[k][j];
				}
			}
		}
	}
	
	public static Block initBlock(int M, int seed) {
    	Block block = new Block(M);
    	Random generator = new Random(seed);
    	
    	for (int i = 0; i < M; i++) {
			for (int j = 0; j < M; j++) {
				double value = (double)(generator.nextDouble()*10.0);
				block.data[i][j] = value;
			}
    	}
    	
		return block;
    }
	
}
