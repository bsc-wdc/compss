/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package matmul.input.files;


public class MatmulImpl {
	
	public static void multiplyAccumulative( String f3, String f1, String f2 ) {
		Block a = new Block( f1 );
		Block b = new Block( f2 );
		Block c = new Block( f3 );
		
		c.multiplyAccum( a, b );
		c.blockToDisk( f3 );
	}
	
}
