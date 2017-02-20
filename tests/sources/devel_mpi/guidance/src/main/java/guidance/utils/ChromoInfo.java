/** @file 
 *  Copyright 2002-2014 Barcelona Supercomputing Center (www.bsc.es)
 *  Life Science Department, 
 *  Computational Genomics Group (http://www.bsc.es/life-sciences/computational-genomics)
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
 *
 *  Last update: $LastChangedDate: 2015-01-07 17:47:11 +0100 (Wed, 07 Jan 2015) $
 *  Revision Number: $Revision: 12 $
 *  Last revision  : $LastChangedRevision: 12 $
 *  Written by     : Friman Sanchez C.
 *                 : friman.sanchez@gmail.com
 *  Modified by    :
 *                
 *  Guidance web page: http://cg.bsc.es/guidance/
 */

package guidance.utils;

public class ChromoInfo {

    public static final int MAX_NUMBER_OF_CHROMOSOMES = 23;
    public static final String MAX_NUMBER_OF_CHROMOSOMES_STR = "23";
    public static final int MAX_CHROMOSOME_LIMIT = 252_000_000;
    
    private int[] minSize = new int[MAX_NUMBER_OF_CHROMOSOMES];
    private int[] maxSize = new int[MAX_NUMBER_OF_CHROMOSOMES];


    public ChromoInfo() {
        // The maximum size (maxSize) of each chromosome is well known, but it does not have
        // a rule to automatically determine it, therefore, we put the values individually.

        maxSize[0] = 252_000_000;
        maxSize[1] = 246_000_000;
        maxSize[2] = 201_000_000;
        maxSize[3] = 192_000_000;
        maxSize[4] = 183_000_000;
        maxSize[5] = 174_000_000;
        maxSize[6] = 162_000_000;
        maxSize[7] = 147_000_000;
        maxSize[8] = 144_000_000;
        maxSize[9] = 138_000_000;
        maxSize[10] = 138_000_000;
        maxSize[11] = 135_000_000;
        maxSize[12] = 117_000_000;
        maxSize[13] = 108_000_000;
        maxSize[14] = 105_000_000;
        maxSize[15] = 93_000_000;
        maxSize[16] = 84_000_000;
        maxSize[17] = 81_000_000;
        maxSize[18] = 60_000_000;
        maxSize[19] = 66_000_000;
        maxSize[20] = 51_000_000;
        maxSize[21] = 54_000_000;
        maxSize[22] = 156_000_000;
    }

    /**
     * Method to access the minSize of a chromosome
     * 
     * @param chromoNumber
     * @return
     */
    public int getMinSize(int chromoNumber) {
        validateChormoNumber(chromoNumber);

        return minSize[chromoNumber - 1];
    }

    /**
     * Method to access the maxSize of a chromosome
     * 
     * @param chromoNumber
     * @return
     */
    public int getMaxSize(int chromoNumber) {
        validateChormoNumber(chromoNumber);

        return maxSize[chromoNumber - 1];
    }

    /**
     * Method to access biomart information
     * 
     * @param chromoNumber
     */
    public void printChromoInfo(int chromoNumber) {
        validateChormoNumber(chromoNumber);

        System.out.println("Gen file information for the chromosome " + chromoNumber);
        System.out.println("Max size     : " + maxSize[chromoNumber - 1]);
    }

    private void validateChormoNumber(int chromoNumber) {
        if (chromoNumber < 1 || chromoNumber > MAX_NUMBER_OF_CHROMOSOMES) {
            System.err.println("[chromoInfo] Errro, chromo " + chromoNumber + " does not exist");
            System.exit(1);
        }
    }
    
}
