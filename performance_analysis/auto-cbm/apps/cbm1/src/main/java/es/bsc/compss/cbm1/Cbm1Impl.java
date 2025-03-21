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
package es.bsc.compss.cbm1;

import java.util.Random;


public class Cbm1Impl {

    /**
     * Fake sleep task.
     * 
     * @param a Integer parameter.
     * @return String return value.
     */
    public static String runTaskI(int a) {
        computeSleep();
        return new String("Wololo");
    }

    private static void computeSleep() {
        double x = new Random().nextDouble();
        // for(int j = 0; j < 100; ++j)
        {
            for (int i = 0; i < 100/* 00000 */; ++i) {
                x = Math.atan(Math.sqrt(Math.pow(x, 10)));
            }
        }
    }
}
