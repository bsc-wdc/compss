/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.colors;

import es.bsc.compss.types.exceptions.NonInstantiableException;
import es.bsc.compss.types.colors.Colors;


public class ColorConfiguration {

    public static final ColorNode[] COLORS = new ColorNode[] { 
            new ColorNode(Colors.COLOR_0, Colors.WHITE),
            new ColorNode(Colors.COLOR_1, Colors.WHITE), 
            new ColorNode(Colors.COLOR_2, Colors.BLACK),
            new ColorNode(Colors.COLOR_3, Colors.BLACK), 
            new ColorNode(Colors.COLOR_4, Colors.BLACK),
            new ColorNode(Colors.COLOR_5, Colors.WHITE), 
            new ColorNode(Colors.COLOR_6, Colors.BLACK),
            new ColorNode(Colors.COLOR_7, Colors.BLACK), 
            new ColorNode(Colors.COLOR_8, Colors.WHITE),
            new ColorNode(Colors.COLOR_9, Colors.WHITE), 
            new ColorNode(Colors.COLOR_10, Colors.BLACK),
            new ColorNode(Colors.COLOR_11, Colors.BLACK), 
            new ColorNode(Colors.COLOR_12, Colors.BLACK),
            new ColorNode(Colors.COLOR_13, Colors.WHITE), 
            new ColorNode(Colors.COLOR_14, Colors.BLACK),
            new ColorNode(Colors.COLOR_15, Colors.BLACK), 
            new ColorNode(Colors.COLOR_16, Colors.WHITE),
            new ColorNode(Colors.COLOR_17, Colors.WHITE), 
            new ColorNode(Colors.COLOR_18, Colors.BLACK),
            new ColorNode(Colors.COLOR_19, Colors.WHITE), 
            new ColorNode(Colors.COLOR_20, Colors.BLACK),
            new ColorNode(Colors.COLOR_21, Colors.WHITE), 
            new ColorNode(Colors.COLOR_22, Colors.BLACK),
            new ColorNode(Colors.COLOR_23, Colors.WHITE) 
        };

    public static final int NUM_COLORS = COLORS.length;


    private ColorConfiguration() {
        throw new NonInstantiableException("ColorConfiguration should not be instantiated");
    }

}
