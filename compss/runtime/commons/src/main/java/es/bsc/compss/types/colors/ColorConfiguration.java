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
package es.bsc.compss.types.colors;

import es.bsc.compss.types.exceptions.NonInstantiableException;


public class ColorConfiguration {

    /**
     * Array containing the Paraver task colors.
     */
    private static final ColorNode[] COLORS = new ColorNode[] { // Paraver colors
        new ColorNode(Colors.COLOR_0, Colors.WHITE), // Paraver 0
        new ColorNode(Colors.COLOR_1, Colors.WHITE), // Paraver 1
        new ColorNode(Colors.COLOR_2, Colors.BLACK), // Paraver 2
        new ColorNode(Colors.COLOR_3, Colors.BLACK), // Paraver 3
        new ColorNode(Colors.COLOR_4, Colors.BLACK), // Paraver 4
        new ColorNode(Colors.COLOR_5, Colors.WHITE), // Paraver 5
        new ColorNode(Colors.COLOR_6, Colors.BLACK), // Paraver 6
        new ColorNode(Colors.COLOR_7, Colors.BLACK), // Paraver 7
        new ColorNode(Colors.COLOR_8, Colors.WHITE), // Paraver 8
        new ColorNode(Colors.COLOR_9, Colors.WHITE), // Paraver 9
        new ColorNode(Colors.COLOR_10, Colors.BLACK), // Paraver 10
        new ColorNode(Colors.COLOR_11, Colors.BLACK), // Paraver 11
        new ColorNode(Colors.COLOR_12, Colors.BLACK), // Paraver 12
        new ColorNode(Colors.COLOR_13, Colors.WHITE), // Paraver 13
        new ColorNode(Colors.COLOR_14, Colors.BLACK), // Paraver 14
        new ColorNode(Colors.COLOR_15, Colors.BLACK), // Paraver 15
        new ColorNode(Colors.COLOR_16, Colors.WHITE), // Paraver 16
        new ColorNode(Colors.COLOR_17, Colors.WHITE), // Paraver 17
        new ColorNode(Colors.COLOR_18, Colors.BLACK), // Paraver 18
        new ColorNode(Colors.COLOR_19, Colors.WHITE), // Paraver 19
        new ColorNode(Colors.COLOR_20, Colors.BLACK), // Paraver 20
        new ColorNode(Colors.COLOR_21, Colors.WHITE), // Paraver 21
        new ColorNode(Colors.COLOR_22, Colors.BLACK), // Paraver 22
        new ColorNode(Colors.COLOR_23, Colors.WHITE) // Paraver 23
    };

    /**
     * Total number of colors.
     */
    public static final int NUM_COLORS = getColors().length;


    /**
     * Private constructor to avoid instantiation.
     */
    private ColorConfiguration() {
        throw new NonInstantiableException("ColorConfiguration should not be instantiated");
    }

    public static ColorNode[] getColors() {
        return COLORS;
    }

}
