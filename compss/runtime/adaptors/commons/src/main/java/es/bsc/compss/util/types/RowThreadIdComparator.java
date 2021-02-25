/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.util.types;

import es.bsc.compss.log.Loggers;

import java.util.Comparator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RowThreadIdComparator implements Comparator<String> {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);


    /**
     * Compares two threads Ids being in a string formated like "label (X:X:X)" .
     */
    public int compare(String a, String b) {
        if (a.startsWith("THREAD ")) {
            a = a.substring(7, a.length());
        } else {
            a = a.substring(a.indexOf("(") + 1, a.indexOf(")"));
        }

        if (b.startsWith("THREAD ")) {
            b = b.substring(7, b.length());
        } else {
            b = b.substring(b.indexOf("(") + 1, b.indexOf(")"));
        }

        String[] valuesA = a.split("\\.");
        String[] valuesB = b.split("\\.");
        for (int i = 0; i < 3; i++) {
            int intA = Integer.parseInt(valuesA[i]);
            int intB = Integer.parseInt(valuesB[i]);
            if (intA > intB) {
                return 1;
            }
            if (intA < intB) {
                return -1;
            }
        }
        return 0;
    }
}
