/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package recursive;

/**
 * Application counting from 1 to N.
 */
public class Count {

    /**
     * Function that recursively counts from 1 to N.
     *
     * @param in upper boundary of the count
     * @return String containing all the numbers from 1 to N.
     */
    public static Appender count(CountObject in) {
        int N = in.getCount();
        System.out.println("Count(" + N + ")");

        Appender app = new Appender();
        if (N > 1) {
            app = count(new CountObject(N - 1));
        }

        String str = app.getStr() + N;
        System.out.println("Appender(" + str + ")");
        return new Appender(str);
    }

    /**
     * Main method of the application.
     *
     * @param args Only takes into account the first argument of the array (the upper boundary of the count)
     */
    public static void main(String[] args) throws Exception {
        int K = Integer.parseInt(args[0]);
        System.out.println("Count(" + K + ")");

        Appender app = new Appender();
        if (K > 1) {
            app = count(new CountObject(K - 1));
        }

        String str = app.getStr() + K;
        System.out.println("Appender(" + str + ")");
    }

}
