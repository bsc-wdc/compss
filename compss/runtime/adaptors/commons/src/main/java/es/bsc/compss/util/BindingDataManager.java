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
package es.bsc.compss.util;

import java.nio.ByteBuffer;


public class BindingDataManager {

    public native static boolean isInBinding(String id);

    public native static int removeData(String id);

    public native static int copyCachedData(String from_id, String to_id);

    public native static int moveCachedData(String from_id, String to_id);

    public native static int storeInFile(String id, String filepath);

    public native static int loadFromFile(String id, String filepath, int type, int elements);

    public native static ByteBuffer getByteArray(String id);

    public native static int setByteArray(String id, ByteBuffer b, int type, int elements);


    static {
        System.loadLibrary("bindings_common");
    }
}
