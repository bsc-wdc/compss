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
package es.bsc.compss.data;

import java.nio.ByteBuffer;


public class BindingDataManager {

    public static native boolean isInBinding(String id);

    public static native int removeData(String id);

    public static native int copyCachedData(String fromId, String toId);

    public static native int moveCachedData(String fromId, String toId);

    public static native int storeInFile(String id, String filepath);

    public static native int loadFromFile(String id, String filepath, int type, int elements);

    public static native ByteBuffer getByteArray(String id);

    public static native int setByteArray(String id, ByteBuffer b, int type, int elements);


    static {
        System.loadLibrary("bindings_common");
    }
}
