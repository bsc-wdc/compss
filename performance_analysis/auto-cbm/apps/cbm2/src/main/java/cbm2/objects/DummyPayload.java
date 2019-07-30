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
package cbm2.objects;

import java.io.Serializable;
import java.util.Random;


//This is a class that contains an array of bytes
//Just made it to be sure COMPSs can pass it by reference n stuff
public class DummyPayload implements Serializable {

    /**
     * SerialId for objects outside the runtime
     */
    private static final long serialVersionUID = 3L;

    public int size;
    private byte[] payload;


    public DummyPayload() {
        size = 1;
        payload = new byte[1];
    }

    public DummyPayload(int sizeInBytes) {
        regen(sizeInBytes);
    }

    public void regen(int sizeInBytes) {
        size = sizeInBytes;
        payload = new byte[sizeInBytes];
        new Random().nextBytes(payload); // fill with random bytes
    }

    public void foo() {
        // For sync
    }

}
