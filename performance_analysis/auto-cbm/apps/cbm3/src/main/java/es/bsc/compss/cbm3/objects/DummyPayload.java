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
package es.bsc.compss.cbm3.objects;

import java.io.Serializable;
import java.util.Random;


/**
 * This is a class that contains an array of bytes just made it to be sure COMPSs can pass it by reference n stuff.
 */
public class DummyPayload implements Serializable {

    /**
     * SerialId for objects outside the runtime.
     */
    private static final long serialVersionUID = 3L;

    private int size;
    private byte[] payload;


    /**
     * Creates a new DummyPayload instance.
     */
    public DummyPayload() {
        this.size = 1;
        this.payload = new byte[1];
    }

    /**
     * Creates a new DummyPayload instance with the given in size.
     * 
     * @param sizeInBytes In size.
     */
    public DummyPayload(int sizeInBytes) {
        regen(sizeInBytes);
    }

    /**
     * Returns the size of the payload.
     * 
     * @return The size of the payload.
     */
    public int getSize() {
        return this.size;
    }

    /**
     * Generates a new size.
     * 
     * @param sizeInBytes New size.
     */
    public void regen(int sizeInBytes) {
        this.size = sizeInBytes;
        this.payload = new byte[sizeInBytes];
        new Random().nextBytes(this.payload); // fill with random bytes
    }

    /**
     * Foo function for synchronization.
     */
    public void foo() {
        // To sync
        this.payload[0] = 5;
    }

}
