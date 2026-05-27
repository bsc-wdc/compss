/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.semantics.task.parameter;

/**
 * Stream types.
 */
public enum StdIOStream {

    STDIN(0), //
    STDOUT(1), //
    STDERR(2), //
    UNSPECIFIED(3);


    public final byte id;


    StdIOStream(int id) {
        this.id = (byte) id;
    }


    private static final StdIOStream[] LOOKUP;

    static {
        LOOKUP = new StdIOStream[256];

        for (StdIOStream t : StdIOStream.values()) {
            int idx = t.id & 0xFF;

            if (LOOKUP[idx] != null) {
                throw new IllegalStateException("Duplicate StdIOStream id detected: " + idx);
            }

            LOOKUP[idx] = t;
        }
    }


    /**
     * Returns the StdIOStream corresponding to the given type id.
     *
     * @param b encoded StdIOStream identifier
     * @return corresponding StdIOStream object
     */
    public static StdIOStream fromByte(byte b) {
        StdIOStream t = LOOKUP[b & 0xFF];
        if (t == null) {
            throw new IllegalArgumentException("Invalid StdIOStream id: " + b);
        }
        return t;
    }

    /**
     * Returns the encoded byte identifier of this StdIOStream.
     *
     * @return encoded StdIOStream identifier as a byte
     */
    public byte toByte() {
        return id;
    }
}
