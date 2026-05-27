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
package es.bsc.compss.semantics.data.access;

/**
 * Describes how an access operates on a value.
 * <p>
 * This enumeration defines the access semantics of a parameter or data item, indicating whether it is read, written, or
 * both, as well as the constraints on concurrent or commutative updates. These modes are used by the runtime to
 * determine data dependencies, synchronization requirements, and execution ordering.
 * </p>
 */
public enum AccessMode {

    READ(0, true, false, false), //
    GENERATE(1, false, true, false), //
    UPDATE(2, true, true, false), //
    CONCURRENT_UPDATE(3, true, false, false), // Read-write concurrent
    COMMUTATIVE_UPDATE(4, true, true, false), // Read-write commutative
    READ_AND_DELETE(5, true, false, true); // Read one-use


    private static final AccessMode[] LOOKUP;

    static {
        LOOKUP = new AccessMode[256];

        for (AccessMode t : AccessMode.values()) {
            int idx = t.id & 0xFF;

            if (LOOKUP[idx] != null) {
                throw new IllegalStateException("Duplicate AccessMode id detected: " + idx);
            }

            LOOKUP[idx] = t;
        }
    }

    private final byte id;
    private final boolean read;
    private final boolean write;
    private final boolean delete;


    AccessMode(int id, boolean read, boolean write, boolean delete) {
        this.id = (byte) id;
        this.read = read;
        this.write = write;
        this.delete = delete;
    }

    public final boolean isRead() {
        return this.read;
    }

    public boolean isWrite() {
        return this.write;
    }

    public boolean isDelete() {
        return this.delete;
    }

    /**
     * Returns the AccessMode corresponding to the given type id.
     *
     * @param b encoded AccessMode identifier
     * @return corresponding AccessMode object
     */
    public static AccessMode fromByte(byte b) {
        AccessMode t = LOOKUP[b & 0xFF];
        if (t == null) {
            throw new IllegalArgumentException("Invalid AccessMode id: " + b);
        }
        return t;
    }

    /**
     * Returns the encoded byte identifier of this AccessMode.
     *
     * @return encoded AccessMode identifier as a byte
     */
    public byte toByte() {
        return id;
    }
}
