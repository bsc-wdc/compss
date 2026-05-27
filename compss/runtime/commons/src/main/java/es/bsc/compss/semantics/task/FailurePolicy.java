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
package es.bsc.compss.semantics.task;

/**
 * On failure behavior.
 */
public enum FailurePolicy {

    RETRY(0), // Task will be resubmitted when failed
    FAIL(1), // Execution will stop when a task fails
    IGNORE(2), // Execution continues ignoring task + dependents
    CANCEL_SUCCESSORS(3); // Successors will not be executed


    public final byte id;


    FailurePolicy(int id) {
        this.id = (byte) id;
    }


    private static final FailurePolicy[] LOOKUP;

    static {
        LOOKUP = new FailurePolicy[256];

        for (FailurePolicy t : FailurePolicy.values()) {
            int idx = t.id & 0xFF;

            if (LOOKUP[idx] != null) {
                throw new IllegalStateException("Duplicate OnFailure id detected: " + idx);
            }

            LOOKUP[idx] = t;
        }
    }


    /**
     * Returns the OnFailure corresponding to the given type id.
     *
     * @param b encoded OnFailure identifier
     * @return corresponding OnFailure object
     */
    public static FailurePolicy fromByte(byte b) {
        FailurePolicy t = LOOKUP[b & 0xFF];
        if (t == null) {
            throw new IllegalArgumentException("Invalid OnFailure id: " + b);
        }
        return t;
    }

    /**
     * Returns the encoded byte identifier of this OnFailure.
     *
     * @return encoded OnFailure identifier as a byte
     */
    public byte toByte() {
        return id;
    }
}
