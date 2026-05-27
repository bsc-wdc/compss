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
package es.bsc.compss.semantics.data;

/**
 * Parameter types (for bindings and internal java).
 */
public enum DataType {

    BOOLEAN_T(0), // Boolean
    CHAR_T(1), // Char
    BYTE_T(2), // Byte
    SHORT_T(3), // Short
    INT_T(4), // Int
    LONG_T(5), // Long
    FLOAT_T(6), // Float
    DOUBLE_T(7), // Double
    STRING_T(8), // String
    STRING_64_T(9), // String
    FILE_T(10), // File
    OBJECT_T(11), // Java Object
    PSCO_T(12), // Java PSCO
    EXTERNAL_PSCO_T(13), // Bindings PSCO
    BINDING_OBJECT_T(14), // Binding Object
    WCHAR_T(15), // Wchar
    WSTRING_T(16), // Wstring
    LONGLONG_T(17), // Long long
    VOID_T(18), // Void
    ANY_T(19), // Any
    ARRAY_CHAR_T(20), // Array Char
    ARRAY_BYTE_T(21), // Array byte
    ARRAY_SHORT_T(22), // Array short
    ARRAY_INT_T(23), // Array int
    ARRAY_LONG_T(24), // Array long
    ARRAY_FLOAT_T(25), // Array float
    ARRAY_DOUBLE_T(26), // Array double
    COLLECTION_T(27), // Collection
    DICT_COLLECTION_T(28), // Dictionary Collection
    STREAM_T(29), // Streams
    EXTERNAL_STREAM_T(30), // Binding Streams
    ENUM_T(31), // Enum
    NULL_T(32), // Null
    DIRECTORY_T(33); // Directory


    public final byte id;


    DataType(int id) {
        this.id = (byte) id;
    }


    private static final DataType[] LOOKUP;

    static {
        LOOKUP = new DataType[256];

        for (DataType t : DataType.values()) {
            int idx = t.id & 0xFF;

            if (LOOKUP[idx] != null) {
                throw new IllegalStateException("Duplicate DataType id detected: " + idx);
            }

            LOOKUP[idx] = t;
        }
    }


    /**
     * Returns the DataType corresponding to the given type id.
     *
     * @param b encoded DataType identifier
     * @return corresponding DataType object
     */
    public static DataType fromByte(byte b) {
        DataType t = LOOKUP[b & 0xFF];
        if (t == null) {
            throw new IllegalArgumentException("Invalid DataType id: " + b);
        }
        return t;
    }

    /**
     * Returns the encoded byte identifier of this DataType.
     *
     * @return encoded DataType identifier as a byte
     */
    public byte toByte() {
        return id;
    }
}
