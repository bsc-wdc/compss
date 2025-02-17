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
package es.bsc.compss.util.serializers;

import es.bsc.compss.types.exceptions.NonInstantiableException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;


/**
 * The serializer class is an utility to Serialize and deserialize objects passed as a parameter of a remote task.
 */
public class BinarySerializer {

    /**
     * Private constructor to avoid instantiation.
     */
    private BinarySerializer() {
        throw new NonInstantiableException("Serializer");
    }

    /**
     * Serializes an objects and leaves it in a file.
     *
     * @param o Object to be serialized.
     * @param file File where the serialized object will be stored.
     * @throws IOException Error writing the file.
     */
    public static void serialize(Object o, String file) throws IOException {
        FileOutputStream fout = new FileOutputStream(file);
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(o);
        oos.close();
    }

    /**
     * Serializes an objects as a byte[].
     *
     * @param o Object to be serialized.
     * @throws IOException Error writing the file.
     */
    public static byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(o);
            out.flush();
            return bos.toByteArray();
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }

    }

    /**
     * Reads an object from a file.
     *
     * @param file File containing the serialized object.
     * @return The object read from the file.
     * @throws IOException Error reading the file.
     */
    public static Object deserialize(String file) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(file);
        ObjectInputStream ois = new ObjectInputStream(fis);
        Object o = ois.readObject();
        ois.close();
        return o;
    }

    /**
     * Reads an object from a byte array.
     *
     * @param data Byte array containing the serialized object.
     * @return The object read from the file.
     * @throws IOException Error reading the file.
     */
    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = null;

        try {
            in = new ObjectInputStream(bis);
            return in.readObject();
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }
}
