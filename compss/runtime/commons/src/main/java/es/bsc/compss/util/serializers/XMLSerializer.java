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

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


/**
 * The serializer class is an utility to Serialize and deserialize objects passed as a parameter of a remote task.
 */
public class XMLSerializer {

    /**
     * Private constructor to avoid instantiation.
     */
    private XMLSerializer() {
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
        XMLEncoder e = new XMLEncoder(new BufferedOutputStream(fout));
        e.writeObject(o);
        e.close();
    }

    /**
     * Serializes an objects as a byte[].
     *
     * @param o Object to be serialized.
     * @throws IOException Error writing the file.
     */
    public static byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XMLEncoder e = new XMLEncoder(new BufferedOutputStream(baos));
        e.writeObject(o);
        e.close();
        return baos.toByteArray();
    }

    /**
     * Reads an object from a file.
     *
     * @param file File containing the serialized object.
     * @return The object read from the file.
     * @throws IOException Error reading the file.
     */
    public static Object deserialize(String file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        XMLDecoder d = new XMLDecoder(new BufferedInputStream(fis));
        Object o = d.readObject();
        d.close();
        return o;
    }

    /**
     * Reads an object from a byte array.
     *
     * @param data Byte array containing the serialized object.
     * @return The object read from the file.
     */
    public static Object deserialize(byte[] data) {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        XMLDecoder d = null;

        try {
            d = new XMLDecoder(new BufferedInputStream(bis));
            return d.readObject();
        } finally {
            if (d != null) {
                d.close();
            }
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }

}
