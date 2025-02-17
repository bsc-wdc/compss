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
package storage.utils;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;


/**
 * The serializer class is an utility to Serialize and deserialize objects passed as a parameter of a remote task.
 */
public class Serializer {

    private Serializer() {
        // To avoid instantiation.
        throw new UnsupportedOperationException();
    }

    /**
     * Serializes an objects and leaves it in a file.
     *
     * @param o Object to be serailized.
     * @param file File where the serialized object will be stored.
     * @throws IOException Error writting the file.
     */
    public static void serialize(Object o, String file) throws IOException {
        try {
            serializeBinary(o, file);
        } catch (NotSerializableException e) {
            serializeXML(o, file);
        }
    }

    /**
     * Serializes an object.
     *
     * @param o Object to be serailized.
     * @throws IOException Error writing the file.
     */
    public static byte[] serialize(Object o) throws IOException {
        try {
            return serializeBinary(o);
        } catch (NotSerializableException e) {
            return serializeXML(o);
        }
    }

    /**
     * Reads an object from a file.
     *
     * @param file Containing the serialized object.
     * @return The object read from the file.
     * @throws IOException Error reading the file.
     */
    public static Object deserialize(String file) throws IOException, ClassNotFoundException {
        try {
            return deserializeBinary(file);
        } catch (Exception e) {
            return deserializeXML(file);
        }
    }

    /**
     * Reads an object from a byte array.
     *
     * @param b Containing the serialized object.
     * @return The object read from the file.
     * @throws IOException Error reading the file.
     */
    public static Object deserialize(byte[] b) throws IOException, ClassNotFoundException {
        try {
            return deserializeBinary(b);
        } catch (Exception e) {
            return deserializeXML(b);
        }
    }

    /**
     * Serializes an objects using the default java serializer and leaves it in a file.
     *
     * @param o Object to be serialized.
     * @param file File where to store the serialized object.
     * @throws IOException Error writing the file.
     */
    private static void serializeBinary(Object o, String file) throws IOException {
        FileOutputStream fout = new FileOutputStream(file);
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(o);
        oos.close();
    }

    /**
     * Serializes an objects using the default java serializer.
     *
     * @param o Object to be serialized.
     * @throws IOException Error writting the byte stream.
     */
    private static byte[] serializeBinary(Object o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(o);
            return bos.toByteArray();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // No need to handle such exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // No need to handle such exception
            }
        }
    }

    /**
     * Reads a binary-serialized object from a file.
     *
     * @param file File containing the serialized object.
     * @return the Object read from the file.
     * @throws IOException Error reading the file.
     */
    private static Object deserializeBinary(String file) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(file);
        ObjectInputStream ois = new ObjectInputStream(fis);
        Object o = ois.readObject();
        ois.close();
        return o;
    }

    /**
     * Reads a binary-serialized object from a byte array.
     *
     * @param data Data containing the serialized object.
     * @return The object read from the data.
     */
    private static Object deserializeBinary(byte[] data) throws IOException, ClassNotFoundException {
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

    /**
     * Serializes an objects using the XML Encoder and leaves it in a file.
     *
     * @param o Object to be serialized.
     * @param file File where to store the serialized object.
     * @throws IOException Error writing the file.
     */
    private static void serializeXML(Object o, String file) throws IOException {
        FileOutputStream fout = new FileOutputStream(file);
        XMLEncoder e = new XMLEncoder(new BufferedOutputStream(fout));
        e.writeObject(o);
        e.close();
    }

    /**
     * Serializes an objects using the XML Encoder.
     *
     * @param o Object to be serialized.
     * @throws IOException Error writing the byte stream.
     */
    private static byte[] serializeXML(Object o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        XMLEncoder e = null;
        try {
            e = new XMLEncoder(new BufferedOutputStream(bos));
            e.writeObject(o);
        } finally {
            if (e != null) {
                e.close();
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return bos.toByteArray();
    }

    /**
     * Reads an XML-serialized object from a file.
     *
     * @param file File containing the serialized object.
     * @return The object read from the file.
     * @throws IOException Error reading the file.
     */
    private static Object deserializeXML(String file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        XMLDecoder d = new XMLDecoder(new BufferedInputStream(fis));
        Object o = d.readObject();
        d.close();
        return o;
    }

    /**
     * Reads a XML-serialized object from a byte array.
     *
     * @param data Data containing the serialized object.
     * @return The object read from the data.
     */
    private static Object deserializeXML(byte[] data) throws IOException, ClassNotFoundException {
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
