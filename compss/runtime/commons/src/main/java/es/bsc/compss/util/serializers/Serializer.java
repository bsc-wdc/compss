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
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;


/**
 * The serializer class is an utility to Serialize and deserialize objects passed as a parameter of a remote task.
 */
public class Serializer {

    public enum Format {
        JSON, XML, BINARY, PYBINDING
    }


    /**
     * Private constructor to avoid instantiation.
     */
    private Serializer() {
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
        try {
            BinarySerializer.serialize(o, file);
        } catch (NotSerializableException e) {
            XMLSerializer.serialize(o, file);
        }
    }

    /**
     * Serializes an objects and leaves it in a file.
     *
     * @param o Object to be serialized.
     * @param file File where the serialized object will be stored.
     * @param priorities List of serializer priorities.
     * @throws IOException Error writing the file.
     */
    public static void serialize(Object o, String file, Format[] priorities) throws IOException {
        loop: for (Format serializer : priorities) {
            switch (serializer) {
                case PYBINDING:
                    PyBindingSerializer.serialize(o, file);
                    break loop;
                case BINARY:
                    BinarySerializer.serialize(o, file);
                    break loop;
                case XML:
                    XMLSerializer.serialize(o, file);
                    break loop;
            }
        }
    }

    /**
     * Serializes an objects as a byte[].
     *
     * @param o Object to be serialized.
     * @throws IOException Error writing the file.
     */
    public static byte[] serialize(Object o) throws IOException {
        try {
            return BinarySerializer.serialize(o);
        } catch (NotSerializableException e) {
            return XMLSerializer.serialize(o);
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
        try {
            return BinarySerializer.deserialize(file);
        } catch (Exception e) {
            return XMLSerializer.deserialize(file);
        }
    }

    /**
     * Reads an object from a file.
     *
     * @param file File containing the serialized object.
     * @return The object read from the file.
     * @throws IOException Error reading the file.
     */
    public static Object deserialize(String file, Format[] priorities) throws IOException, ClassNotFoundException {
        for (Format serializer : priorities) {
            try {
                switch (serializer) {
                    case PYBINDING:
                        return PyBindingSerializer.deserialize(file);
                    case BINARY:
                        return BinarySerializer.deserialize(file);
                    case XML:
                        return XMLSerializer.deserialize(file);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        throw new IOException();
    }

    /**
     * Reads an object from a byte array.
     *
     * @param data Byte array containing the serialized object.
     * @return The object read from the file.
     * @throws IOException Error reading the file.
     */
    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try {
            return BinarySerializer.deserialize(data);
        } catch (Exception e) {
            return XMLSerializer.deserialize(data);
        }
    }

}
