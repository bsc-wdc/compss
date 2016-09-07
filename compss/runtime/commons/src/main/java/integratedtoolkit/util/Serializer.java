package integratedtoolkit.util;

import integratedtoolkit.types.exceptions.NonInstantiableException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


/**
 * The serializer class is an utility to Serialize and deserialize objects passed as a parameter of a remote task
 */
public class Serializer {

    private Serializer() {
        throw new NonInstantiableException("Serializer");
    }

    /**
     * Serializes an objects and leaves it in a file
     *
     * @param o
     *            object to be serialized
     * @param file
     *            file where the serialized object will be stored
     * @throws IOException
     *             Error writing the file
     */
    public static void serialize(Object o, String file) throws IOException {
        try {
            serializeBinary(o, file);
        } catch (NotSerializableException e) {
            serializeXML(o, file);
        }
    }

    /**
     * Reads an object from a file
     *
     * @param file
     *            containing the serialized object
     * @return the object read from the file
     * @throws IOException
     *             Error reading the file
     */
    public static Object deserialize(String file) throws IOException, ClassNotFoundException {
        try {
            return deserializeBinary(file);
        } catch (Exception e) {
            return deserializeXML(file);
        }
    }

    /**
     * Reads an object from a byte array
     *
     * @param b
     *            containing the serialized object
     * @return the object read from the file
     * @throws IOException
     *             Error reading the file
     */
    public static Object deserialize(byte[] b) throws IOException, ClassNotFoundException {
        try {
            return deserializeBinary(b);
        } catch (Exception e) {
            return deserializeXML(b);
        }
    }

    /**
     * Serializes an objects using the default java serializer and leaves it in a file
     *
     * @param o
     *            object to be serialized
     * @param file
     *            file where to store the serialized object
     * @throws IOException
     *             Error writing the file
     */
    private static void serializeBinary(Object o, String file) throws IOException {
        FileOutputStream fout = new FileOutputStream(file);
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(o);
        oos.close();
    }

    /**
     * Reads a binary-serialized object from a file
     *
     * @param file
     *            containing the serialized object
     * @return the object read from the file
     * @throws IOException
     *             Error reading the file
     */
    private static Object deserializeBinary(String file) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(file);
        ObjectInputStream ois = new ObjectInputStream(fis);
        Object o = ois.readObject();
        ois.close();
        return o;
    }

    /**
     * Reads a binary-serialized object from a byte array
     *
     * @param data
     *            containing the serialized object
     * @return the object read from the data
     *
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
     * Serializes an objects using the XML Encoder and leaves it in a file
     *
     * @param o
     *            object to be serialized
     * @param file
     *            file where to store the serialized object
     * @throws IOException
     *             Error writing the file
     */
    private static void serializeXML(Object o, String file) throws IOException {
        FileOutputStream fout = new FileOutputStream(file);
        XMLEncoder e = new XMLEncoder(new BufferedOutputStream(fout));
        e.writeObject(o);
        e.close();
    }

    /**
     * Reads an XML-serialized object from a file
     *
     * @param file
     *            containing the serialized object
     * @return the object read from the file
     * @throws IOException
     *             Error reading the file
     */
    private static Object deserializeXML(String file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        XMLDecoder d = new XMLDecoder(new BufferedInputStream(fis));
        Object o = d.readObject();
        d.close();
        return o;
    }

    /**
     * Reads a XML-serialized object from a byte array
     *
     * @param data
     *            containing the serialized object
     * @return the object read from the data
     *
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
