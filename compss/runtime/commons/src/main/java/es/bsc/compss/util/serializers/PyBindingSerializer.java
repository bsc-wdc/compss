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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import es.bsc.compss.types.exceptions.NonInstantiableException;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Base64;


/**
 * The serializer class is an utility to Serialize and deserialize objects passed as a parameter of a remote task.
 */
public class PyBindingSerializer {

    private static final String JSON_SERIALIZER_ID = "0004";


    /**
     * Private constructor to avoid instantiation.
     */
    private PyBindingSerializer() {
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
        FileWriter fw = new FileWriter(file);
        // 4 is JSON package ID in Python binding
        fw.write(JSON_SERIALIZER_ID);
        Gson gson = new Gson();
        String json = gson.toJson(o);
        fw.write(json);
        fw.close();
    }

    /**
     * Serializes an objects as a byte[].
     *
     * @param o Object to be serialized.
     * @throws IOException Error writing the file.
     */
    public static byte[] serialize(Object o) throws IOException {
        Gson gson = new Gson();
        String json = gson.toJson(o);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutput out;
            out = new ObjectOutputStream(bos);
            out.writeObject(json);
            out.flush();
            return bos.toByteArray();
        }
    }

    /**
     * Reads an object from a file.
     *
     * @param fileName File containing the serialized object.
     * @return The object read from the file.
     * @throws IOException Error reading the file.
     */
    public static Object deserialize(String fileName) throws IOException {
        String jsonContent = getFileContent(fileName).substring(JSON_SERIALIZER_ID.length());
        return stringToJSON(jsonContent);
    }

    /**
     * Reads an object from a byte array.
     *
     * @param data Byte array containing the serialized object.
     * @return The object read from the file.
     * @throws IOException Error reading the file.
     */
    public static Object deserialize(byte[] data) {
        String str = new String(data);
        return stringToJSON(str);
    }

    private static Object stringToJSON(String in) {
        JsonElement jsonElement = JsonParser.parseString(in);
        if (jsonElement.isJsonPrimitive()) {
            return jsonElement.getAsJsonPrimitive().getAsString();
        } else if (jsonElement.isJsonObject()) {
            return jsonElement.toString();
        } else if (jsonElement.isJsonArray()) {
            return jsonElement.getAsJsonArray().toString();
        }
        return null;
    }

    /**
     * Reads an object from a byte array.
     *
     * @param fileName file to be read
     * @return The String object read from the file.
     * @throws IOException Error reading the file.
     */
    private static String getFileContent(String fileName) throws IOException {
        File f = new File(fileName);
        StringBuilder content = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                content.append(line);
            }
        }
        return content.toString();
    }

    private static String decodeBase64(String encoded) {
        byte[] decoded = Base64.getDecoder().decode(encoded);
        return new String(decoded);
    }

}
