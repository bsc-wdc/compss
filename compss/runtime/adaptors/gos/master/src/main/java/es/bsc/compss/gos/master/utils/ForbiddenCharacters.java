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
package es.bsc.compss.gos.master.utils;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;


public class ForbiddenCharacters {

    private static final HashMap<String, String> encodeCharacters = new HashMap<>();
    private static final HashMap<String, String> decodeCharacters = new HashMap<>();

    private static boolean initalized = false;

    private static final String internalPrefix = "__Special";


    /**
     * Init internal resources to correctly encode and decode forbidden characters.
     */
    public static void init() {
        if (initalized) {
            return;
        }
        initalized = true;
        String[] chars = { "$",
            "#",
            "--",
            "~",
            ";",
            "<",
            ">",
            "--",
            " " };

        for (String decodedChar : chars) {
            String encodedChar = internalPrefix + new String(Base64.getEncoder().encode(decodedChar.getBytes()));
            encodeCharacters.put(decodedChar, encodedChar);
            decodeCharacters.put(encodedChar, decodedChar);

        }

    }

    /**
     * Encode string.
     *
     * @param replaceSpace the ignore space
     * @return the string
     */
    public static String encode(String word, boolean replaceSpace) {
        for (Map.Entry<String, String> entry : encodeCharacters.entrySet()) {
            if (!replaceSpace && entry.getKey().equals(" ")) {
                continue;
            }
            word = word.replace(entry.getKey(), entry.getValue());
        }
        return word;
    }

    /**
     * Decode.
     *
     * @param word the word
     */
    public static String decode(String word) {
        if (word.contains(internalPrefix)) {
            for (Map.Entry<String, String> entry : decodeCharacters.entrySet()) {
                word = word.replace(entry.getKey(), entry.getValue());
            }
        }
        return word;
    }

    /**
     * Decode arguments.
     *
     * @param args the args
     */
    public static void decode(String[] args) {
        for (int i = 0; i < args.length; i++) {
            args[i] = decode(args[i]);
        }
    }


    private static final String RESET = "\033[0m"; // Text Reset

    // Regular Colors
    private static final String RED = "\033[0;31m"; // RED
    private static final String GREEN = "\033[0;32m"; // GREEN


    /**
     * Main for testing purposes.
     */
    public static void main(String[] args) {

        ForbiddenCharacters.init();
        String[] initalString = { "mariachi",
            "$return0",
            "[#POTATO]",
            "CLEAR;CLEAR",
            "SPACE INVADERS",
            "$KEY:@HASH#;$KEY",
            ">IOASSKl<dñlskfñls 'das'",
            "comillas \" ",
            ";#$%&: ::***@,a",
            "--reserved" };
        String[] encodedString = new String[initalString.length];
        for (int i = 0; i < initalString.length; i++) {
            encodedString[i] = encode(initalString[i], true);
        }

        String[] decoded1 = encodedString.clone();
        decode(decoded1);
        String[] decoded2 = encodedString.clone();
        for (int i = 0; i < decoded2.length; i++) {
            decoded2[i] = decode(decoded2[i]);
        }

        for (int i = 0; i < initalString.length; i++) {
            System.out.println("Inital String:         " + initalString[i]);
            System.out.println("Encode String:         " + encodedString[i]);
            System.out.println("Decode String Args:    " + decoded1[i]);
            System.out.println("Decode String:  Single " + decoded2[i]);
            String color = RED;
            boolean c1 = decoded1[i].equals(decoded2[i]);
            boolean c2 = initalString[i].equals(decoded1[i]);
            if (c1 && c2) {
                color = GREEN;
            }
            System.out.println(color + "TEST WORD: DIFFERNCE " + c1 + " result " + c2 + RESET + "\n");

        }

    }
}
